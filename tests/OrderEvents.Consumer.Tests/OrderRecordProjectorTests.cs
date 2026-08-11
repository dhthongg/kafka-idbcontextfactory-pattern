using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using OrderEvents.Consumer.Models;
using OrderEvents.Consumer.Persistence;
using Xunit;

namespace OrderEvents.Consumer.Tests;

/// <summary>
/// Verifies OrderRecordProjector's core guarantee: each call opens its own
/// DbContext, so concurrent calls against the same underlying database do not
/// throw the "second operation started before first completed" exception that
/// a shared, scoped DbContext would raise under concurrent access.
/// </summary>
public class OrderRecordProjectorTests
{
    private static IDbContextFactory<OrdersDbContext> CreateFactory(string dbName)
    {
        var services = new ServiceCollection();

        services.AddDbContextFactory<OrdersDbContext>(options =>
            options.UseInMemoryDatabase(dbName));

        return services.BuildServiceProvider().GetRequiredService<IDbContextFactory<OrdersDbContext>>();
    }

    [Fact]
    public async Task ProjectAsync_NewEvent_PersistsOrderRecord()
    {
        var factory = CreateFactory(nameof(ProjectAsync_NewEvent_PersistsOrderRecord));
        var projector = new OrderRecordProjector(factory);
        var @event = new OrderPlacedEvent(Guid.NewGuid(), Guid.NewGuid(), 99.90m, "AUD", DateTimeOffset.UtcNow);

        await projector.ProjectAsync(@event, CancellationToken.None);

        await using var context = await factory.CreateDbContextAsync();
        var stored = await context.OrderRecords.SingleAsync(o => o.OrderId == @event.OrderId);
        Assert.Equal(99.90m, stored.TotalAmount);
    }

    [Fact]
    public async Task ProjectAsync_DuplicateEvent_DoesNotInsertTwice()
    {
        var factory = CreateFactory(nameof(ProjectAsync_DuplicateEvent_DoesNotInsertTwice));
        var projector = new OrderRecordProjector(factory);
        var @event = new OrderPlacedEvent(Guid.NewGuid(), Guid.NewGuid(), 50m, "AUD", DateTimeOffset.UtcNow);

        await projector.ProjectAsync(@event, CancellationToken.None);
        await projector.ProjectAsync(@event, CancellationToken.None); // simulate Kafka at-least-once redelivery

        await using var context = await factory.CreateDbContextAsync();
        var count = await context.OrderRecords.CountAsync(o => o.OrderId == @event.OrderId);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ProjectAsync_ManyConcurrentDistinctEvents_AllPersistWithoutContextConflict()
    {
        // This is the scenario that originally broke with a single shared DbContext:
        // many messages processed concurrently. Because each call to ProjectAsync
        // creates its own DbContext via the factory, this completes cleanly.
        var factory = CreateFactory(nameof(ProjectAsync_ManyConcurrentDistinctEvents_AllPersistWithoutContextConflict));
        var projector = new OrderRecordProjector(factory);

        var events = Enumerable.Range(0, 50)
            .Select(_ => new OrderPlacedEvent(Guid.NewGuid(), Guid.NewGuid(), 10m, "AUD", DateTimeOffset.UtcNow))
            .ToList();

        await Task.WhenAll(events.Select(e => projector.ProjectAsync(e, CancellationToken.None)));

        await using var context = await factory.CreateDbContextAsync();
        var count = await context.OrderRecords.CountAsync();
        Assert.Equal(50, count);
    }
}
