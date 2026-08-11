using Microsoft.EntityFrameworkCore;
using OrderEvents.Consumer.Models;

namespace OrderEvents.Consumer.Persistence;

/// <summary>
/// Encapsulates the "one DbContext per unit of work" persistence logic so it can
/// be unit tested independently of Kafka. Each call to ProjectAsync opens its own
/// DbContext via the factory and disposes it before returning, making concurrent
/// calls from multiple in-flight message handlers safe.
/// </summary>
public class OrderRecordProjector
{
    private readonly IDbContextFactory<OrdersDbContext> _dbContextFactory;

    public OrderRecordProjector(IDbContextFactory<OrdersDbContext> dbContextFactory)
    {
        _dbContextFactory = dbContextFactory;
    }

    public async Task ProjectAsync(OrderPlacedEvent @event, CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);

        var exists = await dbContext.OrderRecords
            .AnyAsync(o => o.OrderId == @event.OrderId, cancellationToken);

        if (exists)
            return;

        dbContext.OrderRecords.Add(new OrderRecord
        {
            OrderId = @event.OrderId,
            CustomerId = @event.CustomerId,
            TotalAmount = @event.TotalAmount,
            Currency = @event.Currency,
            PlacedAtUtc = @event.PlacedAtUtc,
            ProcessedAtUtc = DateTimeOffset.UtcNow
        });

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
