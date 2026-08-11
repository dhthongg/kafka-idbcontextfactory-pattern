using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using OrderEvents.Consumer.AI;
using OrderEvents.Consumer.Configuration;
using OrderEvents.Consumer.Models;
using OrderEvents.Consumer.Persistence;

namespace OrderEvents.Consumer.Consumers;

/// <summary>
/// Background service that consumes OrderPlacedEvent messages and persists them
/// as a local read model.
///
/// WHY IDbContextFactory INSTEAD OF A SCOPED DbContext:
/// A single BackgroundService runs on one long-lived thread, but the Kafka
/// consumer loop below processes messages continuously and, under load, we
/// process batches with Task.WhenAll to keep up with partition throughput.
/// Injecting a single scoped DbContext into this class means every concurrent
/// message handler shares the same DbContext instance — EF Core's DbContext is
/// NOT thread-safe, and concurrent SaveChangesAsync calls against one instance
/// throw "A second operation was started on this context before a previous
/// operation completed" or silently corrupt the change tracker.
///
/// IDbContextFactory&lt;TContext&gt; solves this by creating a new, independent
/// DbContext per unit of work. Each message gets its own short-lived context,
/// created and disposed within the handler, so concurrent processing is safe
/// without introducing manual locking that would serialize (and slow down)
/// the whole consumer.
/// </summary>
public class OrderPlacedConsumer : BackgroundService
{
    private readonly OrderRecordProjector _projector;
    private readonly IFailureAnalyzer _failureAnalyzer;
    private readonly ILogger<OrderPlacedConsumer> _logger;
    private readonly KafkaConsumerSettings _settings;
    private readonly IConsumer<string, string> _consumer;

    public OrderPlacedConsumer(
        OrderRecordProjector projector,
        IFailureAnalyzer failureAnalyzer,
        IOptions<KafkaConsumerSettings> settings,
        ILogger<OrderPlacedConsumer> logger)
    {
        _projector = projector;
        _failureAnalyzer = failureAnalyzer;
        _logger = logger;
        _settings = settings.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_settings.Topic);

        // Process messages in small concurrent batches to keep pace with the
        // topic's throughput. Each task below opens its own DbContext via the
        // factory, so this loop is safe to parallelise.
        var inFlight = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            var result = _consumer.Consume(stoppingToken);
            if (result?.Message is null)
                continue;

            inFlight.Add(ProcessMessageAsync(result, stoppingToken));

            if (inFlight.Count >= _settings.MaxConcurrentHandlers)
            {
                await Task.WhenAll(inFlight);
                _consumer.Commit();
                inFlight.Clear();
            }
        }
    }

    private async Task ProcessMessageAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        try
        {
            var @event = JsonSerializer.Deserialize<OrderPlacedEvent>(result.Message.Value)
                ?? throw new InvalidOperationException("Message payload deserialized to null.");

            // Each call opens and disposes its own DbContext internally — safe to
            // run concurrently across many in-flight messages.
            await _projector.ProjectAsync(@event, cancellationToken);
        }
        catch (Exception ex)
        {
            var explanation = await _failureAnalyzer.ExplainAsync(
                ex,
                context: $"topic={result.Topic}, partition={result.Partition.Value}, offset={result.Offset}",
                cancellationToken);

            _logger.LogError(ex,
                "Failed to process message at offset {Offset}. AI analysis: {Explanation}",
                result.Offset, explanation);
            throw;
        }
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        base.Dispose();
    }
}
