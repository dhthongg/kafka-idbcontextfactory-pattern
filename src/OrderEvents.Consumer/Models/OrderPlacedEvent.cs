namespace OrderEvents.Consumer.Models;

/// <summary>
/// Message contract published to the "order-placed" Kafka topic by the upstream
/// order service. Consumed here and persisted into a local read model.
/// </summary>
public sealed record OrderPlacedEvent(
    Guid OrderId,
    Guid CustomerId,
    decimal TotalAmount,
    string Currency,
    DateTimeOffset PlacedAtUtc);
