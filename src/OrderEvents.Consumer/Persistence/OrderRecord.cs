namespace OrderEvents.Consumer.Persistence;

/// <summary>
/// Local read-model projection built from OrderPlacedEvent messages.
/// </summary>
public class OrderRecord
{
    public Guid OrderId { get; set; }
    public Guid CustomerId { get; set; }
    public decimal TotalAmount { get; set; }
    public string Currency { get; set; } = "AUD";
    public DateTimeOffset PlacedAtUtc { get; set; }
    public DateTimeOffset ProcessedAtUtc { get; set; }
}
