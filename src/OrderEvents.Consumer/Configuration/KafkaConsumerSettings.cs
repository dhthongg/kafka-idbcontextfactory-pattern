namespace OrderEvents.Consumer.Configuration;

public class KafkaConsumerSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "order-events-consumer";
    public string Topic { get; set; } = "order-placed";

    /// <summary>
    /// Maximum number of messages processed concurrently before committing offsets.
    /// Each concurrent handler opens its own DbContext via IDbContextFactory.
    /// </summary>
    public int MaxConcurrentHandlers { get; set; } = 8;
}
