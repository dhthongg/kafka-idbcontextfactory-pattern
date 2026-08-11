namespace OrderEvents.Consumer.AI;

/// <summary>
/// Abstraction over an AI provider used to turn a raw exception into a short,
/// plain-English explanation for whoever is on call. Kept as an interface so the
/// consumer loop never depends on a specific AI vendor's SDK.
/// </summary>
public interface IFailureAnalyzer
{
    Task<string> ExplainAsync(Exception exception, string context, CancellationToken cancellationToken);
}
