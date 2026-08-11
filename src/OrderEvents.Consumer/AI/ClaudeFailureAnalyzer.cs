using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderEvents.Consumer.AI;

public class ClaudeAiSettings
{
    public const string SectionName = "Claude";

    public string ApiKey { get; set; } = string.Empty;
    public string Model { get; set; } = "claude-sonnet-4-6";
    public string BaseUrl { get; set; } = "https://api.anthropic.com/v1/messages";
    public string ApiVersion { get; set; } = "2023-06-01";
    public bool Enabled { get; set; } = false;
}

/// <summary>
/// Calls Claude to translate a raw .NET exception plus surrounding context into
/// a short, actionable explanation for on-call engineers — e.g. distinguishing
/// "transient Kafka broker hiccup, safe to retry" from "schema mismatch,
/// needs a code fix" without someone having to read the full stack trace first.
///
/// This was built after debugging a real concurrency issue in this consumer
/// (concurrent access to a shared DbContext) that initially surfaced only as a
/// generic InvalidOperationException with no obvious cause from the message
/// alone — the kind of failure this analyzer is meant to help triage faster.
/// </summary>
public class ClaudeFailureAnalyzer : IFailureAnalyzer
{
    private readonly HttpClient _httpClient;
    private readonly ClaudeAiSettings _settings;
    private readonly ILogger<ClaudeFailureAnalyzer> _logger;

    public ClaudeFailureAnalyzer(
        HttpClient httpClient,
        IOptions<ClaudeAiSettings> settings,
        ILogger<ClaudeFailureAnalyzer> logger)
    {
        _httpClient = httpClient;
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task<string> ExplainAsync(Exception exception, string context, CancellationToken cancellationToken)
    {
        if (!_settings.Enabled || string.IsNullOrWhiteSpace(_settings.ApiKey))
            return exception.Message; // AI analysis is opt-in; fall back to the raw message.

        var prompt = $"""
            A background Kafka consumer failed while processing a message. Explain in
            2-3 sentences, for an on-call engineer: (1) what likely went wrong, (2) whether
            it is probably transient (safe to let the retry/redelivery handle it) or needs a
            code fix, and (3) the single most useful next step to check.

            Context: {context}
            Exception type: {exception.GetType().FullName}
            Exception message: {exception.Message}
            """;

        var payload = new
        {
            model = _settings.Model,
            max_tokens = 200,
            messages = new[] { new { role = "user", content = prompt } }
        };

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, _settings.BaseUrl)
            {
                Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
            };
            request.Headers.Add("x-api-key", _settings.ApiKey);
            request.Headers.Add("anthropic-version", _settings.ApiVersion);

            using var response = await _httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);

            return document.RootElement.GetProperty("content")[0].GetProperty("text").GetString()
                   ?? exception.Message;
        }
        catch (Exception aiEx)
        {
            _logger.LogWarning(aiEx, "Failure analysis via Claude did not complete; falling back to raw exception message.");
            return exception.Message;
        }
    }
}
