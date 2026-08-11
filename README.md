# Kafka Consumer — IDbContextFactory Concurrency Pattern

A focused reference implementation showing how to safely use **Entity Framework Core inside a concurrent Kafka consumer**, built around a real production bug: a `BackgroundService` processing Kafka messages concurrently, sharing a single scoped `DbContext`, intermittently throwing `InvalidOperationException: A second operation was started on this context before a previous operation completed`.

## The problem

`DbContext` is not thread-safe. In a typical ASP.NET Core app this is invisible because each HTTP request gets its own scoped `DbContext`. A `BackgroundService`, however, is a singleton — if you inject a scoped `DbContext` into it (or resolve one scope and reuse it across the consumer loop), every concurrent message handler shares that same instance. Under load, concurrent `SaveChangesAsync` calls collide.

## The fix — `IDbContextFactory<TContext>`

Instead of injecting a `DbContext`, the consumer injects `IDbContextFactory<OrdersDbContext>` and creates a **new, independent `DbContext` per message**, disposed as soon as that message's work is done:

```csharp
await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
```

This is registered with `AddDbContextFactory<T>()` instead of `AddDbContext<T>()` in `Program.cs`. The persistence logic lives in `OrderRecordProjector`, kept separate from the Kafka-specific consumer loop so it can be unit tested without a running broker (see `tests/OrderEvents.Consumer.Tests/OrderRecordProjectorTests.cs`, including a test that fires 50 concurrent projections against the same in-memory database to prove there's no context conflict).

## Architecture

```
src/OrderEvents.Consumer/
├── Consumers/OrderPlacedConsumer.cs   # BackgroundService: Kafka consume loop, concurrency batching
├── Persistence/
│   ├── OrdersDbContext.cs
│   ├── OrderRecord.cs
│   └── OrderRecordProjector.cs        # The IDbContextFactory pattern, isolated and testable
├── AI/ClaudeFailureAnalyzer.cs        # Optional: AI-assisted failure triage (see below)
├── Configuration/KafkaConsumerSettings.cs
├── Models/OrderPlacedEvent.cs
└── Program.cs

tests/OrderEvents.Consumer.Tests/      # xUnit + EF Core InMemory
```

## AI-assisted failure analysis

When a message fails processing, `IFailureAnalyzer` / `ClaudeFailureAnalyzer` sends the exception and its Kafka context (topic, partition, offset) to Claude and asks for a short triage note: is this transient (safe to let redelivery handle) or does it need a code fix, and what's the next thing to check. This is what actually would have shortened the original debugging session for the `IDbContextFactory` bug this repo demonstrates — the raw exception message alone didn't point at "shared DbContext," but a short AI explanation of the exception in context does.

The feature is **opt-in and fails closed**: disabled by default (`Claude:Enabled: false`), and if the Claude API call itself fails, it falls back to the raw exception message rather than blocking error handling.

## Tech stack

C# · .NET 9 · Worker Service (`Microsoft.Extensions.Hosting`) · Confluent.Kafka · Entity Framework Core (SQL Server + InMemory for tests) · xUnit · Anthropic API (Claude) · Claude Code

## Running locally

Requires a local Kafka broker and SQL Server instance (see `appsettings.json` for connection details).

```bash
dotnet restore
dotnet build
dotnet run --project src/OrderEvents.Consumer
```

## Running tests

```bash
dotnet test
```

## Author

Dang Hoang Thong — .NET / ASP.NET Core software engineer, 5 years of experience building enterprise backend systems, including Kafka-based event-driven services in production.
[LinkedIn](https://www.linkedin.com/in/dhthongg/) · [GitHub](https://github.com/dhthongg)
