# Project instructions for Claude Code

This repo demonstrates one specific pattern: **using EF Core safely inside a concurrent Kafka consumer via `IDbContextFactory`.** Keep every change consistent with that.

## Hard rule
Never inject a scoped or singleton `DbContext` anywhere in `OrderEvents.Consumer`. Always resolve `IDbContextFactory<OrdersDbContext>` and call `CreateDbContextAsync()` per unit of work, disposing it (`await using`) before that unit of work returns. This is what `OrderRecordProjector` does — new persistence logic should follow the same shape.

## Conventions
- Keep Kafka-specific concerns (consuming, committing offsets, batching) in `Consumers/OrderPlacedConsumer.cs`. Keep persistence logic in `Persistence/*`, independent of Kafka, so it can be unit tested with EF Core's InMemory provider without a running broker.
- AI-backed features (`AI/ClaudeFailureAnalyzer.cs`) must fail closed: if the Claude API call fails or is disabled, fall back to the plain behavior (raw exception message) rather than throwing.
- Every new method in `Persistence/*` needs a corresponding test in `tests/OrderEvents.Consumer.Tests`, including at least one concurrency test if it touches the DbContext.

## Useful commands
- `/diagnose-failure <stack trace>` — diagnose a consumer failure, checking first for the shared-DbContext anti-pattern this repo exists to prevent.
