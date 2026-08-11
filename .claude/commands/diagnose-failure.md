---
description: Diagnose a Kafka consumer failure — distinguish a shared-DbContext concurrency bug from other causes, and check whether OrderRecordProjector is being used correctly.
argument-hint: [paste the exception/stack trace]
---

Diagnose this Kafka consumer failure: $ARGUMENTS

This codebase's core invariant is: **every unit of work gets its own `DbContext` via `IDbContextFactory`, never a shared/injected `DbContext` instance.** Most subtle bugs here trace back to that invariant being violated somewhere new.

1. Check whether the exception matches the signature of a shared-DbContext concurrency issue (`InvalidOperationException` mentioning concurrent operations, or unexplained data corruption under load). If so, search for any place a `DbContext` or `OrdersDbContext` might be held longer than one message's processing, or injected as scoped/singleton instead of via the factory.
2. If it doesn't match that pattern, investigate normally: check Kafka consumer config (`KafkaConsumerSettings`), message deserialization in `Models/OrderPlacedEvent.cs`, and the dedup check in `OrderRecordProjector`.
3. Report findings in the same structured format as `/bug-report`: Summary, Root Cause, Suggested Fix, Regression Test Needed.
4. If relevant, note whether `ClaudeFailureAnalyzer`'s runtime explanation (from the log line "AI analysis: ...") already pointed at the right cause — useful feedback for tuning that prompt.
