# Retry Policy and Adaptive Timeout

This document is the v1 contract for the nORM retry policy and adaptive timeout
manager.

## Retry Overview

nORM has two retry surfaces:

1. **`SaveChangesAsync` retry loop** — implemented in `DbContext` and governed by
   `DbContextOptions.RetryPolicy`. Only pre-commit transient failures are retried;
   once `CommitAsync` has been attempted the outcome is unknown and no retry is
   made.
2. **`RetryingExecutionStrategy`** — wraps arbitrary async operations (typically
   queries) with the same `RetryPolicy`.

Both surfaces share the same `RetryPolicy` configuration and the same exclusion
rules.

## Retryable Operations

The following operations are eligible for retry when `RetryPolicy.ShouldRetry`
returns `true` for the exception:

- Idempotent reads (`Query<T>`, `CountAsync`, `AnyAsync`, etc.) executed through
  `RetryingExecutionStrategy`.
- `SaveChangesAsync` write operations **before** `CommitAsync` has been
  attempted. If the failure occurs before the transaction is committed, the
  database is in a known clean state and the operation can be replayed.

## Non-Retryable Operations

The following are **never** retried regardless of the `RetryPolicy` setting:

- Operations executed inside an explicit `BeginTransactionAsync` transaction or
  under an ambient `System.Transactions.TransactionScope`. nORM detects
  `CurrentTransaction != null` or `Transaction.Current != null` at the start of
  `SaveChangesAsync` and executes exactly once. Retrying inside an external
  transaction would replay writes whose rollback path belongs to the caller.
- Any `SaveChangesAsync` attempt where `CommitAsync` has been called. The
  `commitAttempted` flag in the retry loop ensures this; a post-commit exception
  propagates directly.
- `TimeoutException`. A timed-out write may have already been applied by the
  database; retrying it risks duplicating rows. `IsRetryableException` and
  `RetryingExecutionStrategy` both exclude `TimeoutException` from the retryable
  exception set.
- `NormConfigurationException`. Configuration errors are permanent — retrying
  would produce the same failure.
- Any exception type not covered by `ShouldRetry` (non-`DbException`,
  non-`IOException`, non-`SocketException` by default).

## Default Retry Limits and Backoff

| Setting | Default |
|---|---|
| `MaxRetries` | 3 |
| `BaseDelay` | 1 second |
| Backoff | Exponential: `BaseDelay * 2^attempt` |
| Jitter | ±20% applied to each backoff interval (`RetryJitterRange = 0.2`) |
| Maximum delay cap | 5 minutes per interval (`RetryingExecutionStrategy` only) |
| Exponent cap | 30, preventing overflow for very high `MaxRetries` (`RetryingExecutionStrategy` only) |

The delay/exponent caps above apply to `RetryingExecutionStrategy`; the `SaveChangesAsync`
write-path retry loop applies the ±20% jitter but does not cap the exponent, so keep
`MaxRetries` modest on that path. With the defaults a `SaveChangesAsync` retry sequence looks
like:

```
attempt 0: execute immediately
attempt 1: wait ~1 s  (±20% jitter)
attempt 2: wait ~2 s  (±20% jitter)
attempt 3: propagate the exception
```

## Configuring RetryPolicy

```csharp
var options = new DbContextOptions
{
    RetryPolicy = new RetryPolicy
    {
        MaxRetries = 3,
        BaseDelay  = TimeSpan.FromSeconds(1),
        ShouldRetry = ex =>
        {
            // SQL Server transient error numbers
            if (ex is SqlException sqlEx &&
                sqlEx.Number is 4060 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920 or 1205 or 1222)
                return true;

            // Network-level failures
            if (ex is IOException || ex is SocketException)
                return true;

            return false;
        }
    }
};
```

The default `ShouldRetry` covers SQL Server transient error numbers and
network-level `IOException`/`SocketException`. Applications targeting
PostgreSQL, MySQL, or SQLite should add provider-specific transient error
conditions to the delegate.

To disable retry entirely, set `RetryPolicy = null` (the default) or use
`MaxRetries = 0`.

## RetryingExecutionStrategy

`RetryingExecutionStrategy` wraps arbitrary async delegate operations with the
same retry loop. It catches `DbException`, `IOException`, and `SocketException`
but not `TimeoutException`. After `MaxRetries` attempts the last exception is
wrapped in a `NormException` and re-thrown.

```csharp
var strategy = new RetryingExecutionStrategy(ctx, retryPolicy);
var result = await strategy.ExecuteAsync(
    (ctx, ct) => ctx.Query<Order>().Where(o => o.Pending).ToListAsync(ct),
    cancellationToken);
```

The cancellation token is forwarded to every attempt and to the
`Task.Delay` between attempts so cancellation is always respected.

## AdaptiveTimeoutManager Interaction

`AdaptiveTimeoutManager` wraps operations in a `CancellationTokenSource` timeout
derived from `TimeoutConfiguration` and historical execution statistics. When the
timeout fires, it throws `NormTimeoutException` (a `NormException` subtype).

Key interactions with retry:

- `NormTimeoutException` inherits from `NormException`, which itself derives from
  `DbException` (every nORM exception is a `DbException`, so `catch (DbException)`
  catches nORM timeouts). The default `ShouldRetry` delegate returns `false` for it
  because the default predicate only matches transient `SqlException` codes plus
  `IOException` / `SocketException` — not because it is not a `DbException`. This is
  intentional: a timed-out operation has an unknown database outcome.
- `RetryingExecutionStrategy`'s catch guard matches `DbException`, so it DOES catch
  `NormTimeoutException`; `ShouldRetry` then returns `false` and it is rethrown without
  retrying (a retry-error is logged). Only a raw `System.TimeoutException` bypasses the
  guard entirely.
- If an application needs to retry timed-out operations (e.g. for reads where
  idempotency is certain), configure `ShouldRetry` to return `true` for
  `NormTimeoutException` explicitly. Do not use this for writes unless you have
  idempotency guarantees at the database level.

## Logging

Each retry attempt in `RetryingExecutionStrategy` logs an error through
`DbContextOptions.Logger` before sleeping. The log entry includes the retry
count and the wrapped `NormException`. Applications that configure a logger
receive structured retry telemetry without any additional setup.
