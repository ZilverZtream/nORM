# Exception Taxonomy

nORM v1 uses a small exception contract so applications can catch stable failure
classes without parsing provider messages.

| Type | Use |
| --- | --- |
| `NormException` | Base nORM failure. Low-level database exceptions may be wrapped in this type when no narrower category applies. |
| `NormDatabaseException` | Provider/database failures such as SQL errors, constraint violations, or version introspection failures. The original provider exception is preserved as `InnerException`. |
| `NormConnectionException` | Connection creation, opening, pooling, or connection-management failures. |
| `NormTimeoutException` | nORM timeout policy failures. Cancellation requested by a caller remains `OperationCanceledException`. |
| `NormConfigurationException` | Invalid model, provider, tenant, transaction, or options configuration. |
| `NormQueryException` | Query translation, query-shape, SQL generation, or query execution errors. |
| `NormUnsupportedFeatureException` | A recognized LINQ/API shape is intentionally unsupported. This is not a transient database failure. |
| `NormUsageException` | Unsafe or invalid direct API usage, such as dangerous raw SQL or invalid dynamic table names. |
| `DbConcurrencyException` | Optimistic concurrency conflicts. This type does not derive from `NormException` for compatibility with existing catch patterns. |

## Wrapping Rules

- nORM-specific exceptions are not double-wrapped.
- Caller cancellation is not wrapped.
- Provider exceptions are wrapped only when the execution path has enough context
  to preserve SQL and parameter metadata safely.
- SQL and parameter metadata attached to exceptions must follow the logging and
  redaction policy. Sensitive parameter values are not required for catch logic.
- Unsupported LINQ translation and bulk CUD shapes throw
  `NormUnsupportedFeatureException`, not `NotSupportedException`, on public query
  paths.
- Constrained relationship loading paths, including composite-key dependent
  includes and async streaming with Include/GroupJoin, also throw
  `NormUnsupportedFeatureException`.

## Cancellation Cleanup Contract

Every async public API that accepts a `CancellationToken` honors it. When a cancellation token
fires mid-operation, nORM's contract per resource type is:

| Resource | Behavior on cancellation |
| --- | --- |
| Owned transactions (`BeginTransactionAsync` -> work -> `CommitAsync`/`RollbackAsync`) | If cancellation fires before commit, nORM rolls back automatically and disposes the transaction. If cancellation fires *during* commit, the operation completes (commit is not interruptible at the protocol level) and nORM returns successfully - cancellation does NOT throw `OperationCanceledException` for already-committed transactions. |
| Ambient transactions (`TransactionScope`) | nORM never owns the transaction; cancellation propagates to the underlying command but the scope ownership remains with the caller. |
| Temp tables (bulk operations) | Dropped during cancellation cleanup; verified by `BulkTempTableLeakTests` and `BulkOperationCancellationTests`. |
| Command pools and prepared commands | Returned to the pool / disposed before `OperationCanceledException` propagates. |
| Open data readers | Disposed before `OperationCanceledException` propagates. |
| Migrations | Cancellation between steps leaves the migration history in a consistent state; cancellation during a single step rolls back that step's transaction. See `MigrationCommitCancellationTests`. |
| Temporal bootstrap (`EnableTemporalVersioning`) | The bootstrap task is idempotent; partial bootstrap is detected on the next run and resumed. See `TemporalBootstrapCancellationTests`. |
| Interceptors | Receive the same `CancellationToken`; interceptor exceptions surface as the operation's exception. |

Caller cancellation (an `OperationCanceledException` raised by the caller's token) is never
re-wrapped as a nORM exception. Timeouts originating from `DbContextOptions` / `AdaptiveTimeoutManager`
surface as `NormTimeoutException`. Retry policies treat caller cancellations as terminal and
never retry them.

## Guidance

Catch `DbConcurrencyException` for retry or merge workflows. Catch
`NormUnsupportedFeatureException` to fall back to raw SQL or client code for an
unsupported query shape. Catch `NormConfigurationException` during startup or
deployment validation. Application-level retry policies should normally treat
`NormDatabaseException`, `NormConnectionException`, and `NormTimeoutException`
as the only retry candidates, and only after checking provider-specific error
codes on `InnerException`.
