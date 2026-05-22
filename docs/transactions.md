# Transaction Contract

This document is the v1 transaction ownership contract for nORM. It covers
`SaveChangesAsync`, direct writes, bulk operations, raw SQL, compiled queries,
migrations, savepoints, and ambient `TransactionScope`.

## Ownership Rules

| Path | Transaction Owner | Commit/Rollback Owner | Change Tracker Accepts Changes |
|---|---|---|---|
| `SaveChangesAsync` with no active transaction | nORM internal `TransactionManager` | nORM | After nORM commit succeeds |
| `SaveChangesAsync` inside `Database.BeginTransactionAsync` | Caller | Caller | Deferred until caller commits |
| Direct writes without an active transaction | nORM per operation | nORM | Operation result is durable when call returns |
| Direct writes with an explicit transaction | Caller | Caller | Caller controls durability |
| Bulk operations with an explicit transaction | Caller | Caller | Caller controls durability |
| Raw SQL with `Database.CurrentTransaction` | Caller | Caller | n/a |
| Migrations | Migration runner | Migration runner | n/a |
| `TransactionScope` with successful enlistment | Ambient scope | Caller scope | Deferred until ambient scope completes |
| `TransactionScope` with `BestEffort` failed enlistment | Provider operation | Provider operation | Accepted because writes commit independently |
| `TransactionScope` with `Ignore` | Provider operation | Provider operation | Accepted only when de-enlistment is safe |

## Explicit Transactions

Use `await ctx.Database.BeginTransactionAsync()` when one unit of work must span
multiple nORM calls. A context can have only one active explicit transaction.
A second `BeginTransactionAsync` call while `Database.CurrentTransaction` is set
throws `InvalidOperationException`.

`DbContextTransaction.CommitAsync` and `RollbackAsync` always clear
`Database.CurrentTransaction`, even if the provider throws during commit or
rollback. This prevents a failed completion attempt from poisoning the context.

Commit and rollback use `CancellationToken.None` once completion begins. At that
point cancellation cannot reliably distinguish "not committed" from "committed
but acknowledgement failed".

## Ambient TransactionScope

`DbContextOptions.AmbientTransactionPolicy` controls behavior when
`System.Transactions.Transaction.Current` exists and no explicit `DbTransaction`
is active:

| Policy | Behavior |
|---|---|
| `FailFast` | Default. nORM attempts `DbConnection.EnlistTransaction`; failures throw `NormConfigurationException`. |
| `BestEffort` | nORM attempts enlistment; failures are logged and operations may commit independently of the ambient scope. |
| `Ignore` | nORM skips ambient enlistment and attempts to de-enlist from provider auto-enlistment. |

`FailFast` is the v1 default because silently committing outside a
`TransactionScope` is usually worse than failing early.

## Command Binding

Commands created by nORM must bind to `Database.CurrentTransaction` when it is
present. This includes query execution, raw SQL, stored procedures, direct
writes, and bulk operations. Migration runners pass the active migration
transaction directly to migration bodies.

## Savepoints

Savepoints are explicit provider operations. Callers pass the `DbTransaction`
they want to operate on to `CreateSavepointAsync` and `RollbackToSavepointAsync`.
Provider support varies; unsupported providers throw a provider-specific
unsupported-feature exception.

## Guidance

- Prefer explicit `DbTransaction` for application-level atomicity.
- Use ambient `TransactionScope` only with providers and deployment environments
  known to support enlistment.
- Treat `BestEffort` and `Ignore` as explicit opt-ins to provider-specific
  behavior, not as portable atomicity guarantees.
