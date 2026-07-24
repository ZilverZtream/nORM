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
| Raw SQL with raw `Database.CurrentTransaction` | Caller | Caller | n/a |
| Migrations | Migration runner | Migration runner | n/a |
| `TransactionScope` with successful enlistment | Ambient scope | Caller scope | Deferred until ambient scope completes |
| `TransactionScope` with `BestEffort` failed enlistment | Provider operation | Provider operation | Accepted because writes commit independently |
| `TransactionScope` with `Ignore` | Provider operation | Provider operation | Accepted only when de-enlistment is safe |

## Explicit Transactions

Use `await ctx.Database.BeginTransactionAsync()` when one unit of work must span
multiple nORM calls. A context can have only one active explicit transaction.
A second `BeginTransactionAsync` call while a context transaction is active
throws `NormUsageException`.

`DbContextTransaction.CommitAsync` and `RollbackAsync` always clear
the context transaction, even if the provider throws during commit or rollback.
This prevents a failed completion attempt from poisoning the context.

Use `Database.CurrentContextTransaction` when application code needs to inspect
or reuse the active nORM transaction wrapper. It is safe in strict provider
mobility mode because it does not expose the underlying provider transaction.
`Database.CurrentTransaction` returns the raw `DbTransaction` for compatibility
code and is rejected by strict provider mobility.

Commit and rollback use `CancellationToken.None` once completion begins. At that
point cancellation cannot reliably distinguish "not committed" from "committed
but acknowledgement failed".

## External Transactions (`Database.UseTransaction`)

`Database.UseTransaction(rawTransaction)` enlists the context in a `DbTransaction`
begun by raw ADO.NET or another library on the same connection (EF Core parity). The
context does **not** take ownership — disposing the wrapper or the context never
disposes the caller's transaction.

**Contract:** commit or roll back the enlisted transaction **through the returned
wrapper** (`CommitAsync`/`RollbackAsync`); that keeps the context's tracked state
consistent. If you commit or roll back the raw handle **directly** — bypassing the
wrapper — nORM cannot observe the outcome (commit and rollback are indistinguishable
from the context's tracked state), so you **must discard the context afterwards and
not reuse it** for further `SaveChanges`. This is the same contract EF Core requires
for externally-managed transactions; reusing a context after a raw-handle rollback can
silently skip still-pending inserts whose keys were already stamped.

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

Commands created by nORM must bind to the active context transaction when it is
present. This includes query execution, raw SQL, stored procedures, direct
writes, and bulk operations. Migration runners pass the active migration
transaction directly to migration bodies.

## Savepoints

Savepoints are provider-backed transaction operations. Prefer the nORM-owned
`DbContextTransaction.CreateSavepointAsync(name)` and
`DbContextTransaction.RollbackToSavepointAsync(name)` wrapper APIs; they keep the
raw provider transaction handle hidden and are allowed in strict provider
mobility mode. The older `DbContext.CreateSavepointAsync(DbTransaction, ...)`
and `RollbackToSavepointAsync(DbTransaction, ...)` overloads remain compatibility
APIs for code that already owns a raw transaction, and strict mode rejects those
raw-handle overloads.

| Provider | Savepoint Support |
| --- | --- |
| SQLite | Yes (native `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` syntax) |
| SQL Server | Yes (`SAVE TRANSACTION` via ADO.NET savepoint API) |
| PostgreSQL | Yes (native `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` syntax) |
| MySQL | Yes (native `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` syntax) |

## Guidance

- Prefer `Database.BeginTransactionAsync()` and the `DbContextTransaction`
  wrapper for application-level atomicity.
- Use ambient `TransactionScope` only with providers and deployment environments
  known to support enlistment.
- Treat `BestEffort` and `Ignore` as explicit opt-ins to provider-specific
  behavior, not as portable atomicity guarantees.
