# Migration Provider Contract

This document is the v1 migration behavior contract for the built-in migration
runners. Provider differences are intentional where the database engines differ;
the runner behavior below is the stable surface users can automate against.

## Common Contract

- Pending migrations are discovered from the configured migrations assembly,
  ordered by `Migration.Version`.
- Duplicate migration versions fail before any migration body is executed.
- Migration name drift fails when a history row for a version has a different
  migration name than the current assembly.
- `ApplyMigrationsAsync` acquires the provider's serialization lock before
  reading the pending list, except SQLite, where the transaction itself is the
  serialization mechanism.
- Commit uses `CancellationToken.None` after migration commands finish. This
  avoids reporting cancellation for a commit that may already be durable.
- Advisory-lock release is best effort and must not mask the original migration
  failure.
- `HasPendingMigrationsAsync` and `GetPendingMigrationsAsync` ensure the history
  table exists but do not acquire the deployment lock. They are status APIs, not
  deployment coordination APIs.

## Options

All built-in runners accept `MigrationOptions` in addition to the existing
constructor overloads. Defaults preserve the pre-v1 behavior:

| Option | Default | Applies To |
|---|---:|---|
| `HistoryTableName` | `__NormMigrationsHistory` | SQL Server, PostgreSQL, MySQL, SQLite |
| `LockName` | `__NormMigrationsLock` | SQL Server, MySQL |
| `LockTimeout` | 30 seconds | SQL Server, PostgreSQL, MySQL |
| `PostgresAdvisoryLockKey` | stable nORM key | PostgreSQL |

`HistoryTableName` is intentionally restricted to simple identifiers: letters,
digits, and underscores, and it cannot start with a digit. This keeps generated
history-table SQL deterministic across providers. Schema-qualified custom
history tables are not part of the v1 contract.

## Provider Matrix

| Provider | Serialization | Lock Wait | Atomicity | History Rows | Recovery Contract |
|---|---|---:|---|---|---|
| SQL Server | `sp_getapplock` with session owner | `MigrationOptions.LockTimeout` | One transaction for pending batch where DDL is transactional | Applied rows only | Failed transaction rolls back. Retry after fixing the cause. |
| PostgreSQL | `pg_try_advisory_lock(bigint)` retry loop | `MigrationOptions.LockTimeout` | One transaction for pending batch where DDL is transactional | Applied rows only | Failed transaction rolls back. Retry after fixing the cause. |
| MySQL | `GET_LOCK(name, seconds)` | `MigrationOptions.LockTimeout` rounded up to seconds | Per-migration transaction, but DDL may auto-commit | `Partial` then `Applied` | A `Partial` row blocks retry until the operator inspects schema and removes or repairs the checkpoint. |
| SQLite | `IsolationLevel.Serializable` transaction, mapped to an exclusive transaction | SQLite busy timeout / provider behavior | One exclusive transaction for pending batch | Applied rows only | Failed transaction rolls back where SQLite supports rollback for the executed DDL. Retry after fixing the cause. |

## Cancellation

Cancellation is honored while acquiring locks, opening connections, reading
history, and executing migration commands. Once the runner starts commit or
best-effort advisory-lock release, it no longer passes the caller token. That is
intentional: an interrupted commit acknowledgement can leave deployment tooling
with an ambiguous result.

## MySQL Partial State

MySQL DDL can auto-commit independently of the surrounding transaction. The
MySQL runner writes a `Partial` checkpoint before running each migration and
updates it to `Applied` after the migration succeeds. If a process exits or a
step fails after DDL has auto-committed, the next run throws with the affected
versions instead of re-running possibly destructive DDL.

Operator recovery is manual:

1. Inspect the schema and migration body.
2. Complete or revert the partially applied change.
3. Remove or correct the `Partial` history row.
4. Re-run migrations.

## Unsupported In v1

- Schema-qualified history-table names.
- Per-provider custom history schemas.
- Automatic MySQL partial-state repair.
- Running migration deployment concurrently without the built-in runner lock.
