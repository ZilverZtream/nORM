# Production Operations

This page is the v1 operational checklist for running nORM in application
services.

## Provider Setup

Install `nORM` plus the database driver required by the provider:

- SQL Server: `Microsoft.Data.SqlClient` is included by the runtime package.
- SQLite: `Microsoft.Data.Sqlite` is included by the runtime package.
- PostgreSQL: install `Npgsql` and use `new PostgresProvider()`.
- MySQL/MariaDB: install `MySqlConnector` or `MySql.Data` and use
  `new MySqlProvider()`.

Provider capabilities and limitations are listed in
`docs/provider-capabilities.md`.

## Context Lifetime

Use one `DbContext` per unit of work. Do not share a single context across
concurrent requests. Keep the ADO.NET connection lifetime explicit:

```csharp
await using var connection = new NpgsqlConnection(connectionString);
await connection.OpenAsync(cancellationToken);

await using var context = new DbContext(
    connection,
    new PostgresProvider(),
    new DbContextOptions());
```

For web applications, create a context in the request scope and dispose it before
the request completes. For background workers, clear the change tracker between
large batches.

## Connection Pooling

Use the database driver's built-in pooling. nORM no longer ships a public custom
connection pool; `ConnectionManager` is for topology, health checks, and
read/write routing, not for replacing provider-native pooling. Keep driver pool
limits aligned with the database server's connection limit and the host's
concurrency level.

## Timeouts and Retries

Configure operation timeouts with `TimeoutConfiguration`. Treat retries as a
database-write policy, not a blanket exception handler:

- retry transient connection/database errors only,
- do not retry caller cancellation,
- be careful retrying timed-out writes because the database outcome can be
  unknown,
- keep transaction scopes small.

## Transactions

Use `BeginTransactionAsync` for explicit transaction ownership. Commit and
rollback behavior is documented in `docs/transactions.md`. Migration runners use
provider-specific transaction and lock semantics documented in
`docs/migration-provider-contract.md`.

## Migrations

For production deployments:

- run migrations from a single deployment step,
- set lock/cancellation timeouts,
- review generated destructive operations before committing migration files,
- keep migration history tables protected from ad-hoc cleanup scripts,
- test recovery from partial MySQL DDL failures.

## Multi-Tenancy

Treat tenant filters as a security boundary only for the documented protected
paths. Raw SQL, stored procedures, and direct SQL identifiers are caller-owned
unless explicitly documented otherwise. See `docs/multi-tenancy-security.md`.

## Raw SQL

Prefer interpolated raw SQL APIs:

```csharp
var users = await context.FromSqlInterpolatedAsync<User>(
    $"SELECT * FROM Users WHERE Email = {email}",
    cancellationToken);
```

Use raw SQL only for query shapes outside the LINQ support matrix. Do not build
SQL identifiers from untrusted input unless they have been validated and escaped
with the active provider.

## Logging

SQL diagnostics redact literals, parameter values, and connection string secrets
by default. Sensitive-data logging must be an explicit application decision.
Interceptors receive live commands and can leak data if they log command text or
parameter values directly.

## Performance Tuning

Start with the provider matrix benchmarks for evidence. Use compiled nORM
queries for hot paths, use `AsNoTracking()` for read-only queries, batch writes
where possible, and prefer native bulk insert paths for large inserts. Benchmark
claims and baselines are governed by `docs/benchmark-governance.md`.

## Troubleshooting

- `NormUnsupportedFeatureException`: check `docs/linq-support.md`; use a
  supported LINQ shape or raw SQL.
- `NormConfigurationException`: validate provider, model, tenant, transaction,
  and options setup.
- `DbConcurrencyException`: handle merge/reload/retry workflows explicitly.
- Timeout or connection failures: inspect driver/server logs and confirm pool
  saturation, network latency, and server resource pressure.
