# Logging and Redaction

nORM redacts sensitive values by default in framework-owned diagnostics.

## Default Policy

- SQL string literals are redacted before nORM writes SQL to `ILogger`.
- Parameter values passed through `LogQuery` are redacted by default.
- Connection strings are validated separately from redaction. Runtime code uses
  the original validated connection string and only emits redacted strings in
  diagnostics.
- Bulk-operation logs include operation name, table name, row count, and timing,
  not per-row values.
- Exceptions keep the original inner exception but nORM-generated messages must
  not add unredacted connection strings or parameter values.

## Privileged Extensibility

Command interceptors receive the live `DbCommand`, including `CommandText`,
parameters, transaction, timeout, and provider-specific command state. This is a
privileged extension point. Interceptor authors must not log parameter values or
connection strings unless the application has its own sensitive-data policy.

The built-in `BaseDbCommandInterceptor` logs redacted command text and does not
log parameter values.

## Raw SQL

`FromSqlInterpolatedAsync` and `QueryUnchangedInterpolatedAsync` convert
interpolation holes to database parameters before execution. Prefer these APIs
when SQL contains runtime values. `FromSqlRawAsync` and `QueryUnchangedAsync`
remain available for explicit parameter names and advanced SQL, but callers own
the SQL shape.

## Release Gate

The v1 release gate includes tests proving that `LogQuery` does not expose SQL
literal values or parameter values by default, and that CLI/runtime connection
string handling keeps execution strings separate from redacted diagnostics.
