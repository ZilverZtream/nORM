# Raw SQL Security

nORM raw query APIs are read-only query APIs, not a general SQL command escape
hatch.

## Safe Defaults

- Prefer `FromSqlInterpolatedAsync<T>` and `QueryUnchangedInterpolatedAsync<T>`
  when runtime values appear in SQL. Interpolation holes are converted into
  database parameters before execution.
- `FromSqlRawAsync<T>` and `QueryUnchangedAsync<T>` are for explicit parameter
  names and advanced read-only SQL. They still run through the same raw query
  validation path.
- Raw query APIs accept a single read-only `SELECT` statement or CTE. DML, DDL,
  administrative commands, stored procedure execution, and stacked statements
  are rejected before command execution.

## Provider-Aware Gate

`NormValidator.ValidateRawQuerySql` is the v1 raw query gate used by
`FromSqlRawAsync<T>` and `QueryUnchangedAsync<T>`.

| Provider | Raw query validation |
| --- | --- |
| SQL Server | Normalized denylist first, then ScriptDom statement allowlist when available, with a structural SELECT/CTE fallback if ScriptDom cannot parse the statement. |
| PostgreSQL | Normalized denylist plus structural SELECT/CTE allowlist. |
| MySQL | Normalized denylist plus structural SELECT/CTE allowlist. |
| SQLite | Normalized denylist plus structural SELECT/CTE allowlist. |

All providers reject side-effect keywords after comment and whitespace
normalization, so obfuscated forms such as `DR/**/OP` are handled before the
provider-specific gate runs.

## Privileged Escape Hatches

Stored procedures, migrations, scaffolding, and direct `DbConnection` access are
privileged paths. They are not automatically tenant-filtered and are not treated
as raw read-only query APIs. Use them only where the application owns the SQL
shape and security boundary.

## Tenant Boundary

Raw SQL does not automatically inject tenant predicates. Multi-tenant
applications must include tenant predicates explicitly or rely on database
row-level security. See `docs/multi-tenancy-security.md` for the complete
tenant boundary.
