# Stored Procedure Security

Stored procedure APIs are privileged execution paths. nORM does not rewrite a
procedure body, inject tenant predicates, or prove that a procedure is read-only.

## Runtime Validation

- Providers whose `StoredProcedureCommandType` is `StoredProcedure` accept only
  simple or schema-qualified procedure identifiers such as `GetUsers` or
  `reporting.GetUsers`.
- Raw SQL command text, `EXEC ...` text, whitespace-separated command text, and
  stacked statements are rejected by stored procedure APIs.
- SQLite exposes stored procedure APIs as `CommandType.Text` because SQLite does
  not have stored procedures. In that provider mode, nORM applies the same
  read-only raw query gate used by `FromSqlRawAsync<T>`.
- Output parameter names must start with a letter or underscore and contain only
  letters, digits, and underscores.

## Tenant-Safe Pattern

Stored procedures do not receive automatic tenant filters. Multi-tenant code
must pass the tenant ID explicitly or rely on database row-level security.

```csharp
var tenantId = tenantProvider.GetCurrentTenantId();
var rows = await context.ExecuteStoredProcedureAsync<UserStats>(
    "reporting.GetUserStatsForTenant",
    parameters: new { TenantId = tenantId, StartDate = startDate });
```

The procedure body must use the tenant parameter in every read and write:

```sql
SELECT UserId, OrderCount
FROM reporting.UserStats
WHERE TenantId = @TenantId
  AND CreatedAt >= @StartDate;
```

## Review Rule

Treat every stored procedure call as privileged application code. Code review
must verify the procedure name is fixed or allowlisted, all runtime values are
parameters, tenant isolation is enforced inside the procedure, and procedure
permissions follow least privilege.

## Scaffolding Inventory

`dotnet-norm scaffold` and `DatabaseScaffolder` report routines as
provider-bound objects by default. When `--emit-routine-stubs` or
`ScaffoldOptions.EmitRoutineStubs` is enabled, the generated context also
includes convenience wrapper methods that call nORM's stored-procedure APIs
with the discovered schema-qualified routine name. These wrappers do not
translate procedure/function bodies, do not add tenant predicates, and do not
make the routine provider-mobile.

On SQL Server, PostgreSQL, and MySQL the diagnostic detail includes routine
metadata such as parameter count, ordered parameter mode/type summaries,
output-parameter count when available, and declared result/data type hints.
The JSON warning report also exposes structured `metadata` for routine rows so
CI and migration tools can classify provider-bound routines without parsing the
human `detail` string. Use that report as migration inventory: each routine
needs either a generated nORM query/write replacement, a provider-bound
deployment path, or an explicit tenant/security review.
