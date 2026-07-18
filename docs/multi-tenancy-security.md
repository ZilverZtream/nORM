# Multi-Tenancy Security Contract

nORM multi-tenancy is a security boundary for ORM-generated access paths when
`DbContextOptions.TenantProvider` is configured and mapped entities expose the
configured `TenantColumnName`. It is not a replacement for database-level row
security on caller-authored SQL.

## Threat Model Summary

| Protected paths (tenant filter automatically applied) | Bypass-capable paths (caller must enforce tenant isolation) |
| --- | --- |
| All ORM queries through `IQueryable<T>` | Raw SQL (`ExecuteRawSqlAsync`) |
| Compiled queries | Stored procedures |
| Include/EagerLoad | Migrations |
| Change tracking save operations | Scaffolding |
| Bulk CUD operations | Direct connection access (`DatabaseFacade.GetDbConnection()`) |

## Threat Model

nORM protects normal application reads and writes from accidentally reading,
modifying, deleting, caching, or relating rows owned by another tenant. The
current tenant value is supplied by `ITenantProvider.GetCurrentTenantId()` and is
bound as a SQL parameter or validated against entity values.

The tenant provider must be trusted application infrastructure. If application
code supplies the wrong tenant ID, nORM will consistently enforce the wrong
tenant. Use database row-level security or separate databases when the database
must independently verify tenant identity.

## Boundary Inventory

| Boundary | Protected by nORM | Bypass-capable APIs | Required control |
| --- | --- | --- | --- |
| ORM-generated reads | `Query<T>()`, compiled queries, eager-loaded includes, **composable `FromSqlRaw<T>` / `FromSqlInterpolated<T>`** (the tenant filter is applied to the outer query over the raw SQL wrapped as a derived table), result cache keys | terminal `FromSqlRawAsync<T>`, `QueryUnchangedAsync<T>`, direct `DbCommand` | Add tenant predicates manually (or use database row-level security) for caller-authored SQL executed through the terminal raw APIs; the composable `FromSqlRaw` path enforces tenant automatically. |
| ORM-generated writes | `SaveChangesAsync`, direct writes, bulk update/delete, owned/many-to-many maintenance | direct `DbCommand`, migrations, admin CLI commands | Restrict privileged write paths and review tenant predicates in hand-authored SQL. |
| Relationship loading | mapped `Include`, `ThenInclude` eager loading | raw SQL joins, stored procedures | Verify related tables carry tenant columns or enforce tenancy in database code. |
| Cache isolation | query/result cache keys include tenant state where nORM owns the key | external cache providers, application-level cache keys | Prefix external keys/tags with tenant identity. |
| Operational tooling | none; tooling is administrative | migrations, scaffolding, database drop/update commands | Run tooling only with deployment/admin permissions and disposable or approved targets. |

## Enforced Paths

These paths are tenant-scoped when a tenant provider is configured and the
mapped entity has the tenant column:

| Path | Contract |
| --- | --- |
| LINQ queries from `Query<T>()` | A tenant predicate is added before SQL generation. The tenant value is parameterized and coerced to the mapped tenant property type. |
| Compiled queries | Compiled query cache keys include tenant state, and execution still binds the current tenant value. |
| `Include` eager loading | Child and join-table loaders add tenant predicates where the related mapping has a tenant column. |
| `SaveChangesAsync` inserts | Entity tenant values are validated against the current tenant. nORM does not auto-fill a missing tenant value for non-null tenants. |
| `SaveChangesAsync` updates/deletes | Generated `WHERE` clauses include the current tenant predicate. Optimistic-concurrency verification is also tenant-scoped. |
| `InsertAsync`, `UpdateAsync`, `DeleteAsync` | Direct entity writes validate tenant values and scope updates/deletes by tenant. |
| `BulkUpdateAsync` and `BulkDeleteAsync` | Bulk CUD plans include the tenant predicate and reject unsupported shapes before execution. |
| Owned and many-to-many persistence | Owned-child and join-table maintenance is tenant-scoped where tenant columns are mapped. Cross-tenant relation writes are rejected. |
| Query result caching | Tenant provider state is included in cache keys. `NormMemoryCacheProvider` can also prefix tags with the tenant ID. |

Tenant values are compared with the same coercion model used by query filters.
For example, a provider returning boxed `long` can match an `int` tenant column
when the value is convertible. A non-nullable tenant column with a null tenant ID
throws before SQL is generated.

## Caller-Controlled Paths

These paths are privileged or caller-controlled. nORM does not inject tenant
predicates into them:

| Path | Required user action |
| --- | --- |
| `FromSqlRawAsync<T>` and `QueryUnchangedAsync<T>` | Include the tenant predicate yourself, or rely on database row-level security. Parameters are validated, but SQL shape remains caller-owned. |
| `ExecuteStoredProcedure*` APIs | The procedure must enforce tenant isolation internally or receive a tenant parameter and use it. |
| `DbConnection` and `DbCommand` used directly | Caller owns all tenant filtering and secret handling. |
| Migrations and schema management | Migrations run as privileged DDL and are outside row-level tenant isolation. |
| Scaffolding and admin CLI commands | These inspect or mutate schema and are operational/admin features, not tenant-filtered application queries. |
| `DbContext.Query(string)` dynamic-table access | The dynamic entity query path still uses normal query translation, but the caller owns the table identifier and must only expose approved table names. |

Raw SQL is intentionally explicit. nORM validates obvious unsafe SQL patterns and
parameter usage, but it cannot determine whether caller-authored SQL is supposed
to be tenant-scoped. Public APIs and docs should therefore describe raw SQL and
stored procedures as bypass-capable privileged paths.

Stored procedure calls should pass the current tenant explicitly unless the
database enforces row-level security:

```csharp
var tenantId = tenantProvider.GetCurrentTenantId();
var rows = await context.ExecuteStoredProcedureAsync<UserStats>(
    "reporting.GetUserStatsForTenant",
    parameters: new { TenantId = tenantId, StartDate = startDate });
```

The procedure body must use `TenantId` in every tenant-owned read or write. See
`docs/stored-procedure-security.md` for the stored procedure validation and
review contract.

## Operational Requirements

- Configure `TenantProvider` and `TenantColumnName` together. nORM rejects a
  tenant provider without a tenant column name.
- Map the tenant column on every tenant-owned entity, including owned child
  tables and relation targets that must be isolated.
- Keep tenant IDs stable for the lifetime of a `DbContext` operation. Do not
  switch ambient tenant state during query enumeration or `SaveChangesAsync`.
- Use a tenant-aware cache provider or rely on nORM query cache key tenant
  scoping. Shared external caches must include tenant state in keys and tags.
- Treat raw SQL, stored procedures, migrations, scaffolding, and direct
  connection access as privileged APIs that need code review.

## Test Evidence

The v1 gate includes tenant isolation coverage for query filters, null/type
coercion, compiled query cache isolation, result caching, tracked writes, direct
writes, bulk CUD, owned entities, many-to-many relations, overlapping foreign
keys, adversarial tenant IDs, and parallel tenant stress.
