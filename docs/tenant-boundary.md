# Tenant Boundary

Tenant boundary, not tenant convenience.

nORM's v1 tenant contract applies to generated query and write paths when
`DbContextOptions.TenantProvider` is configured. Tenant mode is fail-closed:
every generated tenant-bound path requires the mapped entity to expose the
configured tenant column and requires the provider to return a non-null tenant
ID. The boundary is enforced by generated predicates, write `WHERE` clauses,
tenant-aware cache keys, and deterministic configuration errors. It is not a
promise that caller-authored SQL is rewritten.

## Guarantees

- Generated `Query<T>()` paths include the current tenant predicate.
- Generated update/delete paths include the current tenant predicate in the
  write `WHERE` clause.
- Insert paths validate the entity tenant value against the current tenant.
- Tenant IDs are bound as parameters and coerced to the mapped tenant column
  type where safe.
- Missing tenant columns, null tenant IDs, incompatible tenant IDs, and tenant
  provider failures fail deterministically with nORM exception types instead of
  leaking provider-specific errors or silently broadening scope.
- Compiled query and result-cache identities include tenant context.
- Temporal `AsOf` queries keep tenant predicates and are no-tracking snapshots.
- Temporal restore and entity-scoped pruning use generated tenant-scoped paths.
- `GetTenantBoundaryDiagnostics<T>()` returns a redacted predicate shape for
  operational audit without exposing the tenant value.
- SQL Server and PostgreSQL can optionally receive the current tenant in
  provider-native session context before generated commands execute, so
  database RLS policies can act as defense in depth.

## Non-Guarantees

- Raw SQL is caller-controlled and receives no automatic tenant injection.
- Stored procedures are caller-controlled and receive no automatic tenant
  injection.
- Migrations, scaffolding, direct `DbConnection` usage, and manual DDL/DML are
  privileged paths.
- Tables without the configured tenant column cannot participate in generated
  tenant mode; nORM fails closed instead of running an unscoped generated path.
- nORM does not infer tenant ownership for arbitrary provider-native SQL.
- nORM does not silently install or migrate provider-native RLS policies.
  `GenerateNativeTenantPolicySql<T>()` returns reviewable DDL, while
  `ApplyNativeTenantPolicyAsync<T>()` and `DropNativeTenantPolicyAsync<T>()`
  are explicit deployment operations for applications that want nORM to execute
  the reviewed policy script through the active connection/transaction.

## Failure Behavior

Tenant provider values that cannot be used safely fail before executing unsafe
SQL. Missing mapped tenant columns, null tenant values, provider failures, and
unsafe tenant-ID coercions throw `NormConfigurationException`. Unsupported
tenant-bound shapes throw `NormUnsupportedFeatureException` where the issue is
a deliberate v1 boundary. The intended outcome is deterministic failure, not
silent semantic drift.

## Evidence Matrix

| Path | Tenant behavior | Evidence |
| --- | --- | --- |
| `Query<T>()` | Tenant predicate injected | `LiveProviderMultiTenancySecurityTests`, `TenantProjectionAndTemporalTrackingTests` |
| `CountAsync` hot path | Tenant predicate injected without falling back to the full translator for simple counts | `AdversarialTenantFuzzTests`, `TenantTemporalBenchmarks` |
| Missing tenant column | Generated tenant path fails closed | `TenantFailClosedTests` |
| Null/provider failure tenant ID | Generated tenant path fails closed | `TenantFailClosedTests`, `TenantNullHandlingTests`, `AdversarialTenantFuzzTests` |
| Tenant ID type coercion | Safe int/long/string/Guid coercion; unsafe values throw nORM exception | `TenantTypeMismatchTests`, `TenantWriteCoercionTests`, `ParameterBindingParityTests` |
| DTO projection | Tenant predicate applies before `Select` | `TenantProjectionAndTemporalTrackingTests` |
| `Include().AsSplitQuery()` | Parent and child queries tenant-filtered | `IncludeTenantIsolationTests`, `TenantTemporalProviderSwapTests`, sample app dashboard |
| Many-to-many include/sync | Join operations scoped through tenant-visible ends | `M2MTenantIsolationTests`, `IncludeProcessorCoverageTests` |
| `SaveChanges` update/delete | Tenant predicate included in write `WHERE` | `TenantWriteIsolationTests`, `LiveProviderMultiTenancySecurityTests` |
| Insert/Add + `SaveChangesAsync` | Tenant value validated/stamped | `TenantWriteCoercionTests`, `TenantNullHandlingTests`, `LiveProviderMultiTenancySecurityTests` |
| `BulkUpdateAsync` / `BulkDeleteAsync` | Tenant predicate included in provider SQL | `BulkTenantIsolationTests`, `AdversarialBulkTenantTests` |
| `ExecuteUpdateAsync` / `ExecuteDeleteAsync` | Tenant predicate included in generated DML | `LiveProviderMultiTenancySecurityTests`, sample app scenario |
| Compiled query | Tenant is part of plan/cache identity | `CompiledQueryCrossTypeTenantIsolationTests`, `LiveProviderMultiTenancySecurityTests` |
| Shared result cache | Tenant included in cache key | `TenantIsolationStressTests`, `MultiTenantResultCachePoisoningTests` |
| Temporal `AsOf` | Tenant predicate still applies; snapshots are no-tracking | `TenantProjectionAndTemporalTrackingTests`, `TenantTemporalProviderSwapTests` |
| Temporal history/diff | History rows and diff entries are read only from tenant-visible history | `TemporalLifecycleHardeningTests` |
| Temporal restore | History read, current-row check, and update remain tenant-scoped; missing current rows are not reinserted | `TemporalLifecycleHardeningTests` |
| Temporal pruning | Entity-scoped pruning includes the active tenant predicate; global tag pruning is rejected in tenant mode | `TemporalLifecycleHardeningTests` |
| Tenant diagnostics | Redacted predicate shape reports table, tenant column, parameter name, and tenant ID type | `TenantBoundaryDiagnosticsTests` |
| Native tenant session context | SQL Server/PostgreSQL session state is stamped before generated commands; SQLite/MySQL fail closed if requested | `NativeTenantSecurityTests`, `LiveProviderNativeTenantSecurityTests` |
| Native RLS policy DDL | SQL Server/PostgreSQL generate explicit, reviewable policy scripts for mapped tenant tables; explicit apply/drop APIs execute those scripts through nORM; live tests install policies and prove direct read/write enforcement under non-bypass roles | `NativeTenantSecurityTests`, `LiveProviderNativeTenantSecurityTests`, `docs/tenant-database-native-rls.md` |
| Raw SQL | No automatic tenant injection | privileged path; see `docs/raw-sql-security.md` |
| Stored procedure | No automatic tenant injection | privileged path; see `docs/stored-procedures.md` |
| Migrations/scaffolding/direct connection | Caller-controlled | privileged path |

## Live Provider Evidence

`LiveProviderMultiTenancySecurityTests` runs on SQLite, SQL Server,
PostgreSQL, and MySQL when the live provider connection strings are configured.
It proves generated select/update/delete isolation, tenant parameterization,
insert stamping, and compiled query tenant enforcement.

`TenantTemporalProviderSwapTests` runs the sample store scenario on the same
provider matrix. It proves tenant filtering, cross-tenant zero-row writes,
`Include().AsSplitQuery()`, compiled queries, bulk insert, and temporal
`AsOf(tag)` interaction under the active tenant.

`LiveProviderNativeTenantSecurityTests` runs where SQL Server/PostgreSQL live
connection strings are configured. It proves that enabling
`EnableNativeTenantSessionContext()` writes the active tenant into provider
session state on the same connection before generated queries execute.

## Operational Guidance

Treat raw SQL, stored procedures, migrations, scaffolding, and direct
connections as privileged. Review those paths the same way you would review
handwritten ADO.NET. Supply tenant predicates yourself and test them against the
same live provider matrix when they are part of a tenant boundary.

Tenant predicates are ordinary SQL predicates. Production schemas should index
the configured tenant column, usually as the leading column in composite indexes
that match common tenant-scoped filters such as `(TenantId, IsActive)` or
`(TenantId, CreatedAt)`. nORM enforces the boundary; it does not create every
workload-specific tenant index automatically.

Use `GetTenantBoundaryDiagnostics<T>()` in health checks, support tooling, or
startup validation when operators need to confirm that a mapped entity is
tenant-scoped. The diagnostic result is intentionally redacted: it reports the
predicate shape and tenant ID type, not the tenant value.

Use `EnableNativeTenantSessionContext()` only for providers that support native
session context. Pair it with reviewed SQL Server/PostgreSQL policy DDL from
`GenerateNativeTenantPolicySql<T>()` when you want database-native RLS as a
second boundary under nORM's generated predicates.
