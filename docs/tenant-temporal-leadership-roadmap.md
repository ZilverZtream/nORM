# Tenant And Temporal Leadership Roadmap

This document is intentionally not release marketing. It defines what nORM
would need before claiming market-leading tenant or temporal capability.

## Current Claim Boundary

RC3 can claim a bounded best-in-class position for generated-path tenant
boundaries plus provider-neutral nORM-managed temporal history on the supported
v1 provider matrix. That claim is scoped: nORM can stamp SQL Server/PostgreSQL
native tenant session context and generate reviewable RLS policy DDL, but it
does not silently install or own database-native RLS policies, does not rewrite
caller-authored SQL, and does not use SQL Server system-versioned temporal
tables for v1 temporal support.

## Tenant: Market-Leading Scope

| Area | Required capability | Evidence required |
| --- | --- | --- |
| Fail-closed defaults | Missing tenant provider, unsupported tenant type, or mismatched tenant metadata fails with nORM exceptions before SQL execution. | Unit tests and live-provider tests. |
| Generated queries | Tenant predicates apply before projection, include, split query, grouping, paging, compiled query, and cache paths. | Matrix rows plus live-provider tests. |
| Generated writes | `SaveChanges`, `ExecuteUpdate`, `ExecuteDelete`, bulk update/delete, sync, and navigation writes scope writes by tenant. | Live write-isolation tests for all providers. |
| Tenant types | `int`, `long`, `string`, and `Guid` tenants bind consistently across all supported providers. | Type-coercion tests across provider matrix. |
| Cache identity | Result cache, compiled-query cache, materializer cache, and model cache cannot cross-contaminate tenants. | Stress tests and adversarial cache tests. |
| Privileged paths | Raw SQL, stored procedures, migrations, scaffolding, and direct connections are clearly documented as caller-controlled. | Documentation contract tests. |
| Database-native integration | Optional SQL Server/PostgreSQL row-level-security session context, reviewable policy DDL, and explicit apply/drop deployment APIs are available as defense in depth without replacing generated-path predicates. | `NativeTenantSecurityTests`, `LiveProviderNativeTenantSecurityTests`, `docs/tenant-database-native-rls.md`; hidden automatic policy installation remains out of scope. |
| Multi-tenant topology | Shared-table, database-per-tenant, and connection-per-tenant patterns are documented with lifecycle and pooling guidance. | `docs/tenant-deployment-patterns.md` and sample configuration. |
| Operational audit | `GetTenantBoundaryDiagnostics<T>` reports a redacted generated predicate shape for support and startup validation. | `TenantBoundaryDiagnosticsTests` and sample dashboard output. |

## Temporal: Market-Leading Scope

| Area | Required capability | Evidence required |
| --- | --- | --- |
| Provider-neutral reads | `AsOf(DateTime)` and `AsOf(tag)` behave consistently across SQLite, SQL Server, PostgreSQL, and MySQL. | Live-provider temporal parity tests. |
| SQL Server native bridge | Explicit provider-native mode can bootstrap SQL Server system-versioned temporal tables, execute reviewed bootstrap DDL through nORM, and translate `AsOf` with `FOR SYSTEM_TIME AS OF`. | `ProviderNativeTemporalTests` plus `LiveProviderNativeTemporalTests` for SQL Server native bootstrap and `AsOf` execution. |
| Tenant interaction | Temporal queries still enforce tenant predicates and cannot alias tracked current entities. | Tenant-temporal provider-swap tests. |
| Bootstrap lifecycle | Temporal bootstrap is idempotent, cancellable, permission-aware, and fails deterministically. | Unit and live-provider tests. |
| Transaction participation | Temporal tags, history reads, restore, and pruning bind active nORM transactions and command interceptors. | `TemporalLifecycleHardeningTests` and `LiveProviderTemporalParityTests`. |
| Bulk interaction | Bulk inserts, updates, deletes, and fallback paths have explicit temporal behavior. | Bulk-temporal parity tests. |
| Schema evolution | Renames, type changes, rollbacks, and migration repair paths are documented and tested where supported. | Migration caveat docs and migration tests. |
| Restore workflows | `RestoreTemporalVersionAsync<T>` restores an existing current row from a tag or point in time without undelete side effects. | `TemporalLifecycleHardeningTests`; sample workflow still required. |
| History inspection | `GetTemporalHistoryAsync<T>` and `GetTemporalDiffAsync<T>` support timeline and changed-property views without handwritten history-table SQL. | `TemporalLifecycleHardeningTests` and sample UI. |
| Retention/pruning | `PruneTemporalHistoryAsync<T>` supports tenant-scoped entity cleanup; administrative all-table cleanup can prune global tags outside tenant mode. | `TemporalLifecycleHardeningTests`; live-provider pruning evidence still required. |
| Trigger overhead | Benchmark evidence quantifies write overhead with temporal enabled and disabled. | Benchmark artifacts and governance rows. |
| Precision model | Time precision, clock source, and provider-specific timestamp behavior are documented. | `docs/temporal-precision.md` and temporal parity tests. |

## Release Position

The RC3 position should be:

> nORM demonstrates a bounded best-in-class tenant and temporal niche:
> generated-path tenant boundaries and provider-neutral nORM-managed temporal
> history across the supported provider matrix.

The claim remains bounded by the explicit non-goals above.
