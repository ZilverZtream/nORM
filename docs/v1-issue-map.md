# v1 Issue Map

This is the execution map for the v1 blocker set. It is intentionally ordered so
API and package decisions happen before behavior freezes, and benchmark trust is
treated as a release blocker rather than marketing polish.

## Execution Order

1. Package, provider usability, and public API freeze.
2. CLI, migrations, and destructive-operation safety.
3. LINQ, client-eval, bulk, and exception semantics.
4. Provider, multi-tenant, temporal, and security contracts.
5. Benchmark governance and release evidence.
6. Production documentation and public project governance.

## Blocker Map

| ID | Area | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Ship source generator in package | Done | Package validation verifies analyzer asset under `analyzers/dotnet/cs/`. |
| 2 | Public PostgreSQL/MySQL provider constructors | Done | Public provider setup and package docs. |
| 3 | CLI connection string validation/redaction split | Done | CLI validation and redaction tests. |
| 4 | CLI non-zero failures | Done | CLI integration exit-code tests. |
| 5 | Migration code generation escaping | Done | Generated migration SQL round-trip/compile coverage. |
| 6 | v1 public API surface | Done | `tests/PublicApi.Shipped.txt` and public API policy. |
| 7 | XML documentation policy | Done | Public API documentation policy and generated docs. |
| 8 | Obsolete pre-v1 compatibility debt | Done | Compatibility decisions documented before freeze. |
| 9 | Supported SDK pin | Done | `global.json` and CI SDK validation. |
| 10 | CI release gate alignment | Done | `eng/v1-release-gate.ps1` used by CI/RC workflows. |
| 11 | Package validation and consumer tests | Done | Package validation and consumer smoke tests. |
| 12 | Package/dependency architecture | Done | `docs/provider-packages.md`. |
| 13 | Real LINQ support matrix | Done | `docs/linq-support.md`; README no longer claims full LINQ. |
| 14 | Explicit client evaluation policy | Done | Client-eval policy option and tests. |
| 15 | Structural bulk CUD validation | Done | Bulk CUD shape metadata replaces SQL-string validation. |
| 16 | Remove `DynamicInvoke` from bulk update values | Done | Safe constant/captured value extraction. |
| 17 | Sync/async policy | Done | `docs/sync-policy.md`. |
| 18 | AOT/trimming policy | Done | `docs/aot-trimming.md`. |
| 19 | Cache memory limits and diagnostics | Done | `docs/cache-policy.md` and cache diagnostics. |
| 20 | Migration destructive change gate | Done | Destructive migration generation requires explicit acknowledgement. |
| 21 | Migration provider contract | Done | `docs/migration-provider-contract.md`. |
| 22 | Scaffolding preview contract | Done | `docs/scaffolding.md`. |
| 23 | Design-time migration context factory | Done | `docs/design-time-migrations.md`. |
| 24 | Harden destructive database drop | Done | `--yes`/dry-run/protection behavior. |
| 25 | Transaction ownership contract | Done | `docs/transactions.md`. |
| 26 | Multi-tenancy security boundary | Done | `docs/multi-tenancy-security.md`. |
| 27 | Safer raw SQL APIs | Done | `FromSqlInterpolatedAsync` and `QueryUnchangedInterpolatedAsync`. |
| 28 | Logging/redaction policy | Done | `docs/logging-redaction.md`. |
| 29 | Interceptor contract | Done | `docs/interceptors.md`. |
| 30 | Provider capabilities | Done | `docs/provider-capabilities.md`. |
| 31 | Temporal versioning contract | Done | `docs/temporal-versioning.md`. |
| 32 | Bulk operation semantics | Done | `docs/bulk-operations.md`. |
| 33 | Optimistic concurrency guarantees | Done | `docs/optimistic-concurrency.md`; MySQL matched-row provider option. |
| 34 | Exception taxonomy | Done | `docs/exception-taxonomy.md`; unsupported query shapes use nORM exceptions. |
| 35 | Repository hygiene | Done | `docs/repository-hygiene.md`; scripts no longer globally ignored. |
| 36 | Source-generator dead paths | Done | Legacy excluded generator tree removed; `docs/source-generation.md`. |
| 37 | Benchmark governance | Done | `docs/benchmark-governance.md`; fair Raw ADO categories added. |
| 38 | Production operations docs | Done | `docs/production-operations.md`. |
| 39 | Public governance files | Done | `SECURITY.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SUPPORT.md`. |
| 40 | v1 issue map and execution order | Done | This file. |
| 41 | Benchmark trust and optimized Raw ADO baselines | Done | Promoted blocker; provider and SQLite join benchmarks include convenience, optimized, and prepared-optimized Raw ADO categories. |

## Closure Rule

A blocker is not considered closed unless at least one of the following is true:

- an automated test covers the behavior,
- a public doc defines the support contract,
- a release gate or CI workflow enforces it,
- the item is explicitly marked not applicable with rationale.

Benchmark-related blockers require both code-level benchmark categories and docs
that constrain public claims.
