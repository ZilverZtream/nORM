# Provider Capabilities

Each `DatabaseProvider` exposes a `Capabilities` descriptor for startup
validation, diagnostics, and release documentation. `IsAvailableAsync` remains
the runtime probe that checks whether the driver can be loaded and a compatible
server can be reached. Provider target decisions are classified by
`ProviderMobilityTranslator.DecideProviderCapabilityProfile`, so version floors,
JSON, temporal, bulk, savepoint, and parameter-limit behavior use the same
support/severity model as strict provider mobility certification.

| Provider | Minimum Version | Notes | JSON | Temporal | Native Temporal DDL | Native Bulk Insert | Savepoints | Native Tenant Session | Driver |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQL Server | 13.0 (SQL Server 2016) | JSON support requires 2016 | Yes | Yes | Yes | Yes | Yes | Yes | `Microsoft.Data.SqlClient` |
| PostgreSQL | 12.0 | Generated identity (10) + JSON path operators (12) | Yes | Yes | No | Yes | Yes | Yes | `Npgsql` |
| MySQL | 8.0 | RENAME COLUMN requires 8.0 | Yes | Yes | No | Yes | Yes | No | `MySqlConnector` or `MySql.Data` |
| SQLite | 3.25 | RENAME COLUMN, window functions, and UPSERT require 3.25 | JSON1-dependent | Yes | No | No | Yes | No | `Microsoft.Data.Sqlite` |

MariaDB is not a separate v1 provider target in this table. The CLI accepts
`mariadb` as a MySQL-compatible alias for inventory convenience, but release
evidence must not call that a MariaDB-certified provider profile until nORM has
MariaDB-specific capability/version decisions and a live gate.

Versions in the Minimum Version column are floors enforced at runtime: connections to older
servers fail startup validation with a `NormConfigurationException` that names the actual
and minimum versions. `ProviderCapabilityContractTests` enforces that this table and the
runtime capability surface stay in sync for every column: `Capabilities.MinimumServerVersion`,
the JSON/temporal/native-bulk/savepoint flags, provider-native temporal DDL support,
provider-native tenant session support, and the driver named in `Capabilities.Notes`.
Temporal "Yes" means nORM-managed history tables/triggers, not provider-native
temporal tables; `LiveProviderTemporalParityTests` verifies the live execution
contract across all four providers. MySQL temporal history and tags use
microsecond `DATETIME(6)` windows and database-clock tag creation to avoid
same-second `AsOf(tag)` drift.

Native Temporal DDL "Yes" means the provider can generate reviewable migration
DDL for database-native temporal storage. SQL Server emits hidden period columns
and `SYSTEM_VERSIONING = ON`. When
`EnableTemporalVersioning(TemporalStorageMode.ProviderNative)` is selected,
SQL Server also translates `AsOf` with `FOR SYSTEM_TIME AS OF`. Provider-native
temporal mode is explicit and does not replace the provider-neutral default.
`ApplyProviderNativeTemporalBootstrapAsync<T>()` can execute the reviewed
bootstrap DDL through the active nORM connection and transaction.

Native Tenant Session "Yes" means nORM can write the active tenant value to a
provider session primitive for database-native RLS defense in depth. SQL Server
uses `sys.sp_set_session_context`; PostgreSQL uses `set_config`. Policy DDL is
generated for review with `GenerateNativeTenantPolicySql<T>()` and can be
executed explicitly with `ApplyNativeTenantPolicyAsync<T>()` or removed with
`DropNativeTenantPolicyAsync<T>()`. SQLite/MySQL keep generated-path tenant
enforcement and fail closed if native tenant session context is requested.

## Startup Validation

Applications that need hard startup validation should call
`provider.IsAvailableAsync()` during service startup and fail deployment if it
returns `false`. That probe validates driver availability and minimum server
version for the provider's default local connection behavior.

Every `DbContext` connection initialization also validates the actual opened
connection against `Capabilities.MinimumServerVersion`. Unsupported server
versions fail before query execution with a `NormConfigurationException` that
names the provider, actual version, and minimum supported version. That startup
failure is produced through the provider mobility translation layer so runtime
validation and certification reports do not drift.
Descriptor-only certification reports record the declared version floor but do
not fill `ActualServerVersion`; actual server-version evidence requires opening
the target connection during certification.

Applications with non-local databases should still validate the actual
configured connection during service startup. Open the configured connection,
construct the matching provider, and call `InitializeConnectionAsync` or create
a short-lived `DbContext` and run a startup probe. Then use the capability
descriptor to enforce feature requirements. For example, require
`Capabilities.SupportsNativeBulkInsert` before enabling a native bulk-only path.

## Feature Flags

Capability flags describe nORM's v1 provider contract, not every feature a
database engine might support. A provider can still choose fallback
implementations for a feature. SQLite, for example, does not advertise native
bulk insert because nORM uses optimized batched SQL rather than a provider-native
copy API.

`BulkInsert` therefore appears as an emulated warning for SQLite in the provider
mobility capability profile: generated bulk semantics may still be provider
mobile, but public claims must not describe the path as native SQLite bulk.
Provider-native tenant session context and provider-native temporal tables are
provider-bound informational capabilities because they are optional deployment
infrastructure, not generated-path portability. App code that directly opts into
provider-native tenant or temporal infrastructure is still strict-blocked unless
it is handled as reviewed provider-specific deployment work.

Concrete provider reports use `DecideProviderImplementationProfile`, which adds
translation-strategy rows beyond the public descriptor. For example, SQL Server
is reported with emulated row-tuple comparison because nORM rewrites tuple
predicates for that dialect, and SQLite is reported with emulated ordered string
aggregate behavior because the provider lacks native ordered aggregate support.
Feature-specific version floors are reported separately from provider floors;
for example, SQL Server provider startup allows 13.0, but ordered string
aggregate translation requires SQL Server 14.0+ because it uses `STRING_AGG`.
The same profile also records identifier escaping, parameter binding, paging
syntax, boolean predicates, null-safe equality, LIKE escaping, string
concatenation, DateTime/decimal/TimeSpan normalization, temporal tag clock
source, generated-key retrieval, bitwise XOR, case-sensitive string comparison,
regex translation, temporal construction/arithmetic, and SQL statement length
limits for generated SQL splitting. Regex is intentionally visible because it is
not all-four native: PostgreSQL/MySQL use provider regex primitives, SQLite
uses deterministic nORM-registered managed regex functions, and SQL Server is a deterministic
unsupported target without an explicit CLR/provider function. Temporal
construction/arithmetic rows prove the provider owns from-parts, add/subtract,
TimeSpan, and DateTimeOffset epoch/local-offset hooks instead of relying on
silent CLR fallback. Those rows make provider-swap evidence reviewable instead
of hiding dialect rewrites behind a single provider name.

Unsupported database versions and missing drivers fail early with actionable
messages instead of surfacing later as translation or command execution errors.

## Floor Feature Evidence

The minimum-version table is not complete release evidence by itself. A final
v1 RC must include live target capability reports that record actual server
versions at or above the declared floor and prove the floor-gated features below
on those targets. Descriptor-only reports are useful for review, but they must
not be treated as old-version proof.

`eng/rc-artifact-manifest.ps1` records this declared floor-feature ledger in
`artifacts/v1-rc/rc-artifacts.json` and `rc-artifacts.md`. The manifest does
not open provider connections or capture actual server versions; actual version
evidence comes from `dotnet-norm portability certify` target reports.
Live target certification also runs representative floor-feature probes against
each supplied target: JSON translation, `ROW_NUMBER` window translation,
generated-value retrieval, rename-column DDL, savepoints, and idempotent
insert/ignore semantics. A `provider-target-capability` finding means one of
those probes failed and the target report cannot be used as floor evidence.

Example live target report command:

```powershell
New-Item -ItemType Directory -Force artifacts/v1-rc/provider-target-scan | Out-Null
Set-Content artifacts/v1-rc/provider-target-scan/ProviderTargetProbe.cs 'public sealed class ProviderTargetProbe { }'
dotnet run --project src/dotnet-norm -c Release --no-build -- portability certify `
  --scan-path artifacts/v1-rc/provider-target-scan `
  --providers sqlite,sqlserver,postgres,mysql `
  --sqlite-connection "Data Source=:memory:" `
  --sqlserver-connection $env:NORM_TEST_SQLSERVER `
  --postgres-connection $env:NORM_TEST_POSTGRES `
  --mysql-connection $env:NORM_TEST_MYSQL `
  --report artifacts/v1-rc/provider-target-capabilities.json
```

Current working-tree evidence from `2026-06-10` is
`artifacts/v1-rc/provider-target-capabilities.json` /
`artifacts/v1-rc/provider-target-capabilities.html`. The report passed with
actual target versions SQLite `3.41.2`, SQL Server `16.0.1000`, PostgreSQL
`17.5`, and MySQL `8.0.46`, all at or above the documented floors. The remaining
warnings are provider profile decisions, not failed floor probes.

| Provider | Floor | Floor-gated features that need live evidence |
| --- | --- | --- |
| SQL Server | 13.0 / SQL Server 2016 | `JSON_VALUE` JSON translation; `ROW_NUMBER`/window translation; `IDENTITY` plus `OUTPUT` generated-value retrieval; `sp_rename` column rename; `SAVE TRANSACTION` savepoints; `IF NOT EXISTS` idempotent join-table insert; nORM-managed temporal history/triggers; provider-native temporal DDL and `AS OF` translation; native bulk insert; native tenant session context |
| PostgreSQL | 12.0 / PostgreSQL 12 | `jsonb` JSON translation; `ROW_NUMBER`/window translation; `GENERATED AS IDENTITY` plus `RETURNING` generated-value retrieval; `ALTER TABLE RENAME COLUMN`; `SAVEPOINT`; `ON CONFLICT DO NOTHING` idempotent join-table insert; nORM-managed temporal history/triggers; native bulk insert; native tenant session context |
| MySQL | 8.0 / MySQL 8.0 | `JSON_EXTRACT` JSON translation; `ROW_NUMBER`/window translation; `AUTO_INCREMENT` plus `LAST_INSERT_ID` generated-value retrieval; `ALTER TABLE RENAME COLUMN`; `SAVEPOINT`; `INSERT IGNORE` idempotent join-table insert; nORM-managed temporal history/triggers; native bulk insert |
| SQLite | 3.25 / SQLite 3.25 | JSON1 `json_extract` JSON translation; `ROW_NUMBER`/window translation; rowid/`AUTOINCREMENT` generated-value retrieval; `ALTER TABLE RENAME COLUMN`; `SAVEPOINT`; `INSERT OR IGNORE` idempotent join-table insert; nORM-managed temporal history/triggers |

Feature-specific floors above the provider floor must stay visible in provider
mobility reports. For example, SQL Server provider startup allows 13.0, but
ordered string aggregate translation requires SQL Server 14.0+ because it uses
`STRING_AGG`.
