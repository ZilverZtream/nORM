# Temporal Versioning

Temporal versioning is a stable v1 feature for mapped nORM entities when
`DbContextOptions.EnableTemporalVersioning()` is enabled.

## Support Level

The v1 contract is nORM-managed temporal history, not provider-native temporal
tables. nORM owns the `__NormTemporalTags` table, each `<TableName>_History`
table, and the provider-specific triggers/functions that populate history rows.
Applications own live-table schema migrations and any manual rename, rollback,
or cleanup work for temporal infrastructure.

Release candidates must run temporal tests in the live provider gate before
temporal claims are used in release notes. Local unit tests lock SQL generation,
custom column names, SQLite bootstrap, cancellation, idempotency, and `AsOf`
translation; live RC artifacts prove execution against the configured provider
versions. `LiveProviderTemporalParityTests` is the four-provider smoke that
proves bootstrap, tag creation, history trigger writes, and `AsOf(DateTime)`
execution on SQL Server, PostgreSQL, MySQL, and SQLite.

## Runtime Model

On first connection initialization, nORM creates provider-specific temporal
infrastructure for mapped entities:

- `__NormTemporalTags` stores named timestamps created by `CreateTagAsync`.
- Each mapped entity gets a `<TableName>_History` table.
- Provider-specific triggers record insert, update, and delete history rows.
- Bootstrap is idempotent and can be cancelled before or during initialization.
- When temporal versioning is disabled, context initialization does not create
  tags, history tables, triggers, or functions.

History tables mirror the live table column names. When the provider can
introspect live schema metadata, nORM uses live database types and nullability
for the history table. Otherwise it falls back to mapped CLR-type SQL inference.

## Query Model

`AsOf(DateTime)` queries the history table state valid at that timestamp.
`AsOf(string tagName)` resolves the tag from `__NormTemporalTags` and then uses
the resolved timestamp.

`GetTemporalHistoryAsync<T>(key)` reads the provider-neutral audit trail for a
single entity key: historical entity snapshot, `ValidFrom`, `ValidTo`, and
operation marker (`I`, `U`, or `D`). Composite-key entities are accepted through
the key-values overload when the caller supplies the mapped key values in order.
Key values are validated before SQL execution and coerced to the mapped key CLR
type where safe; null values for non-nullable keys throw
`NormUnsupportedFeatureException`.

`GetTemporalDiffAsync<T>(key)` compares consecutive temporal history versions
and returns changed mapped properties. It is intended for audit timelines,
review screens, and support workflows that need a provider-neutral diff without
handwritten history-table SQL.

`RestoreTemporalVersionAsync<T>(key, tagName)` and
`RestoreTemporalVersionAsync<T>(key, timestamp)` restore an existing current row
to the historical snapshot valid at the supplied tag or timestamp. Restore is an
update workflow, not an undelete workflow: if the current row no longer exists,
the method returns `0` and does not reinsert it. In tenant mode, restore reads
history, checks current-row visibility, and writes through tenant-scoped
generated paths, so a tenant cannot restore another tenant's row by key.

Tag names must be non-empty and no longer than 200 characters. That length is
the cross-provider v1 contract because SQL Server stores tag names in
`NVARCHAR(200)`.

`AsOf` requires a constant `DateTime` or constant tag string in the query
expression. Unsupported dynamic temporal expressions fail during translation.

Temporal queries are no-tracking snapshots. A historical row with the same
primary key as an already-tracked current row must materialize as historical
state, not alias to the tracked current entity.

Tenant predicates still apply when multi-tenancy is enabled. `AsOf(tag)` is a
generated query path, so the tenant boundary remains in force for mapped
entities with the configured tenant column.

## Provider Contract

| Provider | Temporal Storage | Time Source | DDL Notes |
| --- | --- | --- | --- |
| SQL Server | History table plus DML triggers | `SYSUTCDATETIME()` | Uses SQL Server conditional DDL and trigger batches. |
| PostgreSQL | History table plus PL/pgSQL trigger function | `now()` / timestamp columns | Uses `CREATE OR REPLACE FUNCTION` and one trigger per table. |
| MySQL | History table plus DML triggers | `UTC_TIMESTAMP(6)` | Trigger statements are split and executed as individual commands; tag/history period columns are tightened to `DATETIME(6)` on bootstrap. |
| SQLite | History table plus DML triggers | SQLite datetime expressions | Uses SQLite trigger syntax and batched SQL. |

Temporal support is implemented by nORM-managed tables/triggers, not by native
SQL Server system-versioned temporal tables.

SQL Server also exposes an explicit provider-native temporal mode for teams that
want native system-versioned tables:

```csharp
options.EnableTemporalVersioning(TemporalStorageMode.ProviderNative);
```

In that mode, SQL Server `AsOf` translation emits `FOR SYSTEM_TIME AS OF`
instead of reading nORM-managed history tables. Bootstrap emits hidden period
columns and `SYSTEM_VERSIONING = ON` with a named history table. Unsupported
providers fail closed during context initialization.

`GenerateProviderNativeTemporalBootstrapSql<T>()` returns the same SQL Server
DDL for review before deployment. Provider-native temporal mode is not the
default provider-neutral v1 storage mode. Use it only when the application
explicitly wants SQL Server-native temporal storage and is willing to own the
migration, retention, rollback, and provider-specific operational model.

`ApplyProviderNativeTemporalBootstrapAsync<T>()` is the explicit execution
companion for reviewed provider-native bootstrap DDL. It runs the provider DDL
through the active nORM connection, transaction, retry strategy, and command
interception pipeline. It does not make native temporal bootstrap automatic for
providers outside their documented support boundary.

Provider-native mode currently supports `AsOf(DateTime)`, `AsOf(tag)`, tag
creation, and SQL Server bootstrap. Provider-neutral history, diff, restore, and
pruning APIs require nORM-managed history metadata and fail with
`NormUnsupportedFeatureException` when provider-native mode is selected.

Timestamp precision and trigger clock behavior are documented in
`docs/temporal-precision.md`.

Built-in providers create temporal tags with the database server clock rather
than the application clock so tag timestamps are comparable to trigger-generated
history windows. MySQL bootstrap also upgrades an existing
`__NormTemporalTags.Timestamp` column to `DATETIME(6)` to avoid same-second
`AsOf(tag)` ambiguity in existing databases.

## Migration Interaction

Temporal bootstrap creates runtime infrastructure. Application schema changes
still belong in migrations. If a mapped table changes shape, generate and review
a migration for the live table first, then let temporal bootstrap create or
refresh provider trigger definitions on the next context initialization.

Renaming a live table or column can leave old history objects behind. Treat
renames as explicit migration work: move or rename history tables and triggers in
the migration rather than relying on automatic detection.

Rollback is also explicit. If a deployment rolls back a live-table schema change,
the rollback migration must restore or remove the matching history table columns,
triggers, and functions. nORM does not infer destructive temporal cleanup from
the runtime model.

## Operational Notes

- Temporal bootstrap requires DDL permissions.
- Bootstrap runs on context connection initialization, so production apps should
  run startup validation or migrations under an account allowed to create the
  temporal infrastructure.
- `CreateTagAsync` requires the tags table to exist and writes the current UTC
  timestamp under the supplied tag name. It participates in the current explicit
  nORM transaction when one is active. Calling it without temporal versioning
  enabled throws `NormConfigurationException`.
- Triggers add write overhead. Bulk operations and direct writes should be
  benchmarked with temporal enabled when temporal history is part of production
  configuration.
- nORM bulk insert, update, and delete paths use provider DML that fires the
  temporal triggers; `TemporalLifecycleHardeningTests` verifies SQLite trigger
  interaction and the live gate verifies provider trigger execution.
- `PruneTemporalHistoryAsync<T>(olderThan)` deletes closed history rows for one
  mapped entity. In tenant mode it includes the active tenant predicate.
- `PruneTemporalHistoryAsync(olderThan, pruneTags)` is an administrative
  cleanup path over all mapped history tables. In tenant mode, global tag
  pruning is rejected because temporal tags are shared infrastructure.
- Pruning deletes rows whose `ValidTo` is older than the cutoff. The current
  open history row for a live entity is retained until a later update or delete
  closes its validity window.
- Direct provider writes can activate triggers, but raw SQL is still a
  privileged path and must supply its own tenant predicates when tenant
  boundaries matter.
- Temporal history reads, tag creation, restore, and pruning use nORM command
  creation so active explicit transactions and command interceptors are applied
  consistently.
- `Include` composes with `AsOf`: eager-loaded relations are reconstructed
  through the same history window as the root, so the whole graph reflects one
  point in time (era-consistent values and membership). The exception is
  many-to-many navigations: the association table is a raw, user-owned table
  with no history, so `AsOf` combined with a many-to-many `Include` throws
  `NormUnsupportedFeatureException` instead of silently joining live
  associations onto historical rows. Query the historical entities without the
  many-to-many `Include`, or model the association as a mapped entity if its
  history matters.
- `AsOf` over an entity that owns collections (`OwnsMany`) also throws
  `NormUnsupportedFeatureException`: the owned child table's history rows do
  not carry the owner key, so the owned rows at a past timestamp cannot be
  correlated to their owners. Model the children as a mapped related entity
  (with `Include`, which reconstructs correctly) when their history matters.

## Test Evidence

The v1 gate covers provider DDL generation, explicit provider-native bootstrap
execution, custom column names, live schema type mirroring, SQLite end-to-end
bootstrap and tag round trips, cancellation, idempotent bootstrap, DDL
validation, provider-specific history table probes, disabled-mode no-artifact
behavior, bulk-trigger interaction, tag validation, provider-neutral history
reads and diffs, temporal restore, tenant-scoped pruning, explicit transaction
participation, and temporal query translation.

`TenantProjectionAndTemporalTrackingTests` proves `AsOf(tag)` returns historical
state even when the current entity is already tracked. `TenantTemporalProviderSwapTests`
runs the sample store tenant plus temporal scenario on SQLite, SQL Server,
PostgreSQL, and MySQL when live provider connection strings are configured.
