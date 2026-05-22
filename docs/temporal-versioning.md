# Temporal Versioning

Temporal versioning is a stable v1 feature for mapped nORM entities when
`DbContextOptions.EnableTemporalVersioning()` is enabled.

## Runtime Model

On first connection initialization, nORM creates provider-specific temporal
infrastructure for mapped entities:

- `__NormTemporalTags` stores named timestamps created by `CreateTagAsync`.
- Each mapped entity gets a `<TableName>_History` table.
- Provider-specific triggers record insert, update, and delete history rows.
- Bootstrap is idempotent and can be cancelled before or during initialization.

History tables mirror the live table column names. When the provider can
introspect live schema metadata, nORM uses live database types and nullability
for the history table. Otherwise it falls back to mapped CLR-type SQL inference.

## Query Model

`AsOf(DateTime)` queries the history table state valid at that timestamp.
`AsOf(string tagName)` resolves the tag from `__NormTemporalTags` and then uses
the resolved timestamp.

`AsOf` requires a constant `DateTime` or constant tag string in the query
expression. Unsupported dynamic temporal expressions fail during translation.

## Provider Contract

| Provider | Temporal Storage | Time Source | DDL Notes |
| --- | --- | --- | --- |
| SQL Server | History table plus DML triggers | `SYSUTCDATETIME()` | Uses SQL Server conditional DDL and trigger batches. |
| PostgreSQL | History table plus PL/pgSQL trigger function | `now()` / timestamp columns | Uses `CREATE OR REPLACE FUNCTION` and one trigger per table. |
| MySQL | History table plus DML triggers | `UTC_TIMESTAMP()` | Trigger statements are split and executed as individual commands. |
| SQLite | History table plus DML triggers | SQLite datetime expressions | Uses SQLite trigger syntax and batched SQL. |

Temporal support is implemented by nORM-managed tables/triggers, not by native
SQL Server system-versioned temporal tables.

## Migration Interaction

Temporal bootstrap creates runtime infrastructure. Application schema changes
still belong in migrations. If a mapped table changes shape, generate and review
a migration for the live table first, then let temporal bootstrap create or
refresh provider trigger definitions on the next context initialization.

Renaming a live table or column can leave old history objects behind. Treat
renames as explicit migration work: move or rename history tables and triggers in
the migration rather than relying on automatic detection.

## Operational Notes

- Temporal bootstrap requires DDL permissions.
- Bootstrap runs on context connection initialization, so production apps should
  run startup validation or migrations under an account allowed to create the
  temporal infrastructure.
- `CreateTagAsync` requires the tags table to exist and writes the current UTC
  timestamp under the supplied tag name.
- Triggers add write overhead. Bulk operations and direct writes should be
  benchmarked with temporal enabled when temporal history is part of production
  configuration.

## Test Evidence

The v1 gate covers provider DDL generation, custom column names, live schema
type mirroring, SQLite end-to-end bootstrap and tag round trips, cancellation,
idempotent bootstrap, DDL validation, provider-specific history table probes,
and temporal query translation.
