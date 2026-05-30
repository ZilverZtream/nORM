# Scaffolding Contract

Scaffolding ships as a bounded v1 tooling surface with the scope below. The
public entry points (`nORM.Scaffolding.DatabaseScaffolder`,
`nORM.Scaffolding.DynamicEntityTypeGenerator`, and the `dotnet-norm scaffold`
command) are API-snapshotted, but the generated model is intentionally a
bootstrap artifact, not a database-first completeness claim.

The "Supported" section is the v1 scaffolding contract; everything in "Not Yet
Stable" is explicitly out of scope for v1.0 and tracked for v1.x. Generated code
must be reviewed and edited like handwritten model code.

## Supported

- Table discovery for SQL Server, PostgreSQL, MySQL, and SQLite.
- Schema-qualified table names are preserved for SQL Server, PostgreSQL, and
  SQLite attached databases. MySQL scaffolding uses the current database for
  discovery but does not emit the database/catalog name as a model schema.
- Entity class generation with `[Table]`, `[Column]`, `[Key]`, provider metadata-backed identity,
  computed/generated-column metadata, and simple `[Required]`/`[MaxLength]` annotations
  when provider metadata exposes them. SQLite rowid integer primary keys,
  SQL Server identity columns, PostgreSQL identity/serial columns, and MySQL
  `AUTO_INCREMENT` columns are marked with `DatabaseGeneratedOption.Identity`.
  SQLite rowid integer primary keys are normalized to non-null `long` properties
  even when provider schema metadata reports nullable `int`.
  Composite primary keys are also emitted in generated context configuration
  with `HasKey(e => new { ... })` so the reverse-engineered model carries the
  full key shape explicitly.
  PostgreSQL serial column defaults and their owned sequences are treated as
  identity metadata, not as separate provider-owned warning rows.
  Computed/generated columns are marked
  with `DatabaseGeneratedOption.Computed` so nORM does not treat them as normal
  insert columns, but their provider expressions remain provider-owned DDL.
  SQL Server rowversion/timestamp columns are marked as `[Timestamp]` and
  database-generated for optimistic concurrency, while the provider DDL remains
  outside the generated model.
- Nullable reference/value type generation from database nullability metadata.
- Non-null reference properties are initialized with `default!` so generated
  nullable-enabled code compiles cleanly before the application adds its own
  constructors or required-member style.
- `DbContext` generation with `IQueryable<T>` properties backed by nORM's
  query provider.
- Provider-specific identifier escaping for the zero-row schema query.
- Literal table and column identifiers discovered from metadata are escaped as
  single database object names, so names containing dots (for example
  `audit.events` or `value.part`) are not silently reinterpreted as
  schema-qualified identifiers unless a schema is supplied separately.
- Dynamic table generation also rejects ambiguous dotted names when the same
  input could mean either a literal table name or a schema-qualified table.
- Generated C# identifiers are sanitized: invalid characters become `_`,
  leading digits are prefixed with `_`, and C# keywords use `@`.
- Generated class and property names are de-duplicated deterministically when
  different database identifiers normalize to the same C# identifier. Property
  and navigation names also avoid inherited `object` member names such as
  `ToString`, `Equals`, `GetHashCode`, and `GetType`.
- When the same table name appears in multiple schemas, generated entity names
  include the schema name instead of relying on opaque numeric suffixes.
- The requested namespace is validated before files are written, and the
  generated context file name follows the escaped context class name rather
  than the raw CLI/API input. If that context name collides with an entity
  class, scaffolding uses a unique context class/file name instead of
  overwriting the entity file.
- Single-column foreign key relationship generation and safe composite foreign
  key relationship generation when provider metadata
  exposes the constraint. Generated entities include reference/collection
  navigations; single-column references include `[ForeignKey]` metadata, and
  composite references are wired through ordered fluent `HasForeignKey`
  selectors. The generated `DbContext` preserves caller-supplied model
  configuration. `ON DELETE CASCADE` is preserved as nORM tracked-graph
  cascade behavior; non-cascade delete actions are generated with
  `cascadeDelete: false`. Relationships are emitted when the FK targets the
  generated principal primary key or an exact unique index exposed by provider
  metadata. Composite relationships are emitted when the ordered FK columns
  reference the exact generated composite primary key or an exact unique index;
  FK shapes targeting keyless tables or non-unique alternate columns are
  reported for manual configuration instead of emitting unsafe fluent code.
  Self-referencing FKs use role-based navigation names derived from the FK
  column instead of vague same-type names.
- Single-column, composite, and multi-membership non-primary-key index
  generation through nORM's `[Index]` metadata, including unique composite
  indexes without converting them into per-column uniqueness. Provider-specific
  filtered/partial, expression, included-column, and descending-key index
  semantics are reported as diagnostics rather than emitted as portable
  `[Index]` attributes.
- Decimal precision/scale preservation for SQL Server, PostgreSQL, and MySQL
  `decimal`/`numeric` columns. Scaffolding emits
  `[Column(TypeName = "decimal(p,s)")]`; schema snapshots read that metadata,
  and provider migration generators round-trip it instead of falling back to
  `DECIMAL(18,2)`. SQLite remains on its provider-neutral `NUMERIC` mapping.
- Pure many-to-many join table generation for the safe v1 subset: exactly two
  non-null foreign-key constraints, no payload columns, a join-table primary
  key made exactly from those FK columns, and both references targeting the
  exact generated primary keys. Single-column and composite-key pure junction
  tables are supported. Both entity sides receive collection navigations, and the join table is emitted as fluent
  `HasMany().WithMany(inverse).UsingTable(...)` configuration instead of a join
  entity. Schema-qualified join tables use the schema-aware `UsingTable`
  overload so generated SQL targets the qualified bridge table rather than a
  literal dotted table name. Self-referencing pure join tables receive distinct
  role-based navigations from the join FK columns and are consumer-build tested.
- Payload bridge tables are emitted as explicit join entities with payload
  columns and normal FK navigations. They are also reported as possible
  many-to-many candidates so reviewers do not accidentally collapse domain data
  into a payload-free skip navigation.
- Optional table filtering through `ScaffoldOptions.Tables` and CLI
  `--tables`; null or blank API filters are treated as empty rather than
  producing raw runtime exceptions. Bare table-name filters fail with an
  actionable error when the same table name exists in multiple schemas; use a
  schema-qualified filter in that case. If a literal dotted table name collides
  with the same display string as a schema-qualified table, scaffolding fails
  deterministically because v1 filter syntax cannot disambiguate those objects.
- Optional overwrite protection through `ScaffoldOptions.OverwriteFiles` and
  CLI `--no-overwrite`. Output file conflicts are preflighted before any file
  is written, so a later collision does not leave a partially generated model.
- Optional warning enforcement through `ScaffoldOptions.FailOnWarnings` and CLI
  `--fail-on-warnings`, which fails the scaffold run after writing
  `nORM.ScaffoldWarnings.md` and `nORM.ScaffoldWarnings.json`.
- Optional provider-bound routine wrappers through
  `ScaffoldOptions.EmitRoutineStubs` and CLI `--emit-routine-stubs`.
  Generated context methods call nORM's stored-procedure APIs with the
  discovered schema-qualified routine name. When provider metadata exposes safe
  input parameter names, scaffolding emits a nested parameter DTO with known CLR
  scalar types (`int?`, `decimal?`, `DateTime?`, `Guid?`, `string?`, `byte[]?`,
  etc.) and falls back to `object?` only for unmapped provider types. When safe
  output parameter metadata is available, scaffolding also emits an
  `OutputParameter[]` factory with mapped `DbType` values. XML comments list the
  discovered parameter metadata. Routine bodies remain provider-owned and are
  not translated across database engines.
- Optional query-artifact entities through
  `ScaffoldOptions.EmitViewEntities` and CLI `--emit-view-entities`.
  Generated view/materialized-view/SQLite virtual-table classes are intended
  for reads; scaffolding still reports missing primary keys where the database
  does not expose one, and nORM does not infer provider-neutral write semantics
  for query artifacts.
- Warning reports are deterministic per run: if a later scaffold produces no
  diagnostics, stale `nORM.ScaffoldWarnings.*` files are removed when overwrite
  is allowed, or reported as an error when overwrite protection is enabled.
- Table discovery and generated output are ordered deterministically so repeated
  scaffolds of the same schema produce reviewable diffs instead of provider
  metadata-order churn. Relationship navigations and fluent relationship
  configuration are also ordered by generated names rather than raw provider FK
  discovery order.
- Deterministic Markdown and JSON diagnostics for discovered database features
  that are not converted into runnable model code. Composite foreign keys that
  do not target the generated principal primary key or an exact unique index
  are listed there instead of being silently ignored or converted into fake
  single-column navigations;
  defaults, computed/generated columns, check
  constraints, provider-specific collations, provider-specific column types,
  SQL Server rowversion/timestamp columns,
  non-default SQL Server identity seed/increment settings, non-default FK referential actions,
  relationships that do not target the generated principal primary key or an
  exact unique index,
  and triggers are inventoried for review; SQL Server provider-native temporal tables
  and tables without primary keys are reported as provider-owned schema; SQLite virtual tables and their shadow tables,
  views, routines, sequences, SQL Server synonyms, PostgreSQL materialized
  views, and MySQL events are discovered and reported as skipped database
  objects; likely many-to-many join tables are flagged when they are scaffolded
  as normal entities.

## Evidence

- `ScaffoldingAndNavigationCoverageTests` covers identifier normalization,
  duplicate generated-name handling, table filtering, overwrite protection,
  deterministic repeated output, nullable initialization, SQLite FK navigation generation, SQLite
  composite-FK navigation/model configuration generation, and SQLite
  single-column/composite index generation and columns that participate in
  multiple indexes, plus role-based naming for duplicate relationships,
  composite primary-key source generation with consumer-build evidence,
  decimal precision/scale preservation across generated `[Column(TypeName)]`,
  schema snapshots, and migration SQL generators,
  composite alternate-key FK generation when the target is an exact unique index,
  FK cascade/non-cascade preservation, computed/generated column write
  exclusion, relationship suppression when the principal key cannot be
  generated safely, schema-qualified many-to-many join table preservation,
  self-referencing FK role-based navigation naming,
  self-referencing pure many-to-many join scaffolding,
  composite-key pure many-to-many join scaffolding,
  provider-specific partial/expression/included-column/descending index diagnostics,
  unsafe composite-FK, many-to-many candidate, and provider-owned schema diagnostics.
- `CliIntegrationTests.Scaffold_sqlite_output_builds_as_consumer_project`
  proves `dotnet-norm scaffold` output builds in a consumer project, including
  quoted/backslash/XML-sensitive table and column identifiers.
- `ScaffoldingAndNavigationCoverageTests` also proves opt-in routine wrapper
  output compiles as a consumer project and keeps the provider-bound routine
  warning/metadata contract explicit.
- `ScaffoldingAndNavigationCoverageTests` proves opt-in query-artifact
  scaffolding converts SQLite views and SQLite virtual tables into compiling
  query artifacts while preserving missing-primary-key/shadow-table diagnostics.
- `LiveProviderScaffoldingParityTests` proves opt-in view entity scaffolding
  emits compiling query artifacts and explicit missing-primary-key diagnostics
  across configured SQLite, SQL Server, PostgreSQL, and MySQL providers.
- `SchemaSignatureTests` covers dynamic scaffolding schema signatures,
  duplicate generated property handling, quoted and dotted literal identifier
  preservation, computed/identity/rowversion metadata in the dynamic cache key
  and generated attributes, non-null reference-column `[Required]` parity, and
  connection ownership for sync/async dynamic scaffolding calls.
- `DynamicTypeQueryTests` proves `DbContext.Query(string)` materializes rows
  when runtime-generated table or column mappings contain literal dotted
  identifiers.
- `LiveProviderScaffoldingParityTests` covers single-column FK relationship
  scaffolding, composite-FK relationship generation when the FK targets the
  generated primary key, composite-FK diagnostics for unsupported relationship
  shapes, provider-owned/default and
  keyless-table diagnostics, and skipped-view table-filter failures against
  SQLite and any configured SQL Server, PostgreSQL, and MySQL live providers.
- `RelationshipConfigurationTests` covers generated non-cascade relationship
  metadata and proves the public fluent API keeps cascade behavior explicit.
- `UpdateNoMutableColumnsTests` covers generated/computed-column exclusion from
  insert and update column sets.
- `PublicApiSnapshotTests` and `PublicApiClassificationTests` keep scaffold
  API additions intentional and documented.

## Not Yet Stable

- Relationship generation beyond primary keys and exact unique indexes.
  Single-column and composite FK constraints that target the exact generated
  primary key or an exact unique index are emitted as navigations and fluent
  model configuration. FKs that target keyless or non-unique alternate columns
  are discovered and reported in scaffold diagnostics.
- Full FK referential-action modeling beyond tracked-graph cascade on/off.
  Non-cascade delete actions and update actions are discovered and reported in
  scaffold diagnostics; provider DDL remains the source of truth.
- Owned types and inheritance inference.
- Payload bridge tables are modeled as explicit join entities, not skip
  navigations. Many-to-many joins whose bridge columns are nullable, missing a primary key made exactly from the FK columns, or whose foreign keys do not
  target the generated primary keys are reported in scaffold diagnostics rather
  than converted into unsafe fluent mappings.
- Provider-specific computed columns, default constraints, check constraints,
  collations, column types, triggers, temporal tables, and keyless tables.
  Defaults, computed/generated columns, check constraints, collations,
  provider-specific column types, non-default identity
  seed/increment settings, triggers, SQL Server provider-native temporal
  tables, keyless tables, SQLite virtual-table shadow tables, sequences,
  synonyms, and events are discovered and reported in scaffold diagnostics, but
  not converted into complete provider-neutral model code. Views and
  materialized views and SQLite virtual tables can be emitted as opt-in query
  artifacts with explicit keyless warnings, and routine wrappers can be emitted
  as opt-in provider-bound call stubs.
  Routine diagnostics include provider metadata such as parameter counts,
  output-parameter counts where the provider exposes them, ordered parameter
  mode/type summaries, and result/data type hints so stored
  procedures/functions are not lost during provider-mobility review.
- Provider-specific filtered/partial indexes, expression indexes,
  included-column indexes, and descending index key sort direction. These are
  discovered and reported for review; v1 scaffolding emits provider-neutral
  key-column indexes only.

## v1 Guidance

Use scaffolding to bootstrap a model, then edit the generated files into the
model you want to own. For production applications, commit the generated code
and review diffs like handwritten model code.

The `dotnet-norm scaffold` command shares the v1 contract above. It uses the
same `DatabaseScaffolder` implementation as the runtime API; both surfaces
support the same "Supported" scope and have the same out-of-scope items.

Examples:

```bash
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --namespace App.Data
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --tables public.customer,public.order --no-overwrite --fail-on-warnings
```

Do not use scaffolding as evidence for provider mobility by itself. Provider
mobility is proven by generated nORM query/write paths, strict certification,
provider capability reports, and live provider gates after the model exists.

## Warning Report Shape

When diagnostics exist, scaffolding writes both `nORM.ScaffoldWarnings.md` and
`nORM.ScaffoldWarnings.json`. The JSON report is intended for CI checks and has
these top-level fields:

- `version`
- `summary`
- `compositeForeignKeys`
- `possibleManyToManyJoinTables`
- `providerOwnedSchemaFeatures`
- `skippedDatabaseObjects`

The `summary` field contains `totalWarnings`, `sectionCounts`, `codes`, and
`categories` so CI can enforce budgets or route remediation without walking
every report section.

Each warning row includes a `suggestedAction` value. The action is intentionally
specific enough for CI/reporting tools to show the next manual step: add
explicit model configuration, keep provider-owned DDL in migrations, replace a
pure join entity with `UsingTable`, add a primary key, or hand-write a
provider-bound routine/view path.

Each row also includes stable diagnostic metadata:

- `code`: machine-readable scaffold diagnostic code such as `SCF001`,
  `SCF100`, or `SCF200`.
- `severity`: currently `Warning` for every report row emitted by v1
  scaffolding.
- `category`: a coarse bucket such as `relationship`, `schema-feature`,
  `index`, `database-object`, `query-object`, or `virtual-table`.
- `metadata`: a structured object for rows that have provider-owned details.
  For `SCF201` routine rows this includes `provider`, `routineType`,
  `parameterCount`, `outputParameterCount`, optional routine `dataType`, and
  ordered `parameters` entries with `name`, `mode`, and provider `dataType`
  when the database exposes it.
- `reasons`: present on `SCF002` possible many-to-many rows. Values explain
  why the bridge was not emitted as `UsingTable`, for example
  `payload-columns`, `nullable-foreign-key`, `composite-foreign-key`,
  `missing-primary-key`, `primary-key-not-exact-bridge-columns`, or
  `principal-key-not-single-column-primary-key`.

Use `code` and `category` for CI baselines, owner routing, and remediation
dashboards. Use routine `metadata` for stored procedure/function migration
inventory. Do not parse `detail` or `suggestedAction` text as a stable API.

### Diagnostic Code Catalog

| Code | Category | Meaning |
| --- | --- | --- |
| `SCF001` | `relationship` | Unsupported composite foreign key discovered; scalar columns are generated, but no navigation is emitted because it does not target the generated principal primary key or an exact unique index. |
| `SCF002` | `many-to-many` | Possible many-to-many table discovered. Pure single-column and composite-key bridges can be generated as `UsingTable`; payload-capable, nullable, keyless, or non-primary-key bridges stay as join entities until explicitly modeled. |
| `SCF100` | `schema-feature` | Database default expression discovered. |
| `SCF101` | `schema-feature` | Computed/generated column expression discovered. |
| `SCF102` | `schema-feature` | Check constraint discovered. |
| `SCF103` | `schema-feature` | Provider/database collation discovered. |
| `SCF104` | `schema-feature` | Provider-specific column type discovered. SQLite custom declarations such as `JSON`, `GEOMETRY`, and `UUID` are included here. |
| `SCF106` | `relationship` | Non-default FK referential action discovered. |
| `SCF107` | `relationship` | FK targets principal columns that are neither the generated primary key nor an exact unique index. |
| `SCF108` | `schema-feature` | Provider rowversion/timestamp column discovered. |
| `SCF109` | `schema-feature` | Non-default identity strategy discovered. |
| `SCF110` | `database-object` | Trigger discovered. |
| `SCF111` | `index` | Filtered/partial index discovered. |
| `SCF112` | `index` | Expression index discovered. |
| `SCF113` | `index` | Included-column index discovered. |
| `SCF114` | `index` | Descending index key discovered. |
| `SCF115` | `database-object` | Provider-native temporal table discovered. |
| `SCF116` | `table-shape` | Table has no primary key. |
| `SCF199` | `schema-feature` | Unknown provider-owned schema feature. |
| `SCF200` | `query-object` | View discovered; skipped unless emitted through `--emit-view-entities`. |
| `SCF201` | `routine` | Routine/stored procedure/function discovered; skipped unless provider-bound stubs are emitted through `--emit-routine-stubs`. |
| `SCF202` | `key-generation` | Standalone sequence discovered and skipped. |
| `SCF203` | `database-object` | SQL Server synonym discovered and skipped. |
| `SCF204` | `query-object` | PostgreSQL materialized view discovered and skipped. |
| `SCF205` | `routine` | MySQL event discovered and skipped. |
| `SCF206` | `virtual-table` | SQLite virtual table discovered; skipped unless emitted as a query artifact through `--emit-view-entities`. |
| `SCF207` | `virtual-table` | SQLite virtual-table shadow table discovered and skipped. |
| `SCF299` | `database-object` | Unknown skipped database object. |

For v1 runtime mapping, `UsingTable` skip navigations support single-column and
composite-key pure junction tables when both sides reference the generated
primary keys. Unsafe bridge shapes remain explicit join entities.

The report is additive: new fields may be added in later versions, but v1 tools
should tolerate unknown fields and should not treat an empty diagnostics file as
provider-mobility evidence.
