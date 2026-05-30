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
  SQL Server `IDENTITY(seed, increment)` metadata is emitted into generated
  context configuration with `Property(...).HasIdentityOptions(...)`, and SQL
  Server migration SQL uses the captured seed/increment when creating or
  rebuilding the table.
  SQLite rowid integer primary keys are normalized to non-null `long` properties
  even when provider schema metadata reports nullable `int`.
  SQLite declared `UUID` columns scaffold as `Guid`, while declared `JSON` and
  `XML` columns scaffold as string storage instead of being warning-only
  provider-specific type rows. JSON/XML query semantics remain ordinary string
  semantics unless the application adds explicit provider-bound queries.
  SQL Server `xml`, PostgreSQL `json`/`jsonb`/`xml`/`uuid`, and MySQL
  `json`/`year` columns likewise scaffold as safe scalar CLR storage instead
  of warning-only provider-specific type rows; native JSON/XML operator
  semantics remain provider-bound.
  PostgreSQL arrays over safe scalar elements scaffold as CLR arrays so the
  model compiles and materializes with Npgsql; they remain provider-specific
  schema diagnostics because SQL Server/MySQL/SQLite do not share native array
  DDL.
  PostgreSQL domain columns are reported with the domain schema/name and
  underlying provider type so reviewers can preserve or remodel domain-owned
  constraints before claiming provider mobility.
  SQL Server alias/user-defined column types are reported with schema/name and
  base system type when available so reviewers can preserve or remodel
  provider-owned type semantics.
  MySQL unsigned integer columns are reported as provider-specific storage
  because SQL Server, PostgreSQL, and SQLite do not share MySQL's unsigned
  integer DDL semantics.
  Composite primary keys are also emitted in generated context configuration
  with `HasKey(e => new { ... })` in provider-reported key ordinal order, so
  the reverse-engineered model carries the full key shape explicitly.
  PostgreSQL serial column defaults and their owned sequences are treated as
  identity metadata, not as separate provider-owned warning rows.
  Simple SQL defaults that pass nORM's migration default allowlist (numeric,
  boolean, `NULL`, single-quoted string literals, and known no-argument
  date/time/UUID functions) are emitted in generated context configuration via
  `Property(...).HasDefaultValueSql(...)` so schema snapshots and migration
  generators can round-trip the default metadata. This is DDL metadata only:
  it does not mark the column as database-generated and does not cause nORM to
  omit the property from runtime inserts.
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
  cascade behavior; valid database referential actions (`NO ACTION`,
  `CASCADE`, `SET NULL`, `RESTRICT`, `SET DEFAULT`) are emitted into generated
  fluent configuration, including `ON UPDATE` actions. Relationships are emitted when the FK targets the
  generated principal primary key or an exact ordered unique index exposed by provider
  metadata. Composite relationships are emitted when the ordered FK columns
  reference the exact generated composite primary key or an exact ordered unique index;
  FK shapes targeting keyless tables or non-unique alternate columns are
  reported for manual configuration instead of emitting unsafe fluent code.
  Self-referencing FKs use role-based navigation names derived from the FK
  column instead of vague same-type names.
- Single-column, composite, and multi-membership non-primary-key index
  generation through nORM's `[Index]` metadata, including unique composite
  indexes without converting them into per-column uniqueness. Provider-specific
  descending key order is preserved with `IndexAttribute.IsDescending`.
  SQL Server, PostgreSQL, and SQLite filtered/partial indexes are preserved
  with `IndexAttribute.FilterSql`; SQL Server and PostgreSQL included-column
  indexes are preserved with `IndexAttribute.IsIncluded`. Expression index
  semantics remain diagnostics rather than portable `[Index]` attributes.
- Decimal precision/scale preservation for SQL Server, PostgreSQL, and MySQL
  `decimal`/`numeric` columns. Scaffolding emits
  `[Column(TypeName = "decimal(p,s)")]`; schema snapshots read that metadata,
  and provider migration generators round-trip it instead of falling back to
  `DECIMAL(18,2)`. SQLite remains on its provider-neutral `NUMERIC` mapping.
- Pure many-to-many join table generation for the safe v1 subset: exactly two
  non-null foreign-key constraints, no payload columns, a join-table primary
  key made exactly from those FK columns, and both references targeting the
  exact generated primary keys or exact ordered unique indexes. Single-column,
  composite-key, shared-column tenant, and alternate-key pure junction tables
  are supported. Both entity sides receive collection navigations, and the join
  table is emitted as fluent `HasMany().WithMany(inverse).UsingTable(...)`
  configuration instead of a join entity. Alternate-key bridges use the
  key-selector `UsingTable` overload so runtime include/loading and join-table
  sync use the referenced unique columns instead of assuming primary keys.
  Schema-qualified join tables use the schema-aware `UsingTable` overload so
  generated SQL targets the qualified bridge table rather than a literal dotted
  table name. Self-referencing pure join tables receive distinct role-based
  navigations from the join FK columns and are consumer-build tested.
  A generated single-column surrogate primary key is also allowed when the
  table has no payload columns and an exact unique index covers the FK columns;
  the generated `UsingTable` mapping writes only the FK columns and leaves the
  surrogate key database-owned.
- Payload bridge tables are emitted as explicit join entities with payload
  columns and normal FK navigations. They are also reported as possible
  many-to-many candidates so reviewers do not accidentally collapse domain data
  into a payload-free skip navigation.
- Optional table filtering through `ScaffoldOptions.Tables`, CLI
  comma-separated `--tables`, and repeatable CLI `--table` entries. Use
  repeatable `--table` for literal table names that contain commas and must not
  be split. Null or blank API filters are treated as empty rather than
  producing raw runtime exceptions. Bare table-name filters fail with an
  actionable error when the same table name exists in multiple schemas; use a
  schema-qualified filter in that case. If a literal dotted table name collides
  with the same display string as a schema-qualified table, scaffolding fails
  deterministically because v1 filter syntax cannot disambiguate those objects.
- Optional overwrite protection through `ScaffoldOptions.OverwriteFiles` and
  CLI `--no-overwrite`. Output file conflicts are preflighted before any file
  is written, so a later collision does not leave a partially generated model.
- Optional dry-run validation through `ScaffoldOptions.DryRun` and CLI
  `--dry-run`. Dry runs perform schema discovery and in-memory generation
  without creating the output directory, deleting stale warning reports, or
  writing generated files. The CLI dry run uses an isolated temporary output
  directory so it can print the same warning summary as a real run while
  leaving the requested output path untouched.
- Optional warning enforcement through `ScaffoldOptions.FailOnWarnings` and CLI
  `--fail-on-warnings`, which fails the scaffold run after writing
  `nORM.ScaffoldWarnings.md` and `nORM.ScaffoldWarnings.json`.
- Optional provider-bound routine wrappers through
  `ScaffoldOptions.EmitRoutineStubs` and CLI `--emit-routine-stubs`.
  Stored procedure wrappers call nORM's stored-procedure APIs with the
  discovered schema-qualified routine name. SQL Server scalar/table-valued
  functions, PostgreSQL functions, and MySQL functions are discovered as
  routines and emitted as provider-bound `SELECT` wrappers (`SELECT
  function(...) AS Value` for scalar functions and `SELECT * FROM
  function(...)` for table-valued functions) instead of being miscalled as
  stored procedures. Table-valued functions also receive a streaming wrapper so
  callers can consume large provider-owned rowsets without buffering. When
  provider metadata exposes safe
  input parameter names, scaffolding emits a nested parameter DTO with known CLR
  scalar types (`int?`, `decimal?`, `DateTime?`, `DateTimeOffset?`,
  `DateOnly?`, `TimeOnly?`, `TimeSpan?`, `Guid?`, `string?`, `byte[]?`,
  etc.) and falls back to `object?` only for unmapped provider types. PostgreSQL
  routine parameters preserve common array metadata (`int[]?`, `string[]?`,
  `Guid[]?`, temporal arrays, etc.) and known user-defined textual types such
  as `citext`. SQL Server scalar alias types are mapped through their base
  system type; SQL Server table-valued parameters scaffold as `DbParameter?`
  so callers pass reviewed provider-specific structured parameters. MySQL
  unsigned routine parameters use unsigned CLR types (`uint?`, `ulong?`,
  `ushort?`, `byte?`) when the provider exposes the modifier. Function
  wrappers whose provider parameter names cannot safely become C# DTO
  properties use positional `object?[]` arguments with an exact scaffolded
  argument-count guard instead of silently dropping or misbinding required
  function arguments. Stored-procedure wrappers with such parameter names use
  `IReadOnlyDictionary<string, object?>` arguments so callers can pass exact
  provider parameter names without unsafe DTO generation.
  Stored-procedure stubs include both a buffered `Task<List<TResult>>` wrapper
  and a streaming `IAsyncEnumerable<TResult>` wrapper for large result sets.
  When safe
  output parameter metadata is available, scaffolding also emits an
  `OutputParameter[]` factory with mapped `DbType` values, discovered
  string/binary sizes where providers expose them, and `InputOutput` direction
  for INOUT parameters and `ReturnValue` direction for provider return-value
  metadata. The generated output wrapper includes a convenience overload that
  uses the scaffold-discovered output definitions, plus an explicit
  `params OutputParameter[]` overload for reviewed routine signature changes.
  Scalar-function stubs also include a direct `Task<TValue?> ...ValueAsync`
  convenience wrapper so callers do not need to hand-author a one-column
  result DTO for the generated `Value` projection.
  XML comments list the discovered parameter metadata, including sized types
  such as `nvarchar(32)` and `decimal(18,2)`. Routine
  bodies remain provider-owned and are not translated across database engines.
- Optional provider-bound standalone sequence wrappers through
  `ScaffoldOptions.EmitSequenceStubs` and CLI `--emit-sequence-stubs`.
  SQL Server and PostgreSQL standalone sequences are discovered with scalar
  value type metadata where the provider exposes it, and generated context
  methods retrieve the next value with provider SQL (`NEXT VALUE FOR` or
  PostgreSQL `nextval(...::regclass)`). Sequence DDL, allocation/caching, and
  cross-provider migration remain provider-owned.
- Optional query-artifact entities through
  `ScaffoldOptions.EmitQueryArtifacts` (or the compatibility alias
  `EmitViewEntities`) and CLI `--emit-query-artifacts` or
  `--emit-view-entities`.
  Generated view/materialized-view/SQLite virtual-table classes are intended
  for reads; scaffolding still reports missing primary keys where the database
  does not expose one. Keyless generated types are marked with
  `[ReadOnlyEntity]`, so nORM can materialize them through queries but rejects
  insert/update/delete and tracked `SaveChanges` writes before SQL generation.
  SQL Server synonyms whose local base object resolves as a table or view can
  also be emitted as read-oriented query artifacts; procedure synonyms, remote
  synonyms, and unresolved synonyms remain provider-owned diagnostics.
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
  do not target the generated principal primary key or an exact ordered unique index
  are listed there instead of being silently ignored or converted into fake
  single-column navigations;
  unsafe/provider-specific defaults that fail the safe default allowlist,
  provider-specific column types that cannot be represented as safe scalar
  storage,
  SQL Server rowversion/timestamp columns,
  unparsed provider-specific identity strategy metadata, unrecognized FK referential actions,
  relationships that do not target the generated principal primary key or an
  exact ordered unique index,
  and triggers are inventoried for review; SQL Server provider-native temporal tables
  are reported as provider-owned schema; tables without primary keys are emitted
  as read-only generated types and still reported so reviewers know writes and
  navigations require a real key; views,
  PostgreSQL materialized views, SQLite virtual tables, routines, sequences,
  SQL Server synonyms, SQLite virtual-table shadow tables, and MySQL events are
  discovered and reported as skipped database objects unless an explicit
  opt-in emits the supported query-artifact, routine-stub, or sequence-stub shape; likely
  many-to-many join tables are flagged when they are scaffolded as normal
  entities.

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
  SQL Server identity seed/increment emission through generated
  `HasIdentityOptions(...)` configuration,
  composite alternate-key FK generation when the target is an exact ordered unique index,
  FK cascade/non-cascade preservation, computed/generated column write
  exclusion, relationship suppression when the principal key cannot be
  generated safely, schema-qualified many-to-many join table preservation,
  self-referencing FK role-based navigation naming,
  self-referencing pure many-to-many join scaffolding,
  composite-key pure many-to-many join scaffolding,
  provider-specific partial/expression/included-column index diagnostics,
  descending index metadata round-tripping,
  safe default-to-`HasDefaultValueSql` promotion, unsafe composite-FK,
  many-to-many candidate, and provider-owned schema diagnostics.
- `CliIntegrationTests.Scaffold_sqlite_output_builds_as_consumer_project`
  proves `dotnet-norm scaffold` output builds in a consumer project, including
  quoted/backslash/XML-sensitive table and column identifiers.
- `ScaffoldingAndNavigationCoverageTests` also proves opt-in routine wrapper
  output compiles as a consumer project, keeps the provider-bound routine
  warning/metadata contract explicit, and emits SQL Server/PostgreSQL/MySQL
  functions as `SELECT` wrappers instead of stored-procedure calls, including
  buffered and streaming table-valued function wrappers.
- `ScaffoldingAndNavigationCoverageTests` proves opt-in SQL Server/PostgreSQL
  sequence wrappers generate typed next-value methods and compile in a consumer
  project.
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
  shapes, provider-owned/default-promotion and
  keyless-table diagnostics, and skipped-view table-filter failures against
  SQLite and any configured SQL Server, PostgreSQL, and MySQL live providers.
- `RelationshipConfigurationTests` covers generated non-cascade relationship
  metadata and proves the public fluent API keeps cascade behavior explicit.
- `MigrationFluentSnapshotTests` and `MigrationDefaultsIdentityTests` prove
  fluent identity options flow into schema snapshots and SQL Server
  `IDENTITY(seed, increment)` migration DDL.
- `UpdateNoMutableColumnsTests` covers generated/computed-column exclusion from
  insert and update column sets.
- `PublicApiSnapshotTests` and `PublicApiClassificationTests` keep scaffold
  API additions intentional and documented.

## Not Yet Stable

- Relationship generation beyond primary keys and exact ordered unique indexes.
  Single-column and composite FK constraints that target the exact generated
  primary key or an exact ordered unique index are emitted as navigations and fluent
  model configuration. FKs that target keyless or non-unique alternate columns
  are discovered and reported in scaffold diagnostics.
- FK referential-action modeling outside the common provider action set.
  `NO ACTION`, `CASCADE`, `SET NULL`, `RESTRICT`, and `SET DEFAULT` are emitted
  into generated fluent configuration for delete/update actions. Unknown
  provider-specific action tokens are discovered and reported in scaffold
  diagnostics.
- Owned types and inheritance inference.
- Payload bridge tables are modeled as explicit join entities, not skip
  navigations. Many-to-many joins whose bridge columns are nullable, whose key
  shape is neither an exact FK-column primary key nor a generated surrogate key
  plus exact FK-column unique index, or whose foreign keys do not target the
  generated primary keys or exact ordered unique indexes are reported in scaffold
  diagnostics rather than converted into unsafe fluent mappings.
- Provider-specific complex default constraints, column types, triggers,
  temporal tables, and keyless tables.
  Simple safe default literals/functions are emitted as migration metadata with
  `HasDefaultValueSql`; table CHECK constraints are emitted as provider-bound
  migration metadata with `HasCheckConstraint`; computed/generated column
  expressions are emitted as provider-bound migration metadata with
  `HasComputedColumnSql`; column collations are emitted as provider-bound
  migration metadata with `HasCollation`; complex/provider-specific defaults
  that fail the allowlist remain diagnostics. Safe scalar JSON/XML/UUID storage
  types are promoted into generated CLR properties where provider metadata
  exposes them; native JSON/XML operators remain provider-bound. PostgreSQL
  arrays over safe scalar elements are emitted as CLR arrays but kept in
  diagnostics as provider-specific schema. Parsed SQL Server
  `IDENTITY(seed, increment)` metadata is emitted with
  `HasIdentityOptions`; provider-specific column types, unparsed identity
  strategies, unrecognized FK referential actions, triggers, SQL Server provider-native temporal
  tables, keyless tables, SQLite virtual-table shadow tables, sequences,
  unresolved/non-query synonyms, and events are discovered and reported in
  scaffold diagnostics, but not converted into complete provider-neutral model
  code. Views, materialized views, SQLite virtual tables, and resolved
  table/view SQL Server synonyms can be emitted as opt-in query artifacts with
  explicit keyless warnings, and routine wrappers can be emitted as opt-in
  provider-bound call stubs.
  Routine diagnostics include provider metadata such as parameter counts,
  output-parameter counts where the provider exposes them, ordered parameter
  mode/type summaries, INOUT direction, output string/binary sizes where
  providers expose them, and result/data type hints so stored
  procedures/functions are not lost during provider-mobility review.
- Provider-specific expression indexes on SQLite and PostgreSQL are emitted as
  provider-bound `HasExpressionIndex` migration metadata. SQL Server and MySQL
  expression-index shapes require generated/computed columns plus ordinary
  indexes or provider-specific migration code. v1 scaffolding emits
  provider-neutral key-column indexes, including descending key order for ordinary column indexes, provider-bound
  filtered/partial index metadata for SQL Server, PostgreSQL, and SQLite, and
  provider-bound included-column index metadata for SQL Server and PostgreSQL.

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
norm scaffold --provider sqlite --connection "Data Source=legacy.db" --table "Keep,Me"
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --dry-run
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
  `payload-columns`, `nullable-foreign-key`,
  `foreign-key-metadata-incomplete`, `missing-primary-key`,
  `primary-key-not-exact-bridge-columns`, or `principal-key-not-primary-key`.

Use `code` and `category` for CI baselines, owner routing, and remediation
dashboards. Use routine `metadata` for stored procedure/function migration
inventory. Do not parse `detail` or `suggestedAction` text as a stable API.

### Diagnostic Code Catalog

| Code | Category | Meaning |
| --- | --- | --- |
| `SCF001` | `relationship` | Unsupported composite foreign key discovered; scalar columns are generated, but no navigation is emitted because it does not target the generated principal primary key or an exact ordered unique index. |
| `SCF002` | `many-to-many` | Possible many-to-many table discovered. Pure single-column, composite-key, alternate-key, and generated-surrogate-key bridges can be generated as `UsingTable`; payload-capable, nullable, keyless, or non-unique bridges stay as join entities until explicitly modeled. |
| `SCF100` | `schema-feature` | Database default expression discovered. |
| `SCF101` | `schema-feature` | Computed/generated column expression discovered but not emitted. Ordinary generated-column expressions are emitted as `HasComputedColumnSql`. |
| `SCF102` | `schema-feature` | Check constraint discovered but not emitted. Ordinary table CHECK constraints are emitted as `HasCheckConstraint`. |
| `SCF103` | `schema-feature` | Provider/database collation discovered but not emitted because no generated property could safely own it. Ordinary column collations are emitted as `HasCollation`. |
| `SCF104` | `schema-feature` | Provider-specific column type discovered. SQLite declared `UUID`, `JSON`, and `XML`, SQL Server `xml`, PostgreSQL `json`/`jsonb`/`xml`/`uuid`, and MySQL `json`/`year` are scaffolded as supported scalar storage; provider-specific declarations such as `GEOMETRY` remain diagnostics. |
| `SCF106` | `relationship` | Non-default FK referential action discovered. |
| `SCF107` | `relationship` | FK targets principal columns that are neither the generated primary key nor an exact ordered unique index. |
| `SCF108` | `schema-feature` | Provider rowversion/timestamp column discovered. |
| `SCF109` | `schema-feature` | Provider-specific identity strategy discovered. SQL Server `IDENTITY(seed, increment)` is emitted as `HasIdentityOptions`; unparsed strategies remain diagnostics. |
| `SCF110` | `database-object` | Trigger discovered. |
| `SCF111` | `index` | Filtered/partial index discovered. |
| `SCF112` | `index` | Expression index discovered but not emitted. SQLite/PostgreSQL expression indexes are emitted as `HasExpressionIndex`. |
| `SCF113` | `index` | Included-column index discovered. |
| `SCF114` | `index` | Descending index key discovered. |
| `SCF115` | `database-object` | Provider-native temporal table discovered. |
| `SCF116` | `table-shape` | Table has no primary key. |
| `SCF199` | `schema-feature` | Unknown provider-owned schema feature. |
| `SCF200` | `query-object` | View discovered; skipped unless emitted through `--emit-query-artifacts` / `--emit-view-entities`. |
| `SCF201` | `routine` | Routine/stored procedure/function discovered; skipped unless provider-bound stubs are emitted through `--emit-routine-stubs`. |
| `SCF202` | `key-generation` | Standalone sequence discovered and skipped. |
| `SCF203` | `database-object` | SQL Server synonym discovered and skipped. Local table/view synonyms can be emitted through `--emit-query-artifacts`; procedure, remote, or unresolved synonyms remain diagnostics. |
| `SCF204` | `query-object` | PostgreSQL materialized view discovered; skipped unless emitted through `--emit-query-artifacts` / `--emit-view-entities`. |
| `SCF205` | `routine` | MySQL event discovered and skipped. |
| `SCF206` | `virtual-table` | SQLite virtual table discovered; skipped unless emitted as a query artifact through `--emit-query-artifacts` / `--emit-view-entities`. |
| `SCF207` | `virtual-table` | SQLite virtual-table shadow table discovered and skipped. |
| `SCF299` | `database-object` | Unknown skipped database object. |

For v1 runtime mapping, `UsingTable` skip navigations support single-column,
composite-key, shared-column tenant, and alternate-key pure junction tables when
both sides reference either the generated primary keys or exact ordered unique indexes.
Unsafe bridge shapes remain explicit join entities.

The report is additive: new fields may be added in later versions, but v1 tools
should tolerate unknown fields and should not treat an empty diagnostics file as
provider-mobility evidence.
