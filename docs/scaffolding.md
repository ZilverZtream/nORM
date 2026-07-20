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
  when provider metadata exposes bounded facets; unbounded provider length
  sentinels are not emitted as `[MaxLength]`. Schema snapshots and provider
  migration generators round-trip bounded string/binary facets where the target
  database has a safe bounded DDL form: SQL Server emits
  `NVARCHAR(n)`/`VARCHAR(n)`/`NCHAR(n)`/`CHAR(n)` plus
  `VARBINARY(n)`/`BINARY(n)` from captured Unicode and fixed-length metadata,
  PostgreSQL emits `VARCHAR(n)`/`CHAR(n)` for strings and keeps `BYTEA`
  unbounded, and MySQL emits `VARCHAR(n)`/`CHAR(n)` plus
  `VARBINARY(n)`/`BINARY(n)` only within conservative row-size-safe bounds.
  Generated contexts preserve those facets with
  `Property(...).HasMaxLength(n)`, `Property(...).IsUnicode(false)`, and
  `Property(...).IsFixedLength()` where provider catalogs expose them. Fluent
  `Property(...).HasMaxLength(n)`, `Property(...).IsUnicode(...)`,
  `Property(...).IsFixedLength()`, and decimal
  `Property(...).HasPrecision(p, s)`/`Property(...).HasPrecision(p)` feed the
  same schema snapshot metadata for hand-tuned models. The EF-parity column verbs
  `Property(...).IsRequired()`, `HasColumnType(...)`, `HasDefaultValue(...)`/
  `HasDefaultValueSql(...)`, `HasComment(...)`, `IsRowVersion()`, and
  `ValueGeneratedOnAdd()`/`ValueGeneratedNever()`/`ValueGeneratedOnAddOrUpdate()`
  feed the same snapshot so migrations round-trip nullability, store type, defaults,
  comments, and concurrency/generation metadata (see the README "Fluent model
  configuration" section).
  SQLite rowid integer primary keys,
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
  Provider catalog `date`/`time` store types map to CLR temporal types where the mapping is unambiguous:
  SQL Server and PostgreSQL `date`/`time`, MySQL `date`, PostgreSQL `interval`,
  PostgreSQL `time with time zone`, SQL Server `datetimeoffset`, and SQLite declared
  `DATE`/`TIME`/`DATETIME`/`TIMESTAMP`/`DATETIMEOFFSET` are applied before
  falling back to provider schema-row CLR metadata. MySQL `TIME` is left to
  provider metadata rather than guessed because it can represent time-of-day or
  elapsed duration depending on schema intent.
  Real-provider static, runtime dynamic, and real CLI scaffold tests cover this
  temporal store-type mapping across SQLite, SQL Server, PostgreSQL, and MySQL,
  including the intentionally unguessed MySQL `TIME` shape.
  SQL Server `xml`, PostgreSQL `citext`/`json`/`jsonb`/`xml`/`uuid`/simple enum columns,
  and MySQL `json`/`year`/simple `enum(...)`/bounded simple `set(...)` columns likewise scaffold as safe
  scalar CLR storage instead of warning-only provider-specific type rows;
  native JSON/XML operator semantics remain provider-bound. Simple PostgreSQL
  and MySQL enum columns are emitted as `string` storage plus a generated
  `HasCheckConstraint` over the discovered literal value set so the allowed
  values can move with generated migrations; malformed enum literal lists remain
  diagnostics instead of generated constraints. MySQL `set(...)` columns with
  eight or fewer comma-free members are emitted as `string` storage plus a
  generated check over the canonical MySQL value combinations; larger or
  ambiguous `set(...)` definitions remain diagnostics and make static or
  runtime dynamic generated entity types read-only; definitions with malformed comma structure
  are treated the same way.
  PostgreSQL arrays over safe scalar elements, including numeric, text/citext,
  UUID, binary, date/time including `time with time zone`, interval, and timestamp arrays, scaffold as CLR
  arrays so the model compiles and materializes with Npgsql; they remain
  provider-specific schema for provider-mobility review because
  SQL Server/MySQL/SQLite do not share native array DDL.
  PostgreSQL domain columns are reported with the domain schema/name and
  underlying provider type so reviewers can preserve or remodel domain-owned
  constraints before claiming provider mobility. Static and dynamic schema
  probes cast safe domains back to their base provider type, so string,
  numeric, temporal, UUID, boolean, binary, safe user-defined scalar bases such
  as `citext`, safe scalar-array domains, and simple enum-base domains keep
  useful CLR types instead of being flattened to `string`. Bounded string and
  numeric facets are preserved where provider metadata exposes them, and safe-domain diagnostics do not make the generated entity `[ReadOnlyEntity]`. Domains over unsafe provider-owned base
  types such as `inet` still make the generated entity read-only.
  SQL Server alias/user-defined column types are reported with schema/name,
  base system type, and bounded facets when available so reviewers can preserve or remodel
  provider-owned type semantics. Alias types over scaffoldable scalar/binary
  bases keep generated writes enabled while remaining provider-specific
  diagnostics. Static and runtime dynamic scaffolding emit alias base
  metadata and use it as a fallback for CLR type and bounded string/binary length when
  provider schema rows are vague; unsafe alias bases such as spatial types still make the
  generated entity read-only.
  MySQL unsigned integer columns scaffold as exact unsigned CLR widths where
  provider metadata exposes them (`uint`, `ulong`, etc.), including metadata
  that still includes legacy display widths such as `int(10) unsigned`.
  Unsigned decimal/numeric columns keep `decimal` storage plus precision/scale
  metadata. Both remain provider-specific diagnostics because SQL Server,
  PostgreSQL, and SQLite do not share MySQL's unsigned numeric DDL semantics.
  Composite primary keys are also emitted in generated context configuration
  with `HasKey(e => new { ... })` in provider-reported key ordinal order, so
  the reverse-engineered model carries the full key shape explicitly. Explicit
  SQL Server, PostgreSQL, and SQLite primary-key constraint names are preserved
  with generated `HasKey(..., "constraint_name")` when provider metadata or
  recoverable SQLite `CREATE TABLE` DDL exposes a meaningful non-default name.
  MySQL's fixed `PRIMARY` metadata is preserved because it is the provider's
  observable primary-key constraint identity. SQL Server system-generated names,
  PostgreSQL default `<table>_pkey` names, and SQLite's unnamed PRAGMA-only key
  shape are not emitted as no-op generated-code noise.
  Native table, view, and column descriptions are preserved as generated XML
  documentation where provider catalogs expose them: SQL Server
  `MS_Description` and PostgreSQL `COMMENT ON` flow into escaped class/property
  summaries for emitted tables, read-only view/materialized-view query artifacts,
  and SQL Server local table/view synonyms resolved to commented base objects, MySQL
  table/column comments flow for base tables, and SQLite keeps the existing
  column-mapping summaries because it has no native comment catalog.
  PostgreSQL identity/serial column defaults and their owned sequences are
  treated as identity metadata, not as separate provider-owned warning rows.
  Independent PostgreSQL `nextval('sequence'::regclass)` defaults that are not
  owned by the column are preserved as ordinary default SQL metadata.
  Simple SQL defaults that pass nORM's migration default allowlist (numeric,
  boolean, `NULL`, single-quoted ANSI/Unicode string literals, safe hex/binary/bit-string
  literals such as `0xDEADBEEF`, `X'DEADBEEF'`, and `B'1010'`, typed temporal
  and interval literals such as `DATE '2026-06-15'` and `INTERVAL '1 hour'`, literal-only
  `LOWER('value')`/`UPPER('value')` string normalization defaults, including
  provider-normalized PostgreSQL literal casts such as `LOWER('value'::text)`
  and MySQL character-set literals such as `LOWER(_utf8mb4'value')`, and known
  no-argument date/time/UUID functions, including no-argument `CURRENT_TIMESTAMP()`/
  `CURRENT_DATE()`/`CURRENT_TIME()` spellings, precision-limited MySQL
  temporal forms such as `CURRENT_TIMESTAMP(6)`/`UTC_TIMESTAMP(6)`, and safe
  PostgreSQL cast suffixes such as `'draft'::text` and
  `now()::timestamp without time zone`, plus strict PostgreSQL UTC timestamp
  defaults such as `now() AT TIME ZONE 'utc'` and `timezone('utc', now())`)
  are emitted in generated context configuration via
  `Property(...).HasDefaultValueSql(...)` so schema snapshots and migration
  generators can round-trip the default metadata. SQL Server explicit
  non-system default-constraint names are preserved through the generated
  `HasDefaultValueSql(..., constraintName: ...)` overload; system-generated
  names still use nORM's stable fallback names. MySQL `information_schema`
  string/date/time literal defaults and catalog-escaped string-expression
  defaults are normalized back to SQL literal text
  before that allowlist is applied. This is DDL metadata only:
  it does not mark the column as database-generated and does not cause nORM to
  omit the property from runtime inserts.
  MySQL `ON UPDATE` timestamp defaults are not promoted as plain default
  metadata because the database mutates the column during updates; they remain
  provider-specific default diagnostics and make the generated entity read-only
  until the update semantics are modeled explicitly.
  Computed/generated columns are marked
  with `DatabaseGeneratedOption.Computed` so nORM does not treat them as normal
  insert columns, but their provider expressions remain provider-owned DDL.
  SQLite generated-column `VIRTUAL`/`STORED`, including abbreviated
  `AS (...)` generated-column declarations without `GENERATED ALWAYS`, SQL
  Server `PERSISTED`, PostgreSQL stored generated, and MySQL `VIRTUAL
  GENERATED`/`STORED GENERATED` storage tokens are separated from the
  expression before emitting `HasComputedColumnSql(...)`; stored/persisted
  generated columns pass `stored: true`.
  SQL Server rowversion/timestamp columns are marked as `[Timestamp]` and
  database-generated for optimistic concurrency, while the provider DDL remains
  outside the generated model.
- Nullable reference/value type generation from database nullability metadata.
  The runtime API defaults to nullable-enabled output. The CLI follows the
  target or inferred project's `Nullable` setting, including the nearest
  `Directory.Build.props` before project overrides: `enable` and `annotations`
  emit `#nullable enable`, while `disable`, `warnings`, or an omitted property
  emit `#nullable disable`.
- Non-null reference properties are initialized with `default!` only in
  nullable-enabled output so generated code compiles cleanly before the
  application adds its own constructors or required-member style.
- Required reference navigations, where every dependent FK column is non-null,
  are emitted as non-null properties initialized with `default!` in
  nullable-enabled output. Optional reference navigations remain nullable.
- Generated entity classes and generated contexts are `partial`. Generated
  contexts apply scaffolded model configuration first, then invoke
  caller-supplied `DbContextOptions.OnModelCreating` and finally call a private
  static `OnModelCreatingPartial(ModelBuilder)` partial method, so repeated
  scaffolds can keep reviewed custom model configuration in a separate partial context
  file instead of editing generated source.
- `DbContext` generation with `IQueryable<T>` properties backed by nORM's
  query provider. Generated contexts expose both `DbConnection` and connection
  string constructors; both require an explicit `DatabaseProvider` and both run
  generated model configuration without embedding a connection string.
- Provider-specific identifier escaping for the zero-row schema query.
- Literal table and column identifiers discovered from metadata are escaped as
  single database object names, so names containing dots (for example
  `audit.events` or `value.part`) are not silently reinterpreted as
  schema-qualified identifiers unless a schema is supplied separately.
- Dynamic table generation also rejects ambiguous dotted names when the same
  input could mean either a literal table name or a schema-qualified table.
  Fluent entity configuration supports `ToTable(name, schema)` so runtime
  models and design-time migrations can carry the same schema-qualified table
  identity as `[Table(..., Schema = ...)]` and scaffolded entities.
  Unqualified dynamic table names fail deterministically when the same object
  name exists in multiple SQLite attached databases, SQL Server schemas, or
  PostgreSQL schemas; use a schema-qualified name so supplemental metadata
  probes cannot mix key/generated-column metadata from another object. When a
  SQL Server or PostgreSQL catalog probe finds exactly one matching schema,
  dynamic generation uses that schema in the emitted table mapping so the
  zero-row schema probe and supplemental metadata probes resolve the same
  object even outside the default schema/search path.
  Runtime-generated dynamic types for keyless tables are marked with
  `[ReadOnlyEntity]`, matching static scaffolding's fail-closed write contract.
  Resolved schema/table identity, composite primary-key ordinals, and emitted
  property order are included in dynamic schema signatures so dynamic cache
  keys distinguish literal dotted table names from schema-qualified objects and
  key handling follows provider metadata rather than table column order.
  Computed/generated-column expression and stored/virtual metadata are included
  in dynamic schema signatures so runtime cache keys change when provider-owned
  generated-column shape changes, while dynamic CLR attributes still only mark
  the property as database-generated.
  MySQL runtime dynamic table names that include a catalog qualifier use that
  catalog for generated-key, primary-key, unsigned-integer, decimal, and
  computed-column metadata probes instead of falling back to the current
  `DATABASE()`.
- Generated C# identifiers are sanitized: invalid characters become `_`,
  leading digits are prefixed with `_`, and C# keywords use `@`.
- Generated class and property names are de-duplicated deterministically when
  different database identifiers normalize to the same C# identifier. Property
  and navigation names also avoid the enclosing entity type name and inherited
  `object` member names such as `ToString`, `Equals`, `GetHashCode`, and
  `GetType`.
- When the same table name appears in multiple schemas, generated entity names
  include the schema name instead of relying on opaque numeric suffixes.
  Provider-bound routine and sequence stubs follow the same rule: when selected
  stubs share a database object name across schemas, generated method, DTO, and
  helper type names include the schema-derived prefix instead of opaque numeric
  suffixes.
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
  configuration, including provider-reported foreign key constraint names and
  recoverable SQLite `CREATE TABLE` foreign-key constraint names when the
  provider exposes real names. SQLite `PRAGMA foreign_key_list` does not expose
  declared constraint names, so unnamed synthetic SQLite FK ids are still not
  emitted into generated source. SQL Server foreign-key names marked
  `is_system_named` by the catalog are likewise treated as generated database
  noise and are not emitted as fluent constraint-name arguments; explicit SQL
  Server FK names are still preserved. PostgreSQL default
  `<table>_<columns>_fkey` names and MySQL default `<table>_ibfk_<n>` names are
  also treated as generated database noise; explicit provider-reported names
  outside those default patterns are preserved. `ON DELETE CASCADE` is preserved as nORM tracked-graph
  cascade behavior; valid database referential actions (`NO ACTION`,
  `CASCADE`, `SET NULL`, `RESTRICT`, `SET DEFAULT`) are emitted into generated
  fluent configuration, including `ON UPDATE` actions. Provider-owned FK
  timing/match semantics such as PostgreSQL `DEFERRABLE` / `MATCH FULL` and
  SQLite `MATCH FULL` / `DEFERRABLE INITIALLY DEFERRED` remain relationship
  diagnostics instead of being flattened into ordinary generated navigations.
  Relationships are emitted when the FK targets the
  generated principal primary key or an exact ordered unfiltered unique index exposed by provider
  metadata. Composite relationships are emitted when the ordered FK columns
  reference the exact generated composite primary key or an exact ordered unfiltered unique index.
  When the dependent FK columns are themselves the dependent primary key or an
  exact unfiltered unique index, scaffolding emits a one-to-one `HasOne(...).WithOne(...)` relationship
  with a nullable reference navigation
  on the principal instead of flattening the model into a collection. This
  applies to required and optional single-column FK column sets, required and
  optional composite FK column sets, self-referencing unique dependent FKs,
  and shared primary-key one-to-one
  relationships where the dependent PK is also the FK. Optional nullable unique
  dependent FKs keep nullable scalar FK columns and the dependent reference
  navigation nullable.
  Multiple unique dependent FKs to the same principal use FK-role-based reference navigation
  names and do not produce possible-join-table warnings;
  otherwise the principal side remains a collection navigation.
  Duplicate single-column and composite FK relationships use the distinguishing FK
  column rather than a shared tenant or partition column for role names.
  FK shapes targeting keyless principals, keyless dependents, or non-unique
  alternate columns are reported for manual configuration instead of emitting
  unsafe fluent code.
  Self-referencing FKs use role-based navigation names derived from the FK
  column instead of vague same-type names.
- Single-column, composite, and multi-membership non-primary-key index
  generation through nORM's `[Index]` metadata, including unique composite
  indexes without converting them into per-column uniqueness. Recoverable
  SQLite named `UNIQUE` constraints are preserved from `CREATE TABLE` DDL;
  SQLite autoindex names for unnamed unique constraints and SQL Server
  unique-constraint indexes marked `is_system_named`, PostgreSQL default
  `<table>_<columns>_key` unique constraint indexes, and MySQL unnamed-unique
  indexes reported with their first key column or `<column>_UNIQUE` as the
  index name are replaced with stable `UX_<Table>_<Columns>` names instead of
  leaking generated catalog artifacts into source.
  Provider-specific
  descending key order is preserved with `IndexAttribute.IsDescending`.
  SQL Server, PostgreSQL, and SQLite filtered/partial indexes are preserved
  with `IndexAttribute.FilterSql`; SQL Server and PostgreSQL included-column
  indexes are preserved with `IndexAttribute.IsIncluded`. PostgreSQL
  `NULLS NOT DISTINCT` unique column indexes are preserved with
  `IndexAttribute.NullsNotDistinct`. PostgreSQL non-default
  `NULLS FIRST/LAST` ordering on ordinary column indexes is preserved with
  `IndexAttribute.NullSortOrder`. Expression index
  semantics remain diagnostics rather than portable `[Index]` attributes, but
  SQLite expression indexes, ordinary PostgreSQL B-tree expression indexes,
  and MySQL expression indexes exposed by `SHOW INDEX` are emitted as
  provider-bound `HasExpressionIndex(...)` metadata, including
  filtered/partial predicates when the provider supports them and representable
  PostgreSQL expression-index `INCLUDE`/null-semantics facets.
  SQLite attached-schema partial index predicates are read from the owning
  schema instead of assuming `main.sqlite_master`.
  MySQL prefix indexes are diagnostics and are not used as unique alternate-key
  evidence when the prefix is shorter than the declared column length because
  prefix uniqueness is not full-column uniqueness. Prefix indexes whose prefix
  covers the full declared column length are emitted as normal indexes.
- Decimal precision/scale preservation, including precision-only metadata, for SQL Server, PostgreSQL, MySQL, and SQLite
  `decimal`/`numeric` columns. SQLite parses declared `DECIMAL(p,s)` and
  `NUMERIC(p,s)` type text from `PRAGMA table_xinfo`. Static scaffolding emits
  `Property(...).HasPrecision(p, s)` or `Property(...).HasPrecision(p)` in
  generated context configuration and keeps
  `[Column(TypeName = "decimal(p,s)")]` or `[Column(TypeName = "decimal(p)")]`
  on generated entity properties as assembly-snapshot fallback metadata.
  Runtime dynamic scaffolding emits the same type-name metadata because it has
  no generated context file; dynamic schema signatures include precision/scale.
  Static scaffold and schema snapshot precision parsing tolerate provider whitespace and domain-wrapped numeric type text, and provider migration generators round-trip it instead of
  falling back to `DECIMAL(18,2)`. Fluent `Property(...).HasPrecision(p, s)` and
  precision-only `Property(...).HasPrecision(p)` feed the same migration
  snapshot metadata.
  SQLite still keeps provider-neutral migration storage, but declared
  precision-bearing numeric columns scaffold as `decimal` properties.
- Pure many-to-many join table generation for the safe v1 subset: exactly two
  non-null foreign-key constraints, no payload columns, a join-table primary
  key made exactly from those FK columns, and both references targeting the
  exact generated primary keys or exact ordered unfiltered unique
  indexes, with delete/update actions inside the common referential-action set. Single-column,
  composite-key, shared-column tenant, and alternate-key pure junction tables
  are supported. Both entity sides receive collection navigations, and the join
  table is emitted as fluent `HasMany().WithMany(inverse).UsingTable(...)`
  configuration instead of a join entity. Alternate-key bridges use the
  key-selector `UsingTable` overload so runtime include/loading and join-table
  sync use the referenced unique columns instead of assuming primary keys.
  Non-default join-table FK delete/update actions are emitted through the
  action-aware `UsingTable` overload and flow into schema snapshots so generated
  migrations recreate the same bridge FK actions instead of silently normalizing
  them to `NO ACTION`.
  Configured `UsingTable` bridges are included in `SchemaSnapshotBuilder.Build(ctx)`
  as migration tables with non-null FK columns, a composite primary key, and
  FK constraints so generated migrations create the bridge table instead of
  requiring manual DDL.
- Fluent `OwnsMany` owned collection mappings are included in
  `SchemaSnapshotBuilder.Build(ctx)` as child migration tables for the runtime
  v1 shape: one non-null owner FK column, the owned item columns, owned item
  primary-key metadata, and a cascade FK back to the owner key that runtime
  persistence uses. Owned element fluent metadata for defaults, collations,
  computed columns, table CHECK constraints, expression indexes, and shadow
  columns is preserved in the child-table snapshot.
  Schema-qualified owned collection tables use the schema-aware `OwnsMany`
  overload so runtime persistence and generated migrations target the qualified
  child table rather than a literal dotted table name.
- Fluent `OwnsOne` owned scalar mappings are included in
  `SchemaSnapshotBuilder.Build(ctx)` as inline columns on the owner table, and
  owned-builder metadata for column names, defaults, collations, computed
  columns, table CHECK constraints, expression indexes, and shadow columns is
  preserved in the owner-table snapshot.
  Database-generated bridge columns such as computed values and
  rowversion/timestamp metadata do not force an otherwise pure bridge to become
  a payload entity.
  Schema-qualified join tables use the schema-aware `UsingTable` overload so
  generated SQL targets the qualified bridge table rather than a literal dotted
  table name. SQL Server and PostgreSQL migration SQL generation emits
  idempotent schema creation before creating schema-qualified added tables.
  SQLite attached-schema foreign-key clauses use the unqualified referenced
  table name required by SQLite's `REFERENCES` grammar.
  Self-referencing pure join tables receive distinct role-based navigations
  from the join FK columns and are consumer-build tested.
  A generated single-column surrogate primary key is also allowed when the
  table has no payload columns and an exact unfiltered unique index covers the non-null FK columns;
  the generated `UsingTable` mapping writes only the FK columns and leaves the
  surrogate key database-owned.
- Payload bridge tables are emitted as explicit join entities with payload
  columns and normal FK navigations. They are also reported as possible
  many-to-many candidates so reviewers do not accidentally collapse domain data
  into a payload-free skip navigation.
- Optional table filtering through `ScaffoldOptions.Tables`, CLI
  comma-separated `--tables`, repeatable CLI `--table` entries, and EF-style
  multi-value `--table First Second` tokens. Filters may use `schema.table`
  or `schema.view` for schema-qualified tables and views, and may use
  object-kind selectors such as `table:dbo.Report`, `view:dbo.Report`,
  `query:dbo.Report`, `routine:dbo.RebuildCache`, or
  `sequence:dbo.InvoiceNumber` when same-schema database objects share a name.
  The `query:` selector covers supported query artifacts including views,
  materialized views, explicitly selected SQLite virtual tables, and local
  table/view SQL Server synonyms. MySQL catalog-qualified table and query-artifact filters
  are accepted when the catalog matches the current
  database, including after an object-kind selector, but generated model
  metadata remains unqualified because MySQL catalogs are not emitted as nORM
  schemas. Literal-name selectors such as `name:aux.orders` and
  `table:name:aux.orders` select literal dotted object names when the same text
  could otherwise be interpreted as a schema-qualified filter. Use repeatable
  `--table` for literal table names that contain commas and must not be split.
  Null or blank API filters are treated as empty rather than producing raw
  runtime exceptions; blank CLI filters are rejected so an empty command-line
  option cannot broaden the run to all tables. Bare table-name filters fail with an
  actionable error when the same table name exists in multiple schemas; use a
  schema-qualified filter in that case. Unfiltered runs that include a literal
  dotted table name with the same display string as a schema-qualified table
  still fail deterministically because the generated model would otherwise be
  ambiguous.
  Relationship and index metadata is scoped to selected tables; FKs whose
  dependent or principal table is intentionally filtered out are not emitted as
  navigations and do not create unrelated scaffold warnings.
  When `--emit-routine-stubs` or `--emit-sequence-stubs` is enabled, explicit
  table filters can also select matching routine or sequence stubs without
  broadening the run to every discovered provider object; without the matching
  opt-in those filters still fail as non-entity database objects.
  Because bare table filters are not object-kind selectors, an explicit filter that
  matches more than one selectable table, query artifact, routine, or sequence
  fails deterministically; schema-qualified filters disambiguate cross-schema
  matches, object-kind selectors disambiguate same-schema object-kind
  collisions, and literal-name selectors disambiguate filtered literal dotted
  name collisions.
- When no table/schema filter is supplied, ordinary views and PostgreSQL
  materialized views are scaffolded by default as read-only query artifacts,
  matching EF's database-first view coverage without inferring provider-neutral
  write semantics.
- Optional schema filtering through `ScaffoldOptions.Schemas`, CLI
  comma-separated `--schemas`, repeatable CLI `--schema` entries, and EF-style
  multi-value `--schema Accounting Sales` tokens. Schema filters include all
  discovered user tables, supported query artifacts, and opt-in routine or
  sequence stubs in the selected schemas and are unioned with explicit table filters.
  They apply to
  providers whose discovery preserves schema identity: SQL Server, PostgreSQL,
  and SQLite attached databases including `main`; SQLite `main` matches the
  unqualified default database and still emits unqualified `[Table]` metadata.
  MySQL scaffolding still uses the current database as an unqualified model
  because nORM does not emit the MySQL catalog/database name as a schema; the
  current catalog can still be used in table/query-artifact filters for EF-style
  command compatibility.
- Optional generated-name pluralizer control through
  `ScaffoldOptions.UsePluralizer` and CLI `--no-pluralize`. By default plural
  database object names are singularized for entity classes, and generated
  `IQueryable<T>` context properties use collection-style names with
  deterministic cleanup for descriptor-like entity names. Disabling
  pluralization preserves generated names without singularizing or pluralizing
  them.
- Optional relationship suppression through `ScaffoldOptions.NoRelationships`
  and CLI `--no-relationships`. When enabled, nORM still scaffolds tables,
  scalar foreign-key columns, keys, indexes, and scalar/provider metadata, but
  omits relationship navigation properties, many-to-many skip mappings, fluent
  `HasForeignKey` configuration, and relationship-only diagnostics.
- Optional database-name preservation through `ScaffoldOptions.UseDatabaseNames`
  and CLI `--use-database-names`. When enabled, legal database table, view,
  sequence, routine, column, and routine result-column names are preserved as
  generated CLR names instead of being normalized to PascalCase. Invalid C#
  identifiers and C# keywords are still escaped or normalized so generated code
  compiles. Synthetic navigation member names remain C#-style names derived
  from the preserved generated names and FK roles.
- EF-style CLI aliases are accepted for common scaffold switches:
  `--output-dir`/`-o` for `--output`, `-n` for `--namespace`, `-c` for
  `--context`, and `-t` for repeatable `--table`.
- EF-style namespace-qualified context names are accepted: `--context
  MyApp.Data.AppDbContext` generates class `AppDbContext` in namespace
  `MyApp.Data`. An explicit `--context-namespace` overrides the namespace
  portion while still using the final `--context` segment as the class name.
  CLI `--namespace`, `--context-namespace`, and the namespace portion of
  qualified `--context` values are validated as C# namespaces before generation
  starts. Explicit `--context` class-name segments are validated as C# type
  identifiers rather than silently corrected. Blank explicit CLI string values
  for scaffold options such as `--namespace`, `--context`, and `--output` are
  rejected instead of being treated as if the option was omitted.
- When CLI `--context` is omitted, nORM derives the generated context class
  name from the database name or SQLite database file name and appends
  `Context`, matching EF's default shape where it can be inferred. Named
  connection references use the final configuration-key segment. If no stable
  name is available, nORM falls back to `AppDbContext`.
- EF-style positional CLI arguments are accepted as a fallback:
  `norm scaffold <connection> <provider> ...`. Explicit `--connection` and
  `--provider` options take precedence when both forms are supplied. A single
  positional value after `--connection` is treated as the provider.
- EF-style command shape `norm dbcontext scaffold <connection> <provider> ...`
  is accepted as an alias for the same bounded nORM scaffold command, and
  `norm --help` advertises `dbcontext` as an EF-style alias group.
- EF-style named connection references such as `Name=ConnectionStrings:AppDb`,
  `name=ConnectionStrings:AppDb`, or shorthand `Name=AppDb`
  are resolved from environment variables first (for example
  `ConnectionStrings__AppDb`), then startup-project and target-project user
  secrets declared through `UserSecretsId`, then `appsettings.json` /
  `appsettings.{Environment}.json` in the startup project, target project, or
  current directory. nORM reads these configuration files directly and does not execute startup code for this lookup.
- EF Core provider package names are accepted for positional scaffold provider
  values and the explicit `--provider` option, then normalized to nORM providers:
  `Microsoft.EntityFrameworkCore.SqlServer`, `Microsoft.EntityFrameworkCore.Sqlite`,
  `Npgsql.EntityFrameworkCore.PostgreSQL`, `Pomelo.EntityFrameworkCore.MySql`,
  and `MySql.EntityFrameworkCore`.
- EF-style project targeting is accepted through `--project`/`-p`. The value may
  be a `.csproj` file or a directory containing exactly one `.csproj`; relative
  `--output`/`--output-dir` paths are resolved below that project directory, and
  the generated namespace defaults to the project's `RootNamespace`,
  `AssemblyName`, or sanitized project file name plus sanitized output directory
  segments when `--namespace` is omitted. If `--project` is omitted and the
  command working directory contains exactly one `.csproj`, nORM uses that
  project for the same path, namespace, user-secrets, and nullable-reference defaults.
  The nearest `Directory.Build.props` is read before project
  overrides for `RootNamespace`, `AssemblyName`, `UserSecretsId`, and
  `Nullable`. CLI `--context-dir` follows EF-style placement: relative paths are resolved below
  the target project directory, or below the command working directory when
  `--project` is omitted. When
  `--context-dir` is supplied without `--context-namespace`, a qualified
  `--context`, or an explicit `--namespace`, the context namespace defaults to
  the project's root namespace plus the sanitized context directory segments.
- EF common design-time switches `--startup-project`/`-s`,
  `--framework`/`--target-framework`, `--configuration`, `--runtime`, and
  `--no-build` are accepted for command-line compatibility. nORM scaffolding
  connects directly to the database and does not build the target project or
  load a startup application for schema discovery. Legacy EF-style
  `--msbuildprojectextensionspath` is also accepted as a no-op because nORM
  scaffold does not invoke MSBuild.
- EF-style application arguments after `--` are accepted for command
  compatibility. `-- --environment Production` is used only to include
  `appsettings.Production.json` in named-connection lookup; other application
  arguments are ignored because nORM does not execute startup code. When no
  pass-through environment is supplied, `ASPNETCORE_ENVIRONMENT` and then
  `DOTNET_ENVIRONMENT` select the matching `appsettings.{Environment}.json`
  file. An explicit blank `--environment` value is rejected. Unmatched scaffold
  tokens before `--` still fail fast so option typos are not swallowed.
- EF-style `.config/dotnet-ef.json` defaults are read for `project`,
  `startupProject`, `outputDir`/`output`, `namespace`, `context`,
  `contextDir`, `contextNamespace`, `schema`/`schemas`, `table`/`tables`,
  `framework`/`targetFramework`, `configuration`, `runtime`,
  `msbuildProjectExtensionsPath`, `noBuild`, `json`,
  `verbose`, `noColor`, `prefixOutput`, `noPluralize`, `useDatabaseNames`, `noRelationships`,
  `noOnConfiguring`, `dataAnnotations`,
  `force`, `noOverwrite`, `dryRun`, `failOnWarnings`, `emitRoutineStubs`,
  `emitSequenceStubs`, `emitViewEntities`, and `emitQueryArtifacts`. Relative project paths are
  resolved relative to the parent directory of `.config`, comma-separated or
  array table/schema defaults are accepted, and present string properties must
  be non-blank. Explicit CLI options take precedence over configuration file
  values. When any CLI table/schema filter is supplied,
  `.config/dotnet-ef.json` table/schema defaults are ignored so they cannot
  expand the explicit selection. Explicit CLI `--force` and `--no-overwrite`
  values also override the opposite config default instead of conflicting with
  it.
  Build/runtime defaults remain compatibility-only because nORM does not build
  or execute startup code during scaffolding.
- EF common output switches are accepted: `--json` or config `json` emits a machine-readable
  scaffold result summary for successful runs and scaffold failures, while `--verbose`/`-v`, `--no-color`, and
  `--prefix-output` are compatibility switches because nORM scaffold output is
  already plain and explicit.
- CLI `--data-annotations`/`-d` is accepted for EF Core scaffold command
  compatibility. It is a no-op because nORM scaffolding already emits
  data-annotation metadata where supported.
- CLI `--no-onconfiguring` is accepted for EF Core scaffold command
  compatibility. It is a no-op because nORM generated contexts do not emit an
  `OnConfiguring` method or hard-coded connection string in the first place.
- Optional generated context placement through `ScaffoldOptions.ContextDirectory`
  for output-relative API placement, `ScaffoldOptions.ContextOutputDirectory`
  for an explicit absolute API context directory, and CLI `--context-dir` for
  EF-style project-relative placement. CLI `--context-dir` rejects absolute
  paths so generated context code cannot escape the target project/current
  directory through this option. Optional context namespace separation is
  available through `ScaffoldOptions.ContextNamespace`, CLI
  `--context-namespace`, or a namespace-qualified `--context` value. Entity
  files and warning reports remain in the scaffold output directory. When the
  context namespace differs from the entity namespace, generated context code
  imports the entity namespace explicitly.
- CLI scaffold output conflicts are preflighted before any file is written, so
  a later collision does not leave a partially generated model. By default the
  CLI refuses to overwrite existing generated files; `--force`/`-f` enables
  explicit overwrite, and `--no-overwrite` is accepted as an explicit guard.
  Passing both `--force` and `--no-overwrite` is rejected. The runtime
  `ScaffoldOptions.OverwriteFiles` flag controls the same behavior for API
  callers.
- Optional dry-run validation through `ScaffoldOptions.DryRun` and CLI
  `--dry-run`. Dry runs perform schema discovery and in-memory generation
  without creating the output directory, deleting stale warning reports, or
  writing generated files. The CLI dry run uses an isolated temporary output
  directory so it can print the same warning summary as a real run while
  leaving the requested output path untouched. With `--dry-run
  --fail-on-warnings --json`, diagnostics still produce a failed
  machine-readable result, but `warnings.reportsWritten` is `false` and the
  requested output directory is not created.
- Optional warning enforcement through `ScaffoldOptions.FailOnWarnings` and CLI
  `--fail-on-warnings`, which fails real scaffold runs after writing
  `nORM.ScaffoldWarnings.md` and `nORM.ScaffoldWarnings.json`.
- Optional provider-bound routine wrappers through
  `ScaffoldOptions.EmitRoutineStubs` and CLI `--emit-routine-stubs`.
  Stored procedure wrappers call nORM's stored-procedure APIs with the
  discovered schema-qualified routine name. SQL Server scalar/table-valued
  functions, PostgreSQL functions, and MySQL functions are discovered as
  routines and emitted as provider-bound `SELECT` wrappers (`SELECT
  function(...) AS Value` for scalar functions and `SELECT * FROM
  function(...)` for table-valued functions with row metadata). PostgreSQL extension-owned routines
  are excluded from routine discovery so installed helper functions such as
  `uuid-ossp` or `pgcrypto` entry points do not flood generated application
  contexts. PostgreSQL set-returning functions
  that return scalars use `SELECT function(...) AS Value` and get a generated
  `Value` result DTO so `RETURNS SETOF
  integer/text/...` has a typed buffered and streaming path instead of forcing a
  hand-written result class
  instead of being miscalled as stored procedures. Table-valued functions also
  receive a streaming wrapper so callers can consume large provider-owned
  rowsets without buffering. When provider metadata exposes a stable
  first-result-set shape, or PostgreSQL table-function OUT columns,
  scaffolding emits a nested result DTO plus typed non-generic buffered and
  streaming overloads alongside the generic escape hatch. When provider metadata exposes safe
  input parameter names, scaffolding emits a nested parameter DTO with known CLR
  scalar types (`int?`, `decimal?`, `DateTime?`, `DateTimeOffset?`,
  `DateOnly?`, `TimeOnly?`, `TimeSpan?`, `Guid?`, `string?`, `byte[]?`,
  etc.) and falls back to `object?` only for unmapped provider types. PostgreSQL
  routine parameters preserve common array metadata (`int[]?`, `string[]?`,
  `Guid[]?`, temporal arrays, etc.), known safe PostgreSQL user-defined
  scalar types such as `citext` and `uuid`, and PostgreSQL domain parameters
  over safe scalar/array/enum bases. Simple domain identifiers are emitted as
  function argument casts so domain-typed overloads stay provider-faithful.
  SQL Server scalar alias types are mapped through their base
  system type; SQL Server table-valued parameters scaffold as `DbParameter?`
  so callers pass reviewed provider-specific structured parameters. MySQL
  unsigned routine parameters use unsigned CLR types (`uint?`, `ulong?`,
  `ushort?`, `byte?`) when the provider exposes the modifier. Function wrappers
  enforce the exact scaffolded input-argument count before building the SQL
  invocation, and stored-procedure wrappers with scaffolded input metadata fail
  with `NormConfigurationException` when the generated wrapper is called
  without the expected parameter object or dictionary shape. Function wrappers
  exclude provider metadata return rows from callable input counts; return
  types are tracked separately through routine result/data type metadata. When
  provider parameter names cannot safely become C# DTO properties, wrappers use
  positional `object?[]` arguments instead of silently dropping or misbinding
  required function arguments. PostgreSQL function wrappers cast safe argument
  placeholders to the scaffold-discovered provider types so overloaded
  functions and provider inference do not drift between calls.
  Stored-procedure wrappers with such parameter names use
  `IReadOnlyDictionary<string, object?>` arguments so callers can pass exact
  provider parameter names without unsafe DTO generation.
  Stored-procedure stubs include both a buffered `Task<List<TResult>>` wrapper
  and a streaming `IAsyncEnumerable<TResult>` wrapper for large result sets.
  When safe
  output parameter metadata is available, scaffolding also emits an
  `OutputParameter[]` factory with mapped `DbType` values, discovered
  string/binary sizes, decimal precision and optional scale where providers expose them,
  and `InputOutput` direction
  for INOUT parameters. SQL Server stored
  procedures include the provider return status as a `ReturnValue` output
  definition. The generated output wrapper includes a convenience overload that
  uses the scaffold-discovered output definitions, plus an explicit
  `params OutputParameter[]` overload for reviewed routine signature changes.
  Procedures with explicit no-result-set metadata emit `Task<int>` non-query
  wrappers, and no-result procedures with output metadata emit
  `StoredProcedureNonQueryResult` wrappers so callers do not need to invent a
  fake row DTO just to read affected rows and output values.
  Scalar-function stubs also include a direct `Task<TValue?> ...ValueAsync`
  convenience wrapper so callers do not need to hand-author a one-column
  result DTO for the generated `Value` projection.
  XML comments list the discovered parameter metadata, including sized types
  such as `nvarchar(32)` and `decimal(18,2)`. When SQL Server,
  PostgreSQL, or MySQL expose native routine comments, those comments become
  the generated wrapper XML summary and the provider-bound execution details
  remain in XML remarks. PostgreSQL overloaded routine names are left without
  generated comment summaries unless the catalog row can be matched
  unambiguously. Same-schema routine overloads use discovered input-type
  suffixes in generated method and DTO names when those suffixes are distinct,
  and fall back to deterministic numeric C# de-duplication only when the
  signature cannot be named safely. Routine stubs with the same name in
  different schemas use schema-prefixed generated method and DTO names so diffs
  identify the provider object being called. Routine bodies remain
  provider-owned and are not translated across database engines.
- Optional provider-bound standalone sequence wrappers through
  `ScaffoldOptions.EmitSequenceStubs` and CLI `--emit-sequence-stubs`.
  SQL Server and PostgreSQL standalone sequences are discovered with scalar
  value type metadata where the provider exposes it, and generated context
  methods retrieve the next value with provider SQL (`NEXT VALUE FOR` or
  PostgreSQL `nextval(...::regclass)`). PostgreSQL serial/identity-owned
  sequences stay attached to their generated columns; independent sequence
  defaults are emitted as `HasDefaultValueSql` when the default expression
  passes the shared allowlist. Sequence DDL, allocation/caching, and
  cross-provider migration remain provider-owned. Sequence warning metadata
  records the provider, discovered data type, generated CLR value type, and
  whether nORM can emit an opt-in next-value stub for that provider. SQL Server
  and PostgreSQL native sequence comments are emitted as wrapper XML summaries
  while provider-bound sequence semantics stay in XML remarks. Sequence stubs
  with the same name in different schemas use schema-prefixed generated method
  and result type names instead of opaque numeric suffixes.
- Optional provider query-artifact entities through
  `ScaffoldOptions.EmitQueryArtifacts` (or the compatibility alias
  `EmitViewEntities`) and CLI `--emit-query-artifacts` (or the compatibility alias `--emit-view-entities`). Ordinary views and PostgreSQL materialized views are
  scaffolded by default as read-oriented query artifacts. Explicit table filters
  (`--table`/`ScaffoldOptions.Tables`) and schema filters
  (`--schema`/`ScaffoldOptions.Schemas`) also include matching supported query
  artifacts, so EF-style filtered scaffolds of views produce read-oriented nORM
  model code instead of a skipped-object failure. When query-artifact emission
  is enabled together with filters, only selected query artifacts count as emitted;
  unselected SQLite virtual-table shadow storage is not reported for a
  filtered scaffold that did not select the virtual table.
  Generated view/materialized-view/SQLite virtual-table classes are intended
  for reads; scaffolding still reports missing primary keys where the database
  does not expose one. Query-artifact generated types are marked with
  `[ReadOnlyEntity]` even when a provider exposes key-like metadata, so nORM can
  materialize them through queries but rejects insert/update/delete and tracked
  `SaveChanges` writes before SQL generation.
  Key-looking view columns such as `Id` and `ParentId` remain scalar properties
  on read-only query artifacts; scaffolding does not infer keys, relationships,
  or generated write semantics from view column names.
  SQL Server synonyms whose local base object resolves as a table or view can
  also be emitted as read-oriented query artifacts; procedure synonyms, remote
  synonyms, and unresolved synonyms remain provider-owned diagnostics.
- Warning reports are deterministic per run: if a later scaffold produces no
  diagnostics, stale `nORM.ScaffoldWarnings.*` files are removed when overwrite
  is explicitly allowed, or reported as an error when overwrite protection is
  enabled.
- Table discovery and generated output are ordered deterministically so repeated
  scaffolds of the same schema produce reviewable diffs instead of provider
  metadata-order churn. Relationship navigations and fluent relationship
  configuration are also ordered by generated names rather than raw provider FK
  discovery order.
- Deterministic Markdown and JSON diagnostics for discovered database features
  that are provider-owned or not fully converted into runnable model code.
  Composite foreign keys that
  do not target the generated principal primary key or an exact ordered unfiltered unique index
  are listed there instead of being silently ignored or converted into fake
  single-column navigations;
  unsafe/provider-specific defaults that fail the safe default allowlist,
  provider-specific column types that cannot be represented as safe scalar
  storage,
  SQL Server rowversion/timestamp columns whose concurrency metadata is emitted
  but whose exact provider DDL remains provider-owned,
  unparsed provider-specific identity strategy metadata, unrecognized FK referential actions,
  relationships that do not target the generated principal primary key or an
  exact ordered unfiltered unique index,
  and triggers are inventoried for review; trigger-backed tables are emitted as
  `[ReadOnlyEntity]` until the provider-owned side effects are hand-modeled;
  tables with unsafe/provider-specific defaults are emitted as
  `[ReadOnlyEntity]` until default semantics are hand-modeled or converted to
  safe generated metadata;
  tables with unparsed provider-specific identity strategies are emitted as
  `[ReadOnlyEntity]` until generated key semantics are hand-modeled;
  SQL Server provider-native temporal tables
  (base and history) are reported as provider-owned schema and emitted as
  `[ReadOnlyEntity]` so generated write paths fail closed; tables with unsafe
  provider-specific columns are emitted the same way;
  tables without primary keys, ordinary views, and PostgreSQL materialized views
  are emitted as read-only generated types and still reported where reviewers
  need to know writes and navigations require a real key; SQLite virtual tables,
  routines, sequences, SQL Server synonyms, SQLite virtual-table shadow tables,
  and MySQL events are discovered and reported as skipped database objects unless
  an explicit opt-in emits the supported query-artifact, routine-stub, or sequence-stub shape; likely
  many-to-many join tables are flagged when they are scaffolded as normal
  entities.

## Evidence

- The focused `DatabaseScaffolder*` coverage files cover identifier normalization,
  duplicate generated-name handling, table filtering, overwrite protection,
  deterministic repeated output, nullable initialization, SQLite FK navigation generation, SQLite
  composite-FK navigation/model configuration generation, and SQLite
  single-column/composite index generation and columns that participate in
  multiple indexes, plus role-based naming for duplicate relationships,
  composite primary-key source generation with consumer-build evidence,
  decimal precision/scale preservation across generated `[Column(TypeName)]`,
  string/binary length, Unicode, and fixed-length preservation across generated
  fluent configuration, schema snapshots, and migration SQL generators,
  SQL Server identity seed/increment emission through generated
  `HasIdentityOptions(...)` configuration,
  single-column and composite alternate-key FK generation when the target is an exact ordered unfiltered unique index,
  FK cascade/non-cascade preservation, computed/generated column write
  exclusion, relationship suppression when the principal key cannot be
  generated safely, schema-qualified many-to-many join table preservation,
  self-referencing one-to-many and one-to-one FK role-based navigation naming,
  self-referencing pure many-to-many join scaffolding,
  composite-key pure many-to-many join scaffolding,
  provider-specific partial/expression/included-column index diagnostics,
  MySQL prefix-index diagnostics that exclude prefix uniqueness from generated
  relationship inference,
  descending index metadata round-tripping,
  safe default-to-`HasDefaultValueSql` promotion, unsafe default-to-read-only
  hardening, unsafe composite-FK,
  many-to-many candidate, and provider-owned schema diagnostics.
- `CliIntegrationTests.Scaffold_sqlite_output_builds_as_consumer_project`
  proves `dotnet-norm scaffold` output builds in a consumer project, including
  quoted/backslash/XML-sensitive table and column identifiers.
- `DatabaseScaffolderContextWriterTests` proves opt-in routine wrapper
  output compiles as a consumer project, keeps the provider-bound routine
  warning/metadata contract explicit, including SQL Server first-result-set
  metadata parsing, and emits SQL Server/PostgreSQL/MySQL functions as `SELECT`
  wrappers instead of stored-procedure calls, including buffered and streaming
  table-valued function wrappers. Procedures with explicit no-result-set
  metadata emit non-query wrappers instead of fake row-materialization stubs.
- `DatabaseScaffolderContextWriterTests` proves opt-in SQL Server/PostgreSQL
  sequence wrappers generate typed next-value methods and compile in a consumer
  project. `LiveProviderScaffoldingParityTests` proves those sequence wrappers
  are discovered and emitted from live SQL Server and PostgreSQL catalogs.
- `LiveProviderScaffoldingParityTests` proves live FK referential actions are
  preserved in generated model configuration across SQLite, SQL Server,
  PostgreSQL, and MySQL, including `ON DELETE SET NULL` plus `ON UPDATE CASCADE`.
  It also pins `ON DELETE RESTRICT` plus `ON UPDATE CASCADE` on the providers
  whose DDL accepts that shape (SQLite, PostgreSQL, and MySQL), and `SET
  DEFAULT` delete/update actions on SQLite, SQL Server, and PostgreSQL.
- `DatabaseScaffolderDiagnosticsTests` and `DatabaseScaffolderOutputTests` prove opt-in query-artifact
  scaffolding converts SQLite views and SQLite virtual tables into compiling
  query artifacts while preserving missing-primary-key/shadow-table diagnostics.
- `LiveProviderScaffoldingParityTests` proves opt-in view entity scaffolding
  emits compiling query artifacts and explicit missing-primary-key diagnostics
  across configured SQLite, SQL Server, PostgreSQL, and MySQL providers. It
  also pins live promotion of provider CHECK constraints, computed/generated
  column expressions, and explicit column collations into generated model
  configuration across those providers, while provider-native triggers are
  inventoried as `SCF110` diagnostics instead of being silently ignored or
  translated into unsafe model code.
  The SQLite generated-column extractor is parser-based rather than regex-only,
  so quoted identifiers inside expressions do not cause computed metadata to be
  left behind as unresolved diagnostics. The same test class also pins
  PostgreSQL materialized-view query artifacts, SQL Server
  provider-native temporal base/history read-only scaffolds, SQL Server
  rowversion concurrency-token scaffolds, SQL Server scalar/table-valued
  function wrappers, SQL Server no-result procedure non-query wrappers,
  table-valued-parameter routine result wrappers, PostgreSQL array/domain/UUID and MySQL
  scalar-function wrappers with unsigned routine parameter stubs, env-gated
  execution of scaffold-style escaped routine output invocation names,
  PostgreSQL
  overloaded and quoted-parameter function wrappers
  with deterministic generated names, PostgreSQL domain diagnostics,
  SQL Server alias-type diagnostics, MySQL unsigned-column diagnostics,
  SQL Server geometry, PostgreSQL inet, MySQL point, and SQLite declared
  spatial provider-specific column diagnostics,
  PostgreSQL UUID/array column scaffolds,
  MySQL JSON/YEAR column scaffolds, SQL Server local table/view synonym query artifacts
  plus dynamic read-only parity and procedure-synonym rejection,
  MySQL scheduled-event diagnostics, SQLite virtual-table query artifacts, and live
  generated-surrogate, composite generated-surrogate, shared-tenant,
  shared-tenant alternate-key, composite payload bridge preservation,
  alternate-key, self-referencing, and
  schema-qualified many-to-many junction-table mappings, including
  filtered-unique surrogate join tables that must remain
  explicit entities instead of unsafe skip navigations, plus keyless dependent
  and keyless principal FK shapes that must suppress generated navigations.
- `LiveProviderScaffoldCliParityTests` runs the real `dotnet-norm scaffold`
  command against live SQLite, SQL Server, PostgreSQL, and MySQL schemas with a
  shared-tenant composite-key many-to-many bridge, an alternate-key many-to-many
  bridge, a mixed one-to-many plus many-to-many model shape that also preserves
  composite index metadata through the real CLI, a direct composite primary-key FK model shape with generated composite `HasKey` and fluent FK selectors, and with
  `--use-database-names --no-pluralize` over preserved legal database
  identifiers. Default plural table-name singularization and plural query
  property generation are verified through the real CLI across all four
  providers. Invalid table/column identifiers, duplicate normalized entity
  names, duplicate normalized property names, and object-member column names
  are also verified through the real CLI across all four providers, then the
  generated output is built as a consumer project. Project-targeted scaffold
  output is likewise verified through the real CLI across all four providers:
  `--project`, project-relative `--output-dir`, `--context-dir`, inferred
  entity/context namespaces, and project nullable-reference settings all feed
  generated code that builds as the target project. Inherited
  `Directory.Build.props` metadata is also verified across all four providers:
  root namespace, nullable-reference settings, and user-secrets connection
  lookup feed generated code that builds as the target project. Project-file
  metadata overrides conflicting `Directory.Build.props` metadata across all
  four providers too: AssemblyName fallback, nullable-reference settings, and
  user-secrets connection lookup all come from the target project when present.
  Passing a single-project
  directory to `--project` instead of a `.csproj` file is also verified across
  all four providers: relative output and context paths remain project-relative,
  inferred namespaces use the discovered project metadata, and the generated
  target project builds. Short `-p` and `-f` aliases are verified together
  across all four providers by using project-relative output to overwrite stale project-relative generated files
  and then building the target project.
  EF-style short aliases and
  namespace-split context generation are also verified across all four providers:
  `-o`, `-n`, `-c`, and `-t` feed generated code where an explicit
  `--context-namespace` overrides the namespace portion of a qualified
  `--context` value while still using the final context class-name segment.
  Running from a directory
  that contains exactly one `.csproj` is also verified across all four
  providers: the CLI infers that current-directory project, resolves
  `--output-dir` and `--context-dir` under it, applies its root namespace and
  nullable settings, and builds the generated project. Named-connection lookup
  from inferred current-directory project appsettings is also verified across
  all four providers while preserving project-relative output and context paths.
  Named-connection lookup from inferred current-directory project user secrets is
  verified across all four providers too: user-secret connections declared by
  the inferred project's `UserSecretsId` override inferred-project appsettings
  while preserving project-relative output and context paths.
  No-project current-directory appsettings named-connection lookup is verified
  across all four providers as a separate source: with no `.csproj` in the
  working directory, `appsettings.json` still resolves `Name=...`, generated
  files use current-directory-relative output and context paths, and the
  generated code builds in a consumer project. EF-style pass-through
  `-- --environment Production` verifies no-project current-directory environment-specific appsettings too, so `appsettings.Production.json`
  overrides current-directory base `appsettings.json` while keeping generated
  files current-directory-relative.
  EF-style `.config/dotnet-ef.json`
  config-driven scaffolds are also verified across all four providers:
  config-supplied `project`, `startupProject`, and `context`, positional
  `Name=...` named connections from startup-project `appsettings.json`, and
  accepted EF provider package aliases all feed generated code that builds as
  the target project. Expanded config-supplied output directory, namespace,
  context directory, context namespace, table filters, naming, and overwrite
  defaults are likewise verified through real CLI scaffolds across all four
  providers, with generated output built under the target project. Explicit CLI
  `--project`, `--startup-project`, and `--context` values are verified to
  override conflicting
  `.config/dotnet-ef.json` defaults across all four providers while still
  building the generated target project. Explicit `--startup-project`/`-s` named-connection
  lookup is also verified across all four providers: startup-project
  `appsettings.json` wins over target-project `appsettings.json`, both
  `.csproj` file paths and single-project startup directories are accepted,
  startup-project shorthand `Name=AppDb` resolves through startup-project
  `ConnectionStrings:AppDb` entries, and the generated model builds under the target project. Startup-project
  user secrets declared through `UserSecretsId` are verified across all four
  providers too: the startup-project user-secret connection wins over
  target-project user secrets and both startup/target appsettings files, while
  generated code still builds under the target project. Omitting `--context` with a named connection whose final
  configuration-key segment already ends in `Context` is verified across all
  four providers: the generated context uses that named leaf rather than a
  provider/database-specific resolved catalog name, and does not append a
  duplicate `Context` suffix. Lowercase named-connection shorthand such as
  `name=AppDb` is also verified across all four providers against target-project
  `ConnectionStrings:AppDb` appsettings entries. Named-connection environment variable precedence is
  likewise verified across all four providers: `ConnectionStrings__Name`
  overrides project `appsettings.json` for `Name=...` scaffolds. Environment
  variables also override project user secrets declared through `UserSecretsId`,
  including startup-project and target-project user secrets when
  `--startup-project` is supplied, and the
  generated project builds from the environment-selected live database.
  Project user secrets declared through `UserSecretsId` are verified across all
  four providers as well: the user-secret connection wins over target-project
  `appsettings.json` and the generated project builds from the secret-selected
  live database. EF-style
  pass-through `-- --environment Production` app arguments are also verified
  across all four providers for named-connection lookup: environment-specific
  `appsettings.Production.json` overrides base `appsettings.json`, startup-project
  `appsettings.Production.json` wins over target-project environment files
  when `--startup-project` is supplied, and the generated project builds from
  the selected live database. Ambient
  `ASPNETCORE_ENVIRONMENT=Staging` selection is likewise verified across all
  four providers, and `DOTNET_ENVIRONMENT=Development` fallback is verified
  with startup-project `appsettings.Development.json` winning over target-project
  environment files, so environment-specific appsettings lookup builds from the
  selected live database without mutating the test runner environment.
  Output-safety semantics
  are verified through the real CLI across all four providers as well:
  successful `--dry-run --json` does not create output files, `--no-overwrite`
  fails before touching existing files, and `--force --json` removes stale
  warning reports when the current scaffold is clean. CSV `--tables` filters,
  EF-style multi-value `--table First Second` filters, and object-kind-prefixed
  `table:`, `view:`, and `query:` filters are also verified through the real
  CLI across all four providers so filter parsing and provider catalog matching
  stay aligned. CSV `--schemas` filters and EF-style
  multi-value `--schema Accounting Sales` filters are also verified through the
  real CLI across all four providers: SQLite `main`, SQL Server and PostgreSQL
  schemas, plus MySQL's current database as a filter-only catalog that remains
  unqualified model metadata. Combined `--schema` plus explicit `--table`
  filters are verified through the real CLI across all four providers as a
  union: schema-selected tables and explicitly selected tables are generated,
  while unrelated tables outside the selected SQL Server/PostgreSQL schemas stay
  out of the generated model. EF compatibility switches that should not change
  nORM's generated model semantics are also verified through the real CLI
  across all four providers: `--no-onconfiguring` emits no `OnConfiguring` or
  hard-coded scaffold connection string, `--data-annotations` is accepted while
  preserving nORM's default annotation output, and `--no-pluralize` disables
  entity-name singularization and collection query-property pluralization. The
  same clean live scaffold verifies common
  output switches `--json`, `--verbose`, `--no-color`, and `--prefix-output`
  across all four providers by parsing a successful machine-readable summary
  with zero warnings. It also
  runs `--fail-on-warnings --json` against live keyless-table diagnostics so
  machine-readable warning summaries fail closed across all four providers.
  Explicit `--provider <EF Core provider package>` scaffolds and EF-style
  `dbcontext scaffold <connection> <provider-package>` both run against live
  schemas with accepted EF provider package names. Mixed connection/provider
  parser precedence is explicit and EF-compatible: `--connection` and
  `--provider` options take precedence over EF-style positional values when
  both forms are supplied. A single positional provider after `--connection`
  is accepted as the provider value. These live gates also include the
  compatibility design-time switch bundle `--no-build`,
  `--target-framework`/`--framework`, `--configuration`, `--runtime`, and
  `--msbuildprojectextensionspath`.
  Provider-bound routine stubs are verified through the real CLI with `--emit-routine-stubs` on SQL Server, PostgreSQL, and MySQL.
  Provider-bound sequence stubs are verified through the real CLI with `--emit-sequence-stubs` on SQL Server and PostgreSQL.
  Explicit SQL Server/PostgreSQL/SQLite primary-key constraint names and
  MySQL's fixed `PRIMARY` metadata are also verified through the real CLI while
  SQLite's unnamed PRAGMA-only key shape stays as unnamed `HasKey(...)`
  configuration.
  Table-filtered direct API and CLI
  scaffolds both prove relationships to unselected principal or dependent tables are
  suppressed rather than emitted as broken navigations, and selected ordinary
  views are emitted as read-only query artifacts without requiring
  `--emit-query-artifacts`. Key-looking view columns are verified through the
  direct API and real CLI across all four providers so `Id`/`ParentId` naming
  does not create generated keys, relationships, or write semantics. Default and schema-scoped CLI discovery also
  verifies ordinary views are emitted with discovered tables as read-only query
  artifacts across all four live providers. Provider query-artifact opt-in is
  also verified through the real CLI with `--emit-query-artifacts` for SQLite
  virtual tables, SQL Server local table/view synonyms, PostgreSQL materialized
  views, and MySQL views. SQL Server procedure-synonym rejection is verified
  through the real CLI so non-query synonyms do not scaffold as entity classes.
  MySQL scheduled-event diagnostics are verified through the real CLI so event
  inventory remains visible without blocking table scaffolding.
  PostgreSQL serial primary-key scaffolding is verified through the real CLI so
  owned sequences and generated defaults do not produce false warning reports.
  The real CLI path also builds nullable and non-null alternate-key FK relationships from live provider metadata, including the
  unique index, navigation attributes, and generated fluent FK mapping, and
  preserves non-default FK delete/update referential actions, including
  RESTRICT and SET DEFAULT referential actions where providers expose them.
  PostgreSQL deferrable FK timing semantics are verified as relationship
  diagnostics through the real CLI so they are not silently normalized into
  ordinary generated navigations. SQL Server disabled/untrusted/not-for-replication FK state is verified as relationship
  diagnostics through the real CLI so provider-managed FK enforcement state is not silently normalized into generated navigations.
  SQLite FK `MATCH FULL` / `DEFERRABLE`
  semantics are parsed from `CREATE TABLE` SQL and verified as relationship
  diagnostics because `PRAGMA foreign_key_list` does not preserve enough of
  that provider-owned clause text. It also
  proves safe string/binary defaults, table CHECK constraints, computed/generated columns,
  column collations, provider-native table/column comments,
  explicit SQL Server/PostgreSQL/SQLite primary-key constraint names,
  SQL Server/PostgreSQL/MySQL routine comments,
  SQL Server/PostgreSQL sequence comments, SQL Server local-synonym comments,
  and SQL Server/PostgreSQL
  view/materialized-view query-artifact comments as generated XML documentation
  are promoted through the real CLI scaffold path
  instead of being downgraded to provider-owned diagnostics; these metadata rows
  are not downgraded to provider-owned diagnostics.
  Advanced routine wrappers
  are verified through the real CLI for SQL Server scalar/table-valued
  functions, PostgreSQL array/domain/UUID routine parameters, PostgreSQL
  extension-owned routines, and MySQL unsigned
  routine parameters. SQL Server table-valued parameters and PostgreSQL
  overloaded and quoted-parameter function wrappers are also verified through
  the real CLI with consumer builds. PostgreSQL scalar set-returning functions
  are verified through the real CLI as `Value` result DTO wrappers instead of
  `SELECT * FROM` calls. SQL Server and MySQL output-parameter routine factories,
  plus SQL Server no-result stored procedure non-query wrappers, are also
  verified through the real CLI. Trigger-owned tables are
  also scaffolded as read-only entities while the JSON warning report preserves
  trigger metadata across all four live providers. Unsafe provider-specific columns are verified through the real CLI as SCF104 diagnostics with generated `[ReadOnlyEntity]` write blocking across all four live providers.
  Safe provider-specific columns are also verified through the real CLI across
  all four live providers, covering SQLite UUID/JSON/XML declared types, SQL
  Server `xml`, PostgreSQL UUID/array columns, and MySQL JSON/YEAR/enum/set
  columns as writable generated code with no unsafe warning report.
  Writable provider-specific diagnostics are verified through the real CLI for
  SQL Server alias types, PostgreSQL domains over safe base types, and MySQL
  unsigned columns: generated CLR types/facets are preserved, SCF104 remains in
  the warning report for provider-mobility review, and `[ReadOnlyEntity]` is
  not emitted. SQL Server rowversion concurrency metadata is verified through
  the real CLI as `[Timestamp]` plus computed database-generation metadata while
  the provider-owned DDL remains visible as SCF108. SQL Server provider-native
  temporal table diagnostics are verified through the real CLI as read-only
  base/history entities with SCF115 metadata.
  Decimal precision plus
  bounded string/binary length, Unicode, and fixed-length facets are also
  verified through real CLI scaffolds on the providers whose catalogs expose
  those facets. SQLite participates by parsing declared `VARCHAR(n)`,
  `CHAR(n)`, `BINARY(n)`, `VARBINARY(n)`, `DECIMAL(p,s)`, and
  `NUMERIC(p,s)` type text from `PRAGMA table_xinfo`. Ordinary
  single-column indexes and ordered unique composite indexes are likewise
  verified through generated `[Index]` attributes and consumer builds on all
  four live providers. Provider-bound filtered/partial predicates, descending
  key order, and included-column index metadata are also verified through the
  real CLI where each provider exposes the feature. Provider-bound expression-index
  metadata is verified through the real CLI for SQLite, PostgreSQL, and MySQL;
  SQL Server expression-index equivalents require generated/computed columns plus
  ordinary indexes. Provider-specific index implementations are verified through
  the real CLI for SQL Server, PostgreSQL, and MySQL as SCF119 diagnostics rather
  than generated index metadata. MySQL prefix-index diagnostics are verified
  through the real CLI as SCF117 while full-length prefixes remain ordinary
  index metadata. PostgreSQL `NULLS FIRST/LAST` and `NULLS NOT DISTINCT` index
  facets are verified through the real CLI as generated `[Index]` metadata
  without provider-owned warnings. PostgreSQL expression-index `INCLUDE` and
  null-semantics facets are verified through the real CLI as generated
  `HasExpressionIndex` metadata without provider-owned warnings. PostgreSQL
  expression B-tree operator-class/key-option diagnostics are verified through
  the real CLI as SCF119 provider-owned metadata instead of generated
  `HasExpressionIndex` calls. Multiple composite FKs to
  the same principal are also verified through role-named navigations and
  payload-bridge diagnostics on the real CLI path. Direct composite alternate-key FKs are verified through the real CLI across all four providers.
  Multiple unique dependent
  FKs to the same principal are verified as role-named one-to-one relationships
  through the real CLI path as well. Single-column required and nullable
  unique-dependent FKs are verified as one-to-one relationships through one
  combined real CLI scaffold. Composite unique-dependent FKs are
  verified as required one-to-one relationships through the real CLI, and
  nullable composite unique-dependent FKs are also verified as optional one-to-one relationships
  through the same command path. Self-referencing FKs are verified as role-named
  one-to-many relationships through the real CLI path. Self-referencing unique dependent FKs are verified
  as role-named one-to-one relationships through the real CLI path. Shared primary-key FK shapes are verified as
  one-to-one relationships through the real CLI as well. Keyless dependent and
  keyless principal FK shapes are verified through the real CLI to remain
  read-only, navigation-free, and reported as `RelationshipDependentKey` or
  `RelationshipPrincipalKey` diagnostics.
  Self-referencing pure join tables are also verified through the real CLI path
  with distinct role-based skip navigations. Generated-surrogate pure join
  tables are verified through the real CLI path by suppressing the bridge
  entity and emitting skip navigations from the exact unique FK-pair index.
  Pure composite primary-key join tables are verified through the real CLI path
  with exact composite FK column arrays.
  Shared-tenant alternate-key join tables are verified through the real CLI path
  with composite alternate-key principal selectors.
  Pure many-to-many join-table delete/update actions are verified through the
  real CLI path across SQLite, SQL Server, PostgreSQL, and MySQL.
  Database-generated bridge columns are also verified through the real CLI path
  so generated columns do not incorrectly force payload join entities.
  Composite generated-surrogate pure join tables are verified through the real
  CLI path by suppressing the bridge entity and emitting skip navigations from
  the exact composite FK-pair index.
  Composite payload join tables are verified through the real CLI path as
  explicit join entities with payload-column diagnostics without composite-FK rejection.
  Filtered unique surrogate join tables are verified through the real CLI where
  providers expose filtered or partial indexes. They remain explicit entities instead of unsafe skip navigations.
  Nullable-FK surrogate join tables are verified through the real CLI across all four
  live providers; they remain explicit entities with `nullable-foreign-key`
  diagnostics instead of unsafe skip navigations.
  Provider-owned trigger bridge tables are verified through the real CLI across all four
  live providers; they remain explicit `[ReadOnlyEntity]` join entities with
  `provider-owned-write-blocking-schema` diagnostics instead of unsafe skip navigations.
  Schema/catalog-qualified many-to-many join tables are verified through direct
  and real CLI scaffolds across all four providers: schema-aware providers keep
  schema-aware `UsingTable` mapping, while MySQL current-catalog-qualified
  filters qualify selection only and do not emit the catalog as a nORM schema in
  generated entity or `UsingTable` mappings.
  Inference-boundary scaffolds are verified through direct and real CLI runs
  across all four live providers: discriminator-looking columns and owned-type
  naming conventions remain ordinary scalar columns, because nORM does not infer
  owned types or inheritance from database naming patterns.
- `SchemaSignatureTests` covers dynamic scaffolding schema signatures,
  duplicate generated property handling, quoted and dotted literal identifier
  preservation, SQLite `UUID` declared-type parity, keyless dynamic
  `[ReadOnlyEntity]` parity, provider-owned dynamic read-only parity for
  query objects, unmodeled defaults, write-blocking provider-specific column types,
  unsafe PostgreSQL provider-specific array columns, and unsafe MySQL
  `set(...)` declarations,
  composite primary-key ordinal parity,
  computed expression/storage, identity, and rowversion, string/binary Unicode and
  fixed-length metadata in the dynamic cache key and generated attributes,
  normalized dynamic `MaxLength` metadata for generated string and binary
  properties, dynamic decimal precision/scale `Column(TypeName)`
  metadata, PostgreSQL array/domain-column schema probes with base-type preservation,
  non-null reference-column `[Required]` parity, and connection ownership for
  sync/async dynamic scaffolding calls.
- `DynamicTypeQueryTests` proves `DbContext.Query(string)` materializes rows
  when runtime-generated table or column mappings contain literal dotted
  identifiers and that keyless or provider-owned runtime-generated types reject
  generated write paths through the same `[ReadOnlyEntity]` guard as static
  scaffolds.
- `LiveProviderScaffoldingParityTests` covers single-column FK relationship
  scaffolding, one-to-one reference navigation generation for required and
  optional single-column, required and optional composite, shared-primary-key,
  self-referencing one-to-many, self-referencing unique dependent,
  and role-named multiple unique dependent FKs, composite-FK relationship generation when the FK targets the
  generated primary key, composite-FK diagnostics for unsupported relationship
  shapes, single-column nullable and non-null alternate-key FK generation,
  dynamic scaffolding of computed/generated columns, identity columns,
  database-name preservation with role-named FK navigations,
  relationship suppression that keeps scalar FK columns and pure bridge
  tables as explicit entities when `NoRelationships`/`--no-relationships` is
  enabled,
  and composite primary-key ordinal order across live providers,
  nullable-FK many-to-many bridge rejection,
  provider-owned many-to-many bridge rejection,
  schema/catalog-qualified many-to-many filter parity,
  inference-boundary non-inference for owned types and inheritance,
  key-looking view read-only boundary coverage,
  provider-owned/default-promotion and
  keyless-table diagnostics, and skipped-view table-filter failures against
  SQLite and any configured SQL Server, PostgreSQL, and MySQL live providers.
- `RelationshipConfigurationTests` covers generated non-cascade relationship
  metadata, one-to-one `HasOne(...).WithOne(...)` metadata, and proves the
  public fluent API keeps cascade behavior explicit.
- `MigrationFluentSnapshotTests` and `MigrationDefaultsIdentityTests` prove
  fluent identity options flow into schema snapshots and SQL Server
  `IDENTITY(seed, increment)` migration DDL.
- `UpdateNoMutableColumnsTests` covers generated/computed-column exclusion from
  insert and update column sets.
- `PublicApiSnapshotTests` and `PublicApiClassificationTests` keep scaffold
  API additions intentional and documented.
- `ScaffoldOptions.UseNullableReferenceTypes` controls generated nullable
  annotations for runtime scaffolding callers; CLI project inference sets the
  same option automatically.

## Not Yet Stable

- Relationship generation beyond primary keys and exact ordered unfiltered unique indexes.
  Single-column and composite FK constraints that target the exact generated
  primary key or an exact ordered unfiltered unique index are emitted as navigations and fluent
  model configuration. FKs that target keyless principals, start from keyless
  dependents, or target non-unique alternate columns are discovered and reported
  in scaffold diagnostics.
- FK referential-action and provider-owned FK semantic modeling outside the common provider action set.
  `NO ACTION`, `CASCADE`, `SET NULL`, `RESTRICT`, and `SET DEFAULT` are emitted
  into generated fluent configuration for delete/update actions. Unknown
  provider-specific action tokens and FK timing/match semantics such as
  PostgreSQL `DEFERRABLE`, `INITIALLY DEFERRED`, and `MATCH FULL`, plus
  SQLite `MATCH FULL` and `DEFERRABLE INITIALLY DEFERRED`, and SQL Server
  disabled/untrusted/not-for-replication FK state are discovered from
  provider metadata or parsed DDL and reported in scaffold diagnostics, and generated
  navigations/fluent relationships for those FKs are suppressed so unknown
  provider behavior is not silently normalized to `NO ACTION`.
- Owned types and inheritance inference.
- Payload bridge tables are modeled as explicit join entities, not skip
  navigations. Many-to-many joins whose bridge columns are nullable, whose key
  shape is neither an exact FK-column primary key nor a generated surrogate key
  plus exact FK-column unique index, or whose foreign keys do not target the
  generated primary keys or exact ordered unfiltered unique indexes, or whose
  referential actions are provider-specific, are reported in scaffold
  diagnostics rather than converted into unsafe fluent mappings. Structurally
  pure bridges with provider-owned write-blocking schema, such as triggers,
  unmodeled defaults, unparsed identity strategies, provider-native temporal
  behavior, or unsafe provider-specific column types, also stay as explicit
  read-only join entities instead of generating writable skip navigations.
- Provider-specific complex default constraints, column types, triggers,
  temporal tables, and keyless tables.
  Simple safe default literals/functions, including safe hex/binary/bit-string literals,
  typed temporal and interval literals,
  literal-only `LOWER('value')`/`UPPER('value')` string normalization defaults
  with provider-normalized PostgreSQL casts or MySQL character-set literals, and
  safe PostgreSQL typed-cast defaults, are emitted as migration metadata with
  `HasDefaultValueSql`; SQL Server explicit non-system default-constraint
  names are preserved with `HasDefaultValueSql(..., constraintName: ...)`;
  unmodeled complex/provider-specific defaults remain diagnostics;
  table CHECK constraints are emitted as provider-bound
  migration metadata with `HasCheckConstraint`. SQL Server CHECK constraint
  names marked `is_system_named` by the catalog, PostgreSQL default
  `<table>_<columns>_check` names, and MySQL default `<table>_chk_<n>` names
  are replaced with stable generated `CK_<Entity>_<hash>` names so scaffold
  output preserves the predicate without carrying generated catalog artifacts.
  MySQL `ON UPDATE` timestamp defaults remain provider-specific default
  diagnostics rather than being flattened into plain `HasDefaultValueSql`
  metadata, because generated writes cannot infer the provider-managed update
  behavior safely.
  Computed/generated column expressions are emitted as provider-bound migration metadata with
  `HasComputedColumnSql`; column collations are emitted as provider-bound
  migration metadata with `HasCollation`; complex/provider-specific defaults
  that fail the allowlist remain diagnostics and make the generated entity
  `[ReadOnlyEntity]`. Safe scalar citext/JSON/XML/UUID storage
  types are promoted into generated CLR properties where provider metadata
  exposes them; native JSON/XML operators remain provider-bound. PostgreSQL
  arrays over safe scalar elements are emitted as CLR arrays but remain
  provider-specific schema for provider-mobility review. Parsed SQL Server
  `IDENTITY(seed, increment)` metadata is emitted with
  `HasIdentityOptions`; unsafe provider-specific column types make the generated
  entity read-only until the provider-owned type is hand-modeled. SQL Server
  alias types over scaffoldable scalar/binary bases stay writable but diagnostic.
  PostgreSQL
  domains over safe scalar/array/enum base types remain diagnostics for
  provider-mobility review, but generated writes stay enabled because nORM binds
  the safe base CLR type, preserves bounded string/numeric facets where provider
  metadata exposes them, and the database still enforces the domain constraint.
  MySQL unsigned integer widths remain writable because nORM maps them to exact unsigned CLR
  types, and unsigned decimal/numeric columns remain writable with preserved
  precision/scale metadata. Unparsed provider-specific identity strategies and unsafe
  provider-specific column types make the generated entity read-only until the
  provider-owned behavior is hand-modeled. Provider-specific column types,
  unparsed identity strategies, unrecognized FK referential actions, triggers, SQL Server provider-native temporal
  tables, keyless tables, SQLite virtual-table shadow tables, sequences,
  unresolved/non-query synonyms, and events are discovered and reported in
  scaffold diagnostics, but not converted into complete provider-neutral model
  code. Ordinary views and PostgreSQL materialized views are emitted as
  read-only query artifacts by default; tables with provider-owned triggers are
  emitted read-only for generated-write safety; SQLite virtual tables and resolved
  table/view SQL Server synonyms can be emitted when explicitly selected by
  table/schema filters or when query-artifact emission is enabled for the run.
  Generated query artifacts carry explicit keyless warnings where needed.
  Routine wrappers can be emitted as opt-in provider-bound call stubs.
  Routine diagnostics include provider metadata such as parameter counts,
  output-parameter counts where the provider exposes them, ordered parameter
  mode/type summaries, INOUT direction, output string/binary sizes where
  providers expose them, SQL Server first-result-set columns where metadata is
  available, and result/data type hints so stored procedures/functions are not
  lost during provider-mobility review.
- SQLite expression indexes, ordinary PostgreSQL B-tree expression indexes,
  and MySQL expression indexes exposed by `SHOW INDEX` are emitted as
  provider-bound `HasExpressionIndex` migration metadata, including
  filtered/partial predicates for the same expression index when the provider
  supports them. PostgreSQL B-tree expression indexes with DDL-exposed
  `INCLUDE` columns, non-default `NULLS FIRST/LAST` ordering, or
  `NULLS NOT DISTINCT` uniqueness are emitted with expanded
  `HasExpressionIndex` metadata when no non-default operator class or index
  collation is present. SQL Server and PostgreSQL expression indexes that
  depend on provider-specific access methods, non-default operator classes, or index collations
  require generated/computed columns plus ordinary indexes or provider-specific migration code. v1 scaffolding emits
  provider-neutral key-column indexes, including descending key order for ordinary column indexes, provider-bound
  filtered/partial index metadata for SQL Server, PostgreSQL, and SQLite, and
  provider-bound included-column index metadata for SQL Server and PostgreSQL,
  plus PostgreSQL `NULLS NOT DISTINCT` unique column-index metadata and
  ordinary PostgreSQL column-index `NULLS FIRST/LAST` metadata.
  Mixed functional indexes are not partially emitted as ordinary column indexes;
  the whole index remains provider-owned once any key part is expression-based.
  Provider-specific index implementations such as SQL Server columnstore/XML,
  PostgreSQL GIN/GiST/hash/BRIN, PostgreSQL B-tree indexes with non-default
  operator classes or index-level collations, and MySQL
  FULLTEXT/SPATIAL/HASH indexes remain diagnostics instead of being flattened
  into portable `[Index]` attributes.

## v1 Guidance

Use scaffolding to bootstrap a model, then edit the generated files into the
model you want to own. For production applications, commit the generated code
and review diffs like handwritten model code. For repeated database-first
scaffolds, keep custom members and override configuration in separate partial
class files instead of editing generated files directly.

The `dotnet-norm scaffold` command shares the v1 contract above. It uses the
same `DatabaseScaffolder` implementation as the runtime API; both surfaces
support the same "Supported" scope and have the same out-of-scope items.

Examples:

```bash
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --namespace App.Data
norm scaffold "Data Source=app.db" sqlite --output-dir Models -n App.Data -c AppDbContext
norm scaffold "Data Source=app.db" Microsoft.EntityFrameworkCore.Sqlite --project src/App/App.csproj --output-dir Models --context AppDbContext --no-build
norm scaffold "Name=ConnectionStrings:AppDb" Microsoft.EntityFrameworkCore.Sqlite --project src/App/App.csproj --output-dir Models
norm scaffold "Name=AppDb" Microsoft.EntityFrameworkCore.Sqlite --project src/App/App.csproj --output-dir Models
norm scaffold "Data Source=app.db" sqlite --output-dir Models --json
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --tables public.customer,public.order --no-overwrite --fail-on-warnings
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --schema accounting --schema sales
norm scaffold --provider sqlite --connection "Data Source=legacy.db" --table "Keep,Me"
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --no-pluralize
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --use-database-names
norm scaffold --provider sqlite --connection "Data Source=app.db" --output Models --namespace App.Data.Entities --context-dir Data/Contexts --context-namespace App.Data.Contexts
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
  For `SCF001` composite-FK rows this includes
  `relationshipSuppressed`, the suppression `reason`, ordered
  dependent/principal columns, and whether the FK references the generated
  primary key or a scaffoldable unique index.
  For `SCF002` possible many-to-many rows this includes bridge evidence such
  as `foreignKeyColumns`, `primaryKeyColumns`, `payloadColumns`,
  `databaseGeneratedColumns`, `identityColumns`, `nullableForeignKeyColumns`,
  bridge-key booleans, FK-column unique-index candidates with filter evidence,
  and per-constraint principal/dependent column maps plus declared/observed FK
  metadata row counts.
  For `providerOwnedSchemaFeatures` rows this includes stable kind-specific
  facts where nORM can derive them, such as keyless-table
  `readOnlyEntity`/`generatedWritesSupported`, `table`, ordered `columns`,
  generated `properties`, `columnCount`, and the `missing-primary-key`
  `reason`, suppressed-relationship
  `navigationSuppressed`/`generatedNavigationSupported`,
  dependent/principal table and column details, referential
  `onDelete`/`onUpdate` actions plus the suppression `reason`,
  decimal `precision`/`scale`, computed-column
  `computedSql`/`stored`, provider `collation`, rowversion concurrency,
  database-generation, generated-model, generated-write, and provider-owned DDL
  flags, parsed identity `seed`/`increment`, and index-shape flags plus parsed
  `indexSql`/`keySql`/`expressionSql`/`filterSql`/`isUnique` and MySQL
  prefix-column lengths plus declared-length coverage flags when the provider
  exposes them. Provider-specific
  column rows include `providerType`, `readOnlyEntity`,
  `generatedWritesSupported`, and a machine-readable `reason`.
  Provider-specific
  index rows also include `indexType`/`accessMethod` and PostgreSQL B-tree
  reason flags such as `hasNullsNotDistinct`,
  `hasNonDefaultOperatorClass`, `hasIndexCollation`, and
  `hasNonDefaultNullOrdering` when available. Trigger rows include
  provider/object-kind metadata, `table`, `triggerName`, `providerOwnedDdl`,
  `generatedModelConfigurationSupported`, `readOnlyEntity`,
  `generatedWritesSupported`, `reason`, optional
  `timing`/`event`/`orientation`, SQL Server
  `isDisabled`/`isInsteadOf`, and SQLite `triggerSql` when the provider
  exposes the definition. Provider-native temporal table rows include
  provider/object-kind metadata, `table`, `providerNativeTemporal`,
  `generatedTemporalConfigurationSupported`, `readOnlyEntity`,
  `generatedWritesSupported`, a machine-readable `reason`, `temporalType`, and
  optional SQL Server `historyTable`.
  For `SCF200`/`SCF204`/`SCF206` query-object rows this includes `provider`,
  `targetKind`, and `queryArtifactSupported`; `SCF207` SQLite virtual-table
  shadow rows also include `shadowOf` when the owning virtual table can be
  inferred from SQLite's shadow-table naming convention.
  For `SCF201` routine rows this includes `provider`, `routineType`,
  `parameterCount`, `outputParameterCount`, optional routine `dataType`, and
  ordered `parameters` entries with `name`, `mode`, and provider `dataType`
  when the database exposes it. For `SCF202` sequence rows this includes
  `provider`, `stubSupported`, generated value `clrType`, and optional
  provider `dataType`. For SQL Server `SCF203` synonym rows this includes
  `provider`, `baseObject`, `baseType`, `targetKind`, and
  `queryArtifactSupported`. For `SCF205` MySQL event rows this includes
  `provider`, `eventType`, `status`, and optional schedule fields such as
  `intervalValue`, `intervalField`, `executeAt`, `starts`, and `ends`.
- `reasons`: present on `SCF002` possible many-to-many rows. Values explain
  why the bridge was not emitted as `UsingTable`, for example
  `payload-columns`, `nullable-foreign-key`,
  `foreign-key-metadata-incomplete`, `missing-primary-key`,
  `primary-key-not-exact-bridge-columns`, `missing-exact-unique-index`, or
  `principal-key-not-scaffoldable`, `referential-action-not-scaffoldable`, or
  `provider-owned-write-blocking-schema`. Metadata includes
  `providerOwnedWriteBlockingSchema` so tooling can distinguish review-only
  bridge shapes from bridges suppressed because generated writes would be unsafe.

Use `code` and `category` for CI baselines, owner routing, and remediation
dashboards. Use provider-owned feature, query-object, routine, sequence,
synonym, and event `metadata` for schema remediation, query artifact review,
stored procedure/function/sequence migration inventory, synonym target review,
and scheduled-event ownership review. Do not parse `detail` or
`suggestedAction` text as a stable API.

### Diagnostic Code Catalog

| Code | Category | Meaning |
| --- | --- | --- |
| `SCF001` | `relationship` | Unsupported composite foreign key discovered; scalar columns are generated, but no navigation is emitted because it does not target the generated principal primary key or an exact ordered unfiltered unique index. |
| `SCF002` | `many-to-many` | Possible many-to-many table discovered. Pure single-column, composite-key, alternate-key, and generated-surrogate-key bridges can be generated as `UsingTable`; payload-capable, nullable, keyless, or non-unique bridges stay as join entities until explicitly modeled. |
| `SCF100` | `schema-feature` | Database default expression discovered. Simple safe defaults, including safe hex/binary/bit-string literals, typed temporal and interval literals, literal-only `LOWER('value')`/`UPPER('value')` string normalization defaults with provider-normalized PostgreSQL casts or MySQL character-set literals, and safe PostgreSQL typed-cast defaults, are emitted as `HasDefaultValueSql`; SQL Server explicit non-system default-constraint names are preserved with the optional `constraintName` argument. MySQL `ON UPDATE` timestamp defaults remain provider-specific diagnostics because they mutate values during updates. Unmodeled complex/provider-specific defaults remain diagnostics and make the generated entity `[ReadOnlyEntity]`. |
| `SCF101` | `schema-feature` | Computed/generated column expression discovered but not emitted. Ordinary generated-column expressions are emitted as `HasComputedColumnSql`. |
| `SCF102` | `schema-feature` | Check constraint discovered but not emitted. Ordinary table CHECK constraints are emitted as `HasCheckConstraint`; SQL Server, PostgreSQL, and MySQL provider-default names are replaced with stable generated names. |
| `SCF103` | `schema-feature` | Provider/database collation discovered but not emitted because no generated property could safely own it. Ordinary column collations are emitted as `HasCollation`. |
| `SCF104` | `schema-feature` | Provider-specific column type discovered. SQLite declared `UUID`, `JSON`, and `XML`, SQL Server `xml`, PostgreSQL `citext`/`json`/`jsonb`/`xml`/`uuid` plus safe scalar arrays/simple enums, and MySQL `json`/`year`/simple `enum(...)` plus bounded simple `set(...)` are scaffolded as supported storage; MySQL unsigned integer and decimal/numeric columns, SQL Server alias types over scaffoldable scalar/binary bases, and PostgreSQL domains over safe scalar/array/enum base types preserve generated writes and bounded facets where provider metadata exposes them while remaining diagnostics because the DDL is provider-specific. Unsafe provider-specific declarations such as SQL Server/SQLite/MySQL spatial types like `GEOMETRY`/`POINT`, PostgreSQL network/search types such as `inet`, and larger or ambiguous MySQL `set(...)` declarations remain diagnostics and make the generated entity `[ReadOnlyEntity]` so generated writes fail closed. |
| `SCF105` | `schema-feature` | Decimal precision/scale metadata discovered but not emitted. Parsed precision and precision/scale facets are emitted as `HasPrecision`; remaining rows indicate provider numeric facet text that needs explicit model configuration or provider migration DDL. |
| `SCF106` | `relationship` | Unsupported/provider-specific FK referential action or FK timing/match semantic discovered. Valid `NO ACTION`, `CASCADE`, `SET NULL`, `RESTRICT`, and `SET DEFAULT` actions are emitted in generated fluent configuration; PostgreSQL `DEFERRABLE`, `INITIALLY DEFERRED`, and `MATCH FULL`, SQLite `MATCH FULL` / `DEFERRABLE INITIALLY DEFERRED`, and SQL Server `NOT TRUSTED`, `DISABLED`, or `NOT FOR REPLICATION` FK state remain diagnostics so generated navigations are suppressed. |
| `SCF107` | `relationship` | FK targets principal columns that are neither the generated primary key nor an exact ordered unfiltered unique index. |
| `SCF108` | `schema-feature` | Provider rowversion/timestamp column discovered. Static and dynamic scaffolding emit `[Timestamp]` plus computed database-generation metadata so generated writes and concurrency checks can use the column; exact provider rowversion/timestamp DDL remains provider-owned. |
| `SCF109` | `schema-feature` | Provider-specific identity strategy discovered. SQL Server `IDENTITY(seed, increment)` is emitted as `HasIdentityOptions`; unparsed strategies remain diagnostics and make the generated entity `[ReadOnlyEntity]`. |
| `SCF110` | `database-object` | Trigger discovered; the generated table type is read-only until trigger side effects are hand-modeled. |
| `SCF111` | `index` | Filtered/partial index discovered. |
| `SCF112` | `index` | Expression index discovered but not emitted. SQLite, ordinary PostgreSQL B-tree, and MySQL expression indexes are emitted as `HasExpressionIndex`, including descending expression keys, filtered/partial predicates, and representable PostgreSQL expression-index `INCLUDE`, `NULLS FIRST/LAST`, and `NULLS NOT DISTINCT` facets when the provider supports them. SQL Server, provider-specific-access-method expression indexes, and expression indexes with non-default operator classes or index collations remain provider-owned diagnostics. |
| `SCF113` | `index` | Included-column index discovered. Ordinary SQL Server/PostgreSQL included-column indexes are emitted with `IndexAttribute.IsIncluded`; PostgreSQL expression-index INCLUDE columns are emitted through `HasExpressionIndex` when the DDL exposes exact included column names. This diagnostic remains for included-column facets that cannot be safely attached to generated index metadata. |
| `SCF114` | `index` | Descending index key discovered. |
| `SCF115` | `database-object` | Provider-native temporal table discovered. |
| `SCF116` | `table-shape` | Table has no primary key. |
| `SCF117` | `index` | MySQL prefix index discovered. Prefix indexes stay provider-owned and are not used as full-column unique alternate-key evidence unless every prefix covers the full declared column length. |
| `SCF118` | `relationship` | FK dependent table has no primary key, so generated navigations are suppressed. |
| `SCF119` | `index` | Provider-specific index implementation discovered. Non-rowstore/non-B-tree access methods and provider-specific B-tree operator classes/collations on column or expression indexes remain provider-owned diagnostics. Ordinary PostgreSQL column indexes with non-default `NULLS FIRST/LAST` ordering are emitted with `IndexAttribute.NullSortOrder`; ordinary PostgreSQL unique column indexes with `NULLS NOT DISTINCT` are emitted with `IndexAttribute.NullsNotDistinct`; representable PostgreSQL B-tree expression-index null ordering and `NULLS NOT DISTINCT` uniqueness are emitted through `HasExpressionIndex`. |
| `SCF199` | `schema-feature` | Unknown provider-owned schema feature. |
| `SCF200` | `query-object` | View discovered but not emitted. Ordinary views are scaffolded by default as read-only query artifacts; this code is reserved for provider-specific skipped view metadata. |
| `SCF201` | `routine` | Routine/stored procedure/function discovered; skipped unless provider-bound stubs are emitted through `--emit-routine-stubs`. |
| `SCF202` | `key-generation` | Standalone sequence discovered and skipped. |
| `SCF203` | `database-object` | SQL Server synonym discovered and skipped. Local table/view synonyms can be emitted when explicitly selected by table/schema filters or through `--emit-query-artifacts`; procedure, remote, or unresolved synonyms remain diagnostics. |
| `SCF204` | `query-object` | PostgreSQL materialized view discovered but not emitted. Materialized views are scaffolded by default as read-only query artifacts; refresh semantics remain provider-owned. |
| `SCF205` | `routine` | MySQL event discovered and skipped. |
| `SCF206` | `virtual-table` | SQLite virtual table discovered; skipped unless explicitly selected by table/schema filters or emitted as a query artifact through `--emit-query-artifacts` / `--emit-view-entities`. |
| `SCF207` | `virtual-table` | SQLite virtual-table shadow table discovered and skipped. |
| `SCF299` | `database-object` | Unknown skipped database object. |

For v1 runtime mapping, `UsingTable` skip navigations support single-column,
composite-key, shared-column tenant, and alternate-key pure junction tables when
both sides reference either the generated primary keys or exact ordered unfiltered unique indexes.
Action-aware overloads preserve per-side join-table FK delete/update actions in
mapping metadata and migration snapshots.
Unsafe bridge shapes remain explicit join entities.

The report is additive: new fields may be added in later versions, but v1 tools
should tolerate unknown fields and should not treat an empty diagnostics file as
provider-mobility evidence.
