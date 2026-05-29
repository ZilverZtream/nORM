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
- Entity class generation with `[Table]`, `[Column]`, `[Key]`, identity, and
  simple `[Required]`/`[MaxLength]` annotations when provider metadata exposes
  them.
- Nullable reference/value type generation from database nullability metadata.
- Non-null reference properties are initialized with `default!` so generated
  nullable-enabled code compiles cleanly before the application adds its own
  constructors or required-member style.
- `DbContext` generation with `IQueryable<T>` properties backed by nORM's
  query provider.
- Provider-specific identifier escaping for the zero-row schema query.
- Generated C# identifiers are sanitized: invalid characters become `_`,
  leading digits are prefixed with `_`, and C# keywords use `@`.
- Generated class and property names are de-duplicated deterministically when
  different database identifiers normalize to the same C# identifier.
- The requested namespace is validated before files are written, and the
  generated context file name follows the escaped context class name rather
  than the raw CLI/API input.
- Single-column foreign key relationship generation when provider metadata
  exposes the constraint. Generated entities include reference/collection
  navigations with `[ForeignKey]` metadata, and the generated `DbContext` wires
  them through `OnModelCreating` while preserving caller-supplied model
  configuration.
- Single-column, composite, and multi-membership non-primary-key index
  generation through nORM's `[Index]` metadata, including unique composite
  indexes without converting them into per-column uniqueness.
- Pure many-to-many join table generation for the safe v1 subset: exactly two
  single-column foreign keys, no payload columns, and both references targeting
  single-column primary keys. The join table is emitted as fluent
  `HasMany().WithMany().UsingTable(...)` configuration instead of a join entity.
- Optional table filtering through `ScaffoldOptions.Tables` and CLI
  `--tables`.
- Optional overwrite protection through `ScaffoldOptions.OverwriteFiles` and
  CLI `--no-overwrite`.
- Optional warning enforcement through `ScaffoldOptions.FailOnWarnings` and CLI
  `--fail-on-warnings`, which fails the scaffold run after writing
  `nORM.ScaffoldWarnings.md` and `nORM.ScaffoldWarnings.json`.
- Deterministic Markdown and JSON diagnostics for discovered database features
  that are not converted into runnable model code. Composite foreign keys are
  listed there instead of being silently ignored or converted into fake
  single-column navigations; defaults, computed/generated columns, and triggers
  are inventoried for review; SQL Server provider-native temporal tables and
  tables without primary keys are reported as provider-owned schema; views,
  routines, and sequences are discovered and reported as skipped database
  objects; likely many-to-many join tables are flagged when they are scaffolded
  as normal entities.

## Evidence

- `ScaffoldingAndNavigationCoverageTests` covers identifier normalization,
  duplicate generated-name handling, table filtering, overwrite protection,
  nullable initialization, SQLite FK navigation generation, and SQLite
  single-column/composite index generation and columns that participate in
  multiple indexes, plus composite-FK, many-to-many candidate, and
  provider-owned schema diagnostics.
- `SchemaSignatureTests` covers dynamic scaffolding schema signatures and
  duplicate generated property handling.
- `LiveProviderScaffoldingParityTests` covers single-column FK relationship
  scaffolding and composite-FK diagnostic shape against SQLite and any
  configured SQL Server, PostgreSQL, and MySQL live providers.

## Not Yet Stable

- Composite foreign key relationship navigation generation. Composite FK
  constraints are discovered and reported in scaffold diagnostics.
- Composite-key and alternate-key modeling beyond provider schema metadata.
- Owned types and inheritance inference.
- Payload join-table modeling and many-to-many joins whose foreign keys do not
  target single-column primary keys. These are discovered and reported in
  scaffold diagnostics rather than converted into unsafe fluent mappings.
- Provider-specific computed columns, default constraints, triggers, views,
  temporal tables, and keyless tables. Defaults, computed/generated columns,
  triggers, SQL Server provider-native temporal tables, keyless tables, views,
  routines, and sequences are discovered and reported in scaffold diagnostics,
  but not converted into complete provider-neutral model code.

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
- `compositeForeignKeys`
- `possibleManyToManyJoinTables`
- `providerOwnedSchemaFeatures`
- `skippedDatabaseObjects`

Each warning row includes a `suggestedAction` value. The action is intentionally
specific enough for CI/reporting tools to show the next manual step: add
explicit model configuration, keep provider-owned DDL in migrations, replace a
pure join entity with `UsingTable`, add a primary key, or hand-write a
provider-bound routine/view path.

The report is additive: new fields may be added in later versions, but v1 tools
should tolerate unknown fields and should not treat an empty diagnostics file as
provider-mobility evidence.
