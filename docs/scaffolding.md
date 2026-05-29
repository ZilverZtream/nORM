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
- Entity class generation with `[Table]`, `[Column]`, `[Key]`, identity, and
  simple `[Required]`/`[MaxLength]` annotations when provider metadata exposes
  them.
- Nullable reference/value type generation from database nullability metadata.
- Non-null reference properties are initialized with `default!` so generated
  nullable-enabled code compiles cleanly before the application adds its own
  constructors or required-member style.
- `DbContext` generation with `INormQueryable<T>` properties.
- Provider-specific identifier escaping for the zero-row schema query.
- Generated C# identifiers are sanitized: invalid characters become `_`,
  leading digits are prefixed with `_`, and C# keywords use `@`.
- Generated class and property names are de-duplicated deterministically when
  different database identifiers normalize to the same C# identifier.
- Single-column foreign key relationship generation when provider metadata
  exposes the constraint. Generated entities include reference/collection
  navigations with `[ForeignKey]` metadata, and the generated `DbContext` wires
  them through `OnModelCreating` while preserving caller-supplied model
  configuration.
- Single-column non-primary-key index generation through nORM's `[Index]`
  metadata. Composite indexes remain explicit post-processing.
- Optional table filtering through `ScaffoldOptions.Tables` and CLI
  `--tables`.
- Optional overwrite protection through `ScaffoldOptions.OverwriteFiles` and
  CLI `--no-overwrite`.

## Evidence

- `ScaffoldingAndNavigationCoverageTests` covers identifier normalization,
  duplicate generated-name handling, table filtering, overwrite protection,
  nullable initialization, SQLite FK navigation generation, and SQLite
  single-column index generation.
- `SchemaSignatureTests` covers dynamic scaffolding schema signatures and
  duplicate generated property handling.
- `LiveProviderScaffoldingParityTests` covers single-column FK relationship
  scaffolding against SQLite and any configured SQL Server, PostgreSQL, and
  MySQL live providers.

## Not Yet Stable

- Composite foreign key relationship generation.
- Composite index generation.
- Composite-key and alternate-key modeling beyond provider schema metadata.
- Owned types, many-to-many join-table modeling, and inheritance inference.
- Provider-specific computed columns, default constraints, triggers, and
  temporal tables.

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
norm scaffold --provider postgres --connection "$NORM_POSTGRES" --tables public.customer,public.order --no-overwrite
```

Do not use scaffolding as evidence for provider mobility by itself. Provider
mobility is proven by generated nORM query/write paths, strict certification,
provider capability reports, and live provider gates after the model exists.
