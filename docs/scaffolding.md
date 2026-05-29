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
- Single-column, composite, and multi-membership non-primary-key index
  generation through nORM's `[Index]` metadata, including unique composite
  indexes without converting them into per-column uniqueness.
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
  are inventoried for review; likely many-to-many join tables are flagged when
  they are scaffolded as normal entities.

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
  scaffolding against SQLite and any configured SQL Server, PostgreSQL, and
  MySQL live providers.

## Not Yet Stable

- Composite foreign key relationship navigation generation. Composite FK
  constraints are discovered and reported in scaffold diagnostics.
- Composite-key and alternate-key modeling beyond provider schema metadata.
- Owned types, many-to-many join-table modeling, and inheritance inference.
  Likely join tables are discovered and reported in scaffold diagnostics, but
  not converted into fluent many-to-many mappings.
- Provider-specific computed columns, default constraints, triggers, and
  temporal tables. Defaults, computed/generated columns, and triggers are
  discovered and reported in scaffold diagnostics, but not converted into
  provider-neutral model code.

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
