# Scaffolding Contract

Scaffolding ships as a preview v1 tooling surface with the scope below. The
public entry points (`nORM.Scaffolding.DatabaseScaffolder`,
`nORM.Scaffolding.DynamicEntityTypeGenerator`, and the `dotnet-norm scaffold`
command) are API-snapshotted so callers can experiment without churn, but the
generated model is intentionally a bootstrap artifact, not a database-first completeness claim.

The "Supported" section is the v1 preview contract; everything in "Not Yet
Stable" is explicitly out of scope for v1.0 and tracked for v1.x. Generated code
must be reviewed and edited like handwritten model code.

## Supported

- Table discovery for SQL Server, PostgreSQL, MySQL, and SQLite.
- Entity class generation with `[Table]`, `[Column]`, `[Key]`, identity, and
  simple `[MaxLength]` annotations when provider metadata exposes them.
- Nullable reference/value type generation from database nullability metadata.
- `DbContext` generation with `INormQueryable<T>` properties.
- Provider-specific identifier escaping for the zero-row schema query.
- Generated C# identifiers are sanitized: invalid characters become `_`,
  leading digits are prefixed with `_`, and C# keywords use `@`.

## Not Yet Stable

- Relationship and navigation generation.
- Index generation.
- Composite-key and alternate-key modeling beyond provider schema metadata.
- Owned types, many-to-many join-table modeling, and inheritance inference.
- Provider-specific computed columns, default constraints, triggers, and
  temporal tables.

## v1 Guidance

Use scaffolding to bootstrap a model, then edit the generated files into the
model you want to own. For production applications, commit the generated code
and review diffs like handwritten model code.

The `dotnet-norm scaffold` command shares the v1 contract above. It uses the same
`DatabaseScaffolder` implementation as the runtime API; both surfaces support the same
"Supported" scope and have the same out-of-scope items.

Do not use scaffolding as evidence for provider mobility by itself. Provider
mobility is proven by generated nORM query/write paths, strict certification,
provider capability reports, and live provider gates after the model exists.
