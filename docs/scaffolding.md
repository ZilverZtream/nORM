# Scaffolding Contract

Scaffolding ships as a documented v1 feature with the scope below. The "Supported" section is
the v1 contract; everything in "Not Yet Stable" is explicitly out of scope for v1.0 and tracked
for v1.x. Generated code is still meant to be reviewed and edited (it is a starting point, not a
hand-off), but the public surface (`nORM.Scaffolding.DatabaseScaffolder`,
`nORM.Scaffolding.DynamicEntityTypeGenerator`, and the `dotnet-norm scaffold` command) is
stable v1.

## Supported

- Table discovery for SQL Server, PostgreSQL, MySQL, and SQLite.
- Entity class generation with `[Table]`, `[Column]`, `[Key]`, identity, and
  simple `[MaxLength]` annotations when provider metadata exposes them.
- Nullable reference/value type generation from database nullability metadata.
- `DbContext` generation with `INormQueryable<T>` properties.
- Provider-specific identifier escaping for the zero-row schema query.

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
