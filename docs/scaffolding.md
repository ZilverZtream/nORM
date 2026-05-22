# Scaffolding Preview Contract

Database scaffolding is a preview feature for v1. It is useful for generating a
starting point from an existing schema, but generated code must be reviewed
before it is treated as the application's model contract.

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

The `dotnet-norm scaffold` command is also preview. It uses the same
`DatabaseScaffolder` implementation as the runtime API.
