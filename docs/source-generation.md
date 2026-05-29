# Source Generation

nORM has two source-generation surfaces:

- `src/SourceGeneration`: runtime attributes and the compiled materializer store
  that ship in the `nORM` runtime assembly.
- `src/nORM.SourceGenerators`: the Roslyn analyzer project that generates
  materializers and compile-time query methods.

There is no third implementation under `src/nORM/SourceGeneration`. That path
was a legacy excluded tree and must not be recreated. New generator work belongs
in `src/nORM.SourceGenerators`; new attributes or runtime registration helpers
belong in `src/SourceGeneration`.

The runtime package packs the analyzer DLL into
`analyzers/dotnet/cs/nORM.SourceGenerators.dll`, and package-consumer tests must
validate generation from the produced `.nupkg`, not from project references.

## v1 Materializer Support

`[GenerateMaterializer]` is stable for explicit entity types whose mapped scalar
properties can be assigned by generated code:

- The entity type must have a public parameterless constructor.
- Every mapped property must have get and set accessors accessible from generated
  code. Public and internal accessors are supported; private/protected setters
  and get-only computed properties are not.
- Computed or helper properties must be marked `[NotMapped]`.
- Attribute-based table and column names are supported.
- `[Owned]` type-level owned objects are supported when the owned type has an
  accessible parameterless constructor and assignable mapped properties.

Runtime-only mapping remains runtime-only. Fluent-only column renames,
`OwnsOne`, value converters, and provider/runtime model changes are detected by
the runtime materializer guard and fall back to runtime materialization. The
generator cannot see those fluent decisions at compile time, so source-generation
diagnostics focus on shapes visible in source.

## Compile-Time SQL Portability

`[CompileTimeQuery]` embeds caller-authored SQL. It is supported as a
compatibility/performance tool, but it is provider-bound and is not certified
provider-mobile generated surface. `UseStrictProviderMobility()` rejects the
runtime command helper used by generated compile-time SQL methods. For portable
compiled queries, use `Norm.CompileQuery` over LINQ so nORM owns translation per
provider.

## Diagnostics

| Id | Severity | Meaning |
| --- | --- | --- |
| `nORMSG001` | Error | `[CompileTimeQuery]` return type is not `Task<List<T>>`. |
| `nORMSG002` | Error | `[CompileTimeQuery]` is not declared on a public static partial top-level class. |
| `nORMSG003` | Error | `[GenerateMaterializer]` entity lacks a public parameterless constructor. |
| `nORMSG004` | Warning | `[CompileTimeQuery]` entity lacks `[GenerateMaterializer]` and will use runtime materialization. |
| `nORMSG005` | Error | A mapped property cannot be assigned by generated code; mark computed members `[NotMapped]` or remove `[GenerateMaterializer]`. |
| `nORMSG006` | Error | A source-visible `[Owned]` type cannot be constructed by generated code. |
