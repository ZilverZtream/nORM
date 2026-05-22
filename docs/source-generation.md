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
