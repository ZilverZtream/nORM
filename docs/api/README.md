# nORM API Documentation

This directory contains DocFX-generated YAML metadata for the nORM public API.
The files are generated from the `src/nORM.csproj` Release build.

## Regenerating the API docs

Run the following command from the repository root before each release candidate:

```
docfx metadata docfx.json
```

This requires the [docfx](https://dotnet.github.io/docfx/) global tool:

```
dotnet tool install -g docfx
```

The command reads `docfx.json`, builds the `net8.0` Release target, and writes
updated YAML pages to `docs/api/`. Commit any changed or added pages as part of
the RC preparation step in `docs/release-checklist.md`.

## Validation

`DocumentationContractTests` contains tests that load the current public assembly
and reject generated doc pages that reference types absent from the assembly.
Run the tests to confirm no stale API pages remain after regeneration:

```
dotnet test tests/ --filter FullyQualifiedName~DocumentationContractTests --no-build
```

The RC release gate (`eng/v1-release-gate.ps1`) runs those tests on every build.
API docs must be regenerated and validated before tagging a v1.0 release.
