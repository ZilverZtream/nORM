# Domain 10 — Scaffolding & CLI tooling

**Scope:** the `dotnet norm` CLI (packaged as `dotnet-norm`), reverse-engineering/scaffolding
output, compile-checked generated code, provider parity, and safety modes.

## 1.0 exit criteria

- [ ] Scaffolded output **compiles** for every supported shape (in-proc Roslyn compile checks),
      on every provider, with and without relationships.
- [ ] Live scaffold parity: SQLite/SQL Server/PostgreSQL/MySQL produce equivalent, correct
      models from equivalent schemas.
- [ ] Safety modes are correct and documented: `OverwriteFiles`, `DryRun`, `FailOnWarnings`,
      database-drop confirmation, keyless-entity handling.
- [ ] The bounded v1 scaffolding contract (`docs/scaffolding.md`) matches behaviour; every
      `ScaffoldOptions` member is documented and tested.

## Current confidence

Strong. The CLI compile-check refactor moved 84 compile checks in-proc (live suite ~2h → ~11.5min);
scaffold options (filters, nullable RT, pluralizer, database names, no-relationships, context
placement, output safety, routine/sequence/view/query-artifact opt-ins) are each tested and
recorded in the public-API additions policy.

## Open items

- [ ] Keep the in-proc compile-check suite green on all providers as options evolve.
- [ ] Confirm CLI command surface (`dotnet norm ...`) is stable and documented before API freeze.
- [ ] Verify database-drop safety on live providers.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~Cli|FullyQualifiedName~Scaffold"`
- `dotnet test tests/ --filter "FullyQualifiedName~LiveProviderScaffold"` (with creds).
- `docs/scaffolding.md`, `src/dotnet-norm/README.md`.

## Risks

The machine's C: drive can run low during gates (SQLITE_FULL / pack failures) — surface to the
user, do not delete non-artifact temp. Delete `benchmarks/bin` before gates.
