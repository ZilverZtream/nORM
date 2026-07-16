# Domain 10 — Scaffolding & CLI tooling

**Scope:** the `dotnet norm` CLI (packaged as `dotnet-norm`), reverse-engineering/scaffolding
output, compile-checked generated code, provider parity, and safety modes.

## 1.0 exit criteria

- [x] Scaffolded output **compiles** for every supported shape (in-proc Roslyn compile checks),
      with and without relationships (NH-1001: 798 tests green).
- [ ] Live scaffold parity: SQLite/SQL Server/PostgreSQL/MySQL produce equivalent, correct
      models from equivalent schemas. (NH-1001: in-proc compile checks green; live-provider schema
      parity deferred to the live provider gate.)
- [x] Safety modes are correct and documented: `OverwriteFiles`, `DryRun`, `FailOnWarnings`,
      database-drop confirmation, keyless-entity handling (NH-1001: output-safety + keyless-safety
      tests green).
- [x] The bounded v1 scaffolding contract (`docs/scaffolding.md`) matches behaviour; every
      `ScaffoldOptions` member is documented and tested (NH-1001: `ScaffoldingContractDoc` green;
      members recorded in `docs/public-api-policy.md`).

## Current confidence

Strong. The CLI compile-check refactor moved 84 compile checks in-proc (live suite ~2h → ~11.5min);
scaffold options (filters, nullable RT, pluralizer, database names, no-relationships, context
placement, output safety, routine/sequence/view/query-artifact opt-ins) are each tested and
recorded in the public-API additions policy.

## Open items

- [x] The in-proc compile-check suite is green (NH-1001, 798 tests); the suite keeps it green as
      options evolve.
- [x] Confirm CLI command surface (`dotnet norm ...`) is stable and documented before API freeze.
      (Closed 2026-07-16: `CliCommandSurfaceContractTests` pins the reviewed verb list
      (scaffold, dbcontext scaffold, database update/drop, portability certify, migrations add)
      against the CLI sources — a new or renamed verb breaks the test — and cross-checks every
      invocation against its doc. Gap fixed en route: `norm database update` and
      `norm database drop` were undocumented; both now have sections in
      `docs/design-time-migrations.md` including the advisory-lock exactly-once deploy contract
      and the --yes/--dry-run/protected-name drop gates.)
- [x] Verify database-drop safety on live providers. (Closed 2026-07-16:
      `LiveProviderDatabaseDropSafetyTests` 10/10 non-vacuous on live SQL Server/PostgreSQL/
      MySQL + SQLite — protected database names refuse the drop on every server, system-schema
      rows are correctly identified by the schema reader, and the drop gate requires an explicit
      yes flag (dry-run passes the gate without dropping; no flag is refused).)

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~Cli|FullyQualifiedName~Scaffold"`
- `dotnet test tests/ --filter "FullyQualifiedName~LiveProviderScaffold"` (with creds).
- `docs/scaffolding.md`, `src/dotnet-norm/README.md`.

## Risks

The machine's C: drive can run low during gates (SQLITE_FULL / pack failures) — surface to the
user, do not delete non-artifact temp. Delete `benchmarks/bin` before gates.
