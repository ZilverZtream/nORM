# Domain 5 — Provider mobility

**Scope:** the differentiator. The same application code must run, unchanged, on SQLite, SQL
Server, PostgreSQL, and MySQL — supported shapes translate/emulate/fail **deterministically and
identically**. Includes Strict Provider Mobility Mode and the translation contract.

## 1.0 exit criteria

- [ ] Live cross-provider parity tests are green on **all four** real providers, not just SQLite.
- [ ] Every provider-specific SQL construct in `src/nORM/Query/` goes through a `DatabaseProvider`
      virtual hook — no unconditional provider syntax in shared query code.
- [ ] `UseStrictProviderMobility()` blocks exactly the non-portable escape hatches (raw SQL,
      stored procs, direct connection/command/transaction access, provider-native DDL,
      interceptors, client-eval) and admits every portable nORM-translated feature.
- [ ] The Provider Mobility Contract (`docs/provider-mobility-contract.md`) matches enforced
      behaviour; `dotnet norm portability certify` output is accurate.
- [ ] Provider-native concerns (integer division, NULL ordering, AVG(int) casting, concat null
      semantics, decimal compare, date diff) are handled at every hook site.

## Current confidence

Strong. Live parity closed many families: ordinal string equality on MySQL+SqlServer, MySQL
integer division (DIV hook), Postgres nulls-first ordering, SqlServer AVG(int) CAST, null concat,
decimal exact compare, Convert rounding + date diff. All fixed live on the three servers.

## Open items

- [ ] Sustain live parity across all four providers in CI (needs live creds; keep them test-only).
- [x] Audit `src/nORM/Query/` for unconditional provider syntax (NH-0501): the query layer is
      provider-agnostic - zero `is <Provider>` checks; the last one (constant regex-argument
      inlining) is now the `DatabaseProvider.InlinesConstantRegexArguments` hook. (The mobility
      translator, scaffold provider-kind, and connection factory legitimately dispatch on provider
      type - they are not query-SQL generation.)
- [x] Verify strict-mode admit/deny list against the full feature enum. (Closed 2026-07-16:
      `ProviderMobilityDecisionCompletenessTests` pins exhaustive coverage of
      `ProviderMobilityTranslator.Decide` over every `ProviderMobilityFeature` member, the exact
      10-feature strict-runtime ADMIT set as a reviewed list (a new member or flipped decision
      breaks the test so the change is deliberate), denied-implies-Error / admitted-implies-
      Info-or-Warning severity coherence, Unsupported-never-admitted / Portable-and-Emulated-
      always-admitted, and non-empty reason + suggested fix on every decision.)

## Verification

- Live provider gate: `eng/v1-release-gate.ps1 -Mode rc` (live SQL Server, PostgreSQL, MySQL, SQLite).
- `dotnet test tests/ --filter "FullyQualifiedName~ProviderCapabilityContract|FullyQualifiedName~ProviderMobility"`
- `dotnet test tests/ --filter "FullyQualifiedName~LiveProvider"` (with configured creds).

## Risks

Live-server variance (thresholds, temporal clock granularity). Provider parity is the product;
a single unconditional `if (provider is X)` in shared query code is a mobility defect.
