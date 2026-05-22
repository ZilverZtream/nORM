# v1 Issue Map

This is the current v1 blocker map. It tracks the fresh 40-item audit from
2026-05-22 and intentionally does not use a blanket "Done" status for work that
still needs release evidence.

## Status Values

- `Open`: implementation, evidence, or release validation is still missing.
- `In Progress`: code or docs exist, but acceptance evidence is incomplete.
- `Verified`: the current tree has code/docs/tests or release-gate evidence for
  the blocker.
- `Not Applicable`: the blocker was deliberately removed from the v1 contract.

## Execution Order

1. Public claims, package shape, generated docs, and API freeze.
2. CLI, migrations, and destructive-operation safety.
3. LINQ, client-eval, bulk, raw SQL, and transaction semantics.
4. Provider, multi-tenant, temporal, AOT, cache, exception, logging, and
   interceptor contracts.
5. Benchmark evidence, executable performance thresholds, test hygiene, and RC
   release artifacts.

## Blocker Map

| ID | Area | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Remove or prove remaining public marketing claims | Verified | README uses bounded performance and migration language; documentation tests reject unqualified claims. |
| 2 | Fix stale generated API docs | Verified | Removed stale `ConnectionPool` API pages and index entries; documentation tests reject generated docs for missing public types. |
| 3 | Decide whether `ConnectionPool` is removed or restored | Verified | Public docs now direct users to provider-native pooling and `ConnectionManager`; no public `ConnectionPool` examples remain. |
| 4 | Fix v1 issue map and blocker accounting | Verified | This map tracks exactly 40 blockers with evidence-aware statuses. |
| 5 | Make Release build warning-free | Verified | `dotnet build nORM.sln -c Release --nologo` reports 0 warnings on this tree. |
| 6 | Harden public API snapshotting before freeze | In Progress | `tests/PublicApi.Shipped.txt` exists; full supportability classification remains open. |
| 7 | Revisit runtime package dependency architecture | Open | Dependency graph is documented only partially; package split decision remains open. |
| 8 | Clean release artifact and version hygiene | Verified | `NormVersion` is centralized in `Directory.Build.props`; package tests and `eng/v1-release-gate.ps1` clean and validate package outputs. |
| 9 | Run package consumer tests cross-platform | In Progress | Package consumer tests exist; cross-platform CI evidence is still required. |
| 10 | Replace or freeze beta CLI dependency | Verified | `dotnet-norm` now references stable `System.CommandLine` 2.0.8; CLI and package consumer smoke tests passed. |
| 11 | Isolate CLI design-time assembly loading | Verified | CLI migration assembly loading uses `AssemblyLoadContext` plus `AssemblyDependencyResolver`; dependency-backed design-time factory test passed. |
| 12 | Harden destructive database drop behavior | In Progress | `--yes` and `--dry-run` exist; live provider safety contracts remain open. |
| 13 | Make migration rename/data-loss handling first-class | In Progress | Destructive-operation warnings exist; first-class rename workflow remains open. |
| 14 | Prove migration recovery and idempotency across providers | Open | Requires fault-injected live-provider evidence. |
| 15 | Complete migration SQL live parity | Open | Requires generated DDL execution across supported live providers. |
| 16 | Generate LINQ support matrix from tests | Verified | `docs/linq-support-coverage.md` maps non-unsupported matrix rows to test files and documentation contracts enforce coverage entries. |
| 17 | Resolve `Any` and `All` semantics across providers | In Progress | Direct SQLite cardinality tests now exercise `Any`, `Any(predicate)`, and `All`; live provider parity still required. |
| 18 | Stabilize Include and lazy-loading contracts | In Progress | Relationship docs exist; unsupported paths and exception taxonomy need cleanup. |
| 19 | Prove terminal operator parity in every execution path | In Progress | Sync/async SQLite cardinality coverage now includes terminal operators; compiled/live-provider parity remains open. |
| 20 | Decide the v1 default for client evaluation | In Progress | Policy exists; default remains a v1 compatibility decision. |
| 21 | Remove legacy string-based bulk CUD paths | Open | Structural validation exists, but legacy SQL parsing paths remain. |
| 22 | Define bulk update value-expression support | In Progress | Safe extraction exists; documented expression contract needs sharpening. |
| 23 | Replace raw SQL safety heuristics with provider-aware validation | Open | Raw SQL validation still needs provider-aware policy. |
| 24 | Tighten stored procedure security and tenant boundaries | In Progress | Multi-tenancy docs identify bypass paths; examples/helpers remain open. |
| 25 | Finish transaction and sync/async policy hardening | In Progress | Transaction and sync docs exist; live/cancellation/interceptor parity remains open. |
| 26 | Prove `ConnectionManager` failover behavior under load | Open | Needs stress tests for health checks, replica churn, and dispose races. |
| 27 | Treat multi-tenancy as a verified security boundary | In Progress | Adversarial tests and docs exist; full live-provider security gate remains open. |
| 28 | Decide temporal/versioning stability | In Progress | Temporal docs exist; v1 stable/preview decision and live evidence remain open. |
| 29 | Enforce provider version support at startup | In Progress | Capability descriptors exist; startup validation needs to be enforced. |
| 30 | Finish MySQL optimistic concurrency guarantees | In Progress | Provider option and docs exist; live same-value-token verification remains open. |
| 31 | Prove AOT and trimming claims with real publish tests | Open | AOT/trimming docs exist; publish-time tests remain open. |
| 32 | Harden source generator limitations | In Progress | Generator ships and has diagnostics; unsupported mapping diagnostics need completion. |
| 33 | Stress cache and plan memory bounds as release gates | In Progress | Cache policy exists; RC memory stress evidence remains open. |
| 34 | Normalize public exception taxonomy | In Progress | Some unsupported query paths use nORM exceptions; remaining public paths need audit. |
| 35 | Complete logging and redaction coverage | In Progress | Connection-string and SQL redaction exist; artifact/interceptor/benchmark coverage remains open. |
| 36 | Freeze interceptor semantics | In Progress | Interceptor contract docs exist; retry/transaction/mutation verification remains open. |
| 37 | Rebuild benchmark evidence before any performance launch | In Progress | Fair benchmark categories exist; full reproducible release artifacts remain open. |
| 38 | Make benchmark thresholds executable | Open | Governance docs exist; versioned threshold files and gate enforcement remain open. |
| 39 | Reduce test-suite entropy before v1 | In Progress | Build is warning-free; test-result hygiene and coverage-test consolidation remain open. |
| 40 | Run and publish a real RC release gate | Open | Requires `eng/v1-release-gate.ps1 -Mode rc -MinLiveProviders 3` artifacts. |

## Closure Rule

A blocker is not considered `Verified` unless at least one of the following is
true:

- an automated test covers the behavior,
- a public doc defines the support contract,
- a release gate or CI workflow enforces it,
- the item is explicitly marked `Not Applicable` with rationale.

Benchmark-related blockers require code-level benchmark categories, public-claim
constraints, and release artifacts before they can be used for launch claims.
