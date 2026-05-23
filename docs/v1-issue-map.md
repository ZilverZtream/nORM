# v1 Issue Map

Date: 2026-05-23

This map tracks the 40 current blockers in
`docs/v1-blocker-spec.md`. Status values are evidence-oriented:

- `Open`: implementation, API decision, or release evidence is missing.
- `In Progress`: code, docs, or tests exist, but the v1 acceptance gate is not
  fully satisfied.
- `Verified`: the blocker has the required code, tests, docs, and release
  evidence.
- `Not Applicable`: the feature or claim was removed from the v1 contract.

## Blocker Map

| ID | Area | Status | Current evidence gap |
| --- | --- | --- | --- |
| 1 | Freeze and reduce public API surface | Open | Public baseline still includes `nORM.Internal.*` types and broad implementation-facing surface. |
| 2 | Deterministic v1 package versioning | Open | Current Release packages are `0.9.0-preview.1`. |
| 3 | Core/provider package architecture | Open | Core package dependency graph is broad and provider split is not finally decided. |
| 4 | Cross-platform package consumer behavior | In Progress | Tests exist, but this audit did not produce clean Windows/Linux package evidence. |
| 5 | Generated API docs validation | In Progress | Docs exist and have contract tests, but v1 regeneration evidence is still required. |
| 6 | README and package metadata claims | In Progress | Wording is improved, but broad production/provider claims still need final evidence review. |
| 7 | LINQ matrix hard contract | In Progress | Coverage map exists, but provider/result-path evidence is incomplete. |
| 8 | Terminal operator parity | In Progress | SQLite coverage exists; every execution path and live provider matrix still needs closure. |
| 9 | Aggregate/null/type parity | In Progress | Many tests exist; final provider matrix evidence remains open. |
| 10 | Include, ThenInclude, lazy loading | In Progress | Constraints are documented; support surface and unsupported paths still need final freeze. |
| 11 | GroupBy, GroupJoin, SelectMany, set ops | In Progress | Matrix rows exist; complex semantic parity is still a v1 risk. |
| 12 | Client evaluation safety | In Progress | Default is documented as `Throw`; final proof across paths remains required. |
| 13 | Compiled query isolation/parity | In Progress | Extensive tests exist; release-gate/live-provider evidence remains required. |
| 14 | Source generator contracts | In Progress | Diagnostics and package tests exist; final package-consumer and mapping-subset evidence remains required. |
| 15 | Raw SQL and stored procedure boundaries | In Progress | Security docs/tests exist; final live-provider and tenant-boundary evidence remains required. |
| 16 | Multi-tenancy security contract | In Progress | Adversarial tests exist; full live-provider security gate remains required. |
| 17 | Cache bounds, keys, invalidation | In Progress | Cache stress tests and docs exist; RC evidence remains required. |
| 18 | Change tracking and snapshots | In Progress | Many regression tests exist; v1 semantics still need final public contract review. |
| 19 | SaveChanges graph/cascade behavior | In Progress | Graph-write tests exist; cross-provider acceptance remains required. |
| 20 | Optimistic concurrency | In Progress | MySQL residual gap is documented; all write paths need final parity evidence. |
| 21 | Transactions/savepoints/ambient/retry | In Progress | Docs and tests exist; combined provider-path evidence remains open. |
| 22 | Cancellation and timeout audit | In Progress | Many cancellation tests exist; every public async API still needs final audit. |
| 23 | Provider dialect/version parity | In Progress | Capabilities docs exist; final live provider evidence remains required. |
| 24 | Bulk operation contracts | In Progress | Bulk tests exist; native/fallback/provider acceptance remains open. |
| 25 | Migration rename/data-loss handling | Open | Rename workflow is still manual and warning-based. |
| 26 | Migration recovery/idempotency/live DDL | Open | Requires fault-injected live-provider migration evidence. |
| 27 | Destructive CLI database drop hardening | Open | Requires stronger provider-aware object handling and live disposable DB tests. |
| 28 | CLI design-time loading/project discovery | In Progress | `AssemblyLoadContext` exists; realistic app/project coverage remains open. |
| 29 | Scaffolding preview vs stable decision | Open | Public APIs and CLI exist, but docs mark scaffolding preview. |
| 30 | Temporal versioning side effects | In Progress | Contract docs exist; final provider/bootstrap/migration evidence remains required. |
| 31 | AOT/trimming boundary | In Progress | Unsupported boundary is documented; publish evidence must stay in release gate. |
| 32 | Interceptor contracts | In Progress | Contract tests/docs exist; full path coverage remains required. |
| 33 | Exception taxonomy | In Progress | nORM exceptions exist; source still has public/provider unsupported paths using BCL exceptions. |
| 34 | Logging and redaction | In Progress | Tests/docs exist; release artifact and CLI paths need final audit. |
| 35 | ConnectionManager HA behavior | In Progress | Tests exist; load/failover/disposal stress remains required. |
| 36 | Retry/adaptive timeout policy | Open | Public APIs exist, but production retry rules need final contract and tests. |
| 37 | Benchmark fairness/dependency parity | Open | Benchmark docs exist; benchmark dependency drift and release artifacts remain open. |
| 38 | Benchmark thresholds | In Progress | Threshold scripts exist; budgets need release-candidate calibration and enforcement. |
| 39 | Test-suite reliability | In Progress | No skipped tests found, but focused contract-test run timed out during this audit. |
| 40 | Real RC gate evidence | Open | No successful full RC manifest was produced during this audit. |

## Closure Rule

A row can move to `Verified` only when the acceptance gate in
`docs/v1-blocker-spec.md` is satisfied and the evidence is linked from this map.
