# nORM 1.0 sharpening framework

This is the **executable definition of when nORM is `1.0.0-rc.1`**. `RELEASE.md` states
the *promise* ("shippable barring a showstopper", cut only per a real bar, never on a
cadence). This directory turns that promise into a **per-domain checklist you can actually
work and verify**.

The rule is simple: **nORM is not `1.0.0-rc.1` until every domain doc here reports GREEN
and the cross-cutting gate below holds.** No exceptions, no "green today after fixing three
data-loss bugs today", no RC cut to hit a date.

## How this works

1. Each **domain** below has a doc (`docs/v1-sharpening/<domain>.md`) with concrete, checkable
   **exit criteria**, its **current confidence**, an **open-items checklist**, and the exact
   **verification commands** that prove it.
2. A domain is **GREEN** only when *all* its exit criteria are met and its verification
   commands pass on a clean tree.
3. `1.0.0-rc.1` is cut only when **all domains are GREEN** *and* the **cross-cutting gate**
   holds. Then `-rc` means "we intend to ship this."

## The cross-cutting gate (must ALL hold, on top of every domain being GREEN)

These are the RELEASE.md bars, made concrete:

1. **Correctness has settled over time, not just today.** Every differential/oracle fuzzer
   (read-path parity, the CRUD state machines, bulk CUD, OCC interleaving, temporal
   reconstruction, migration data-preservation, and any added since) runs **dry for a
   sustained window — on the order of weeks of active development with zero new correctness
   kills.** A fuzzer that found a silent-data-loss bug this week resets that clock.
2. **The public API is frozen and documented.** `PublicApi.Shipped.txt` is intentional; no
   planned breaking changes; every public member has XML docs and at least one test; a real
   getting-started + per-area docs exist. (Enforced continuously by `PublicApiSnapshotTests`,
   `PublicApiClassificationTests`, `NamespacePolicyContractTests`, `DocumentationContractTests`.)
3. **Real-world validation.** A handful of non-synthetic workloads / external users have run
   nORM against real SQLite, SQL Server, PostgreSQL, and MySQL databases without a correctness
   surprise. (Synthetic suites do not satisfy this; it is the one bar automation cannot fake.)
4. **Zero known correctness defects.** No open item in any domain doc is a correctness/data-loss
   bug. DX/ergonomics gaps may remain as documented, deliberate limitations.
5. **The release gate is green on the exact tagged commit.** `eng/v1-release-gate.ps1 -Mode rc`
   passes end to end (build, encoding, AOT-zero, API snapshot, package/CLI smoke, live provider
   gate, full suite ×2, stress, parity, migration/cache/concurrency, benchmarks, thresholds).

If any of these fails, the answer is "not yet" — fix it and re-evaluate; do not cut the RC.

## Domains

| # | Domain | Doc | What it covers |
|---|--------|-----|----------------|
| 1 | Query & LINQ translation | [query-linq.md](query-linq.md) | Expression → SQL, supported shapes, LINQ-parity oracle, deterministic unsupported handling |
| 2 | Write path & change tracking | [write-path.md](write-path.md) | SaveChanges, cascade, relationship fixup, direct vs tracked writes, identity map |
| 3 | Bulk operations | [bulk.md](bulk.md) | Provider-native + fallback bulk insert/update/delete, converter fidelity, tenant/cache semantics |
| 4 | Schema & migrations | [migrations.md](migrations.md) | Provider-correct DDL, data preservation, rename detection, advisory-locked deploys |
| 5 | Provider mobility | [provider-mobility.md](provider-mobility.md) | SQLite/SQL Server/PostgreSQL/MySQL parity, strict mobility mode, translation contract |
| 6 | Temporal & versioning | [temporal.md](temporal.md) | nORM-managed history, `AsOf`, reconstruction fidelity, sub-second precision |
| 7 | Multi-tenancy | [multi-tenancy.md](multi-tenancy.md) | Enforced tenant boundary on every read/write, native RLS, isolation tests |
| 8 | Caching | [caching.md](caching.md) | Bounded policy, correct tag/invalidation, no stale reads across writes |
| 9 | Resilience & concurrency | [resilience.md](resilience.md) | Transient retry, retry-write invariants, optimistic concurrency, no lost updates |
| 10 | Scaffolding & CLI | [scaffolding-cli.md](scaffolding-cli.md) | `dotnet norm` contract, compile-checked output, provider parity, safety modes |
| 11 | AOT, trimming & source generation | [aot-sourcegen.md](aot-sourcegen.md) | Zero AOT diagnostic baseline, RUC/RDC honesty, source-generated materializers/queries |
| 12 | Public API, DI & documentation | [api-dx.md](api-dx.md) | API freeze, hosting integration, EF-Core-parity DX, docs completeness |
| 13 | Performance & benchmarks | [performance.md](performance.md) | Threshold-gated benchmark governance, competitive hot path, no regressions |
| 14 | Security & data protection | [security.md](security.md) | Raw SQL / stored-proc safety, log redaction, tenant isolation, injection resistance |

## Status dashboard

Status reflects each domain's progress toward its own GREEN bar. **A domain can be strong on
correctness and still be YELLOW** because the cross-cutting "dry for weeks / real users" gate is
not met — that is the honest pre-1.0 reality, not a defect.

| Domain | Correctness | Docs | Status | Notes |
|--------|-------------|------|--------|-------|
| Query & LINQ | strong (parity fuzzer) | good | 🟡 | needs sustained fuzzer-dry window |
| Write path | strong (state-machine fuzzer, KILL 1-42) | good | 🟡 | relationship machine green; needs dry-weeks |
| Bulk | strong | good | 🟡 | staging type-fidelity closed; dry-weeks |
| Migrations | strong (preservation fuzzer) | good | 🟡 | dry-weeks |
| Provider mobility | strong (live 4-provider parity) | strong | 🟡 | dry-weeks |
| Temporal | strong (reconstruction fuzzer) | good | 🟡 | dry-weeks |
| Multi-tenancy | strong | good | 🟡 | dry-weeks |
| Caching | strong (multi-table tag fix) | good | 🟡 | dry-weeks |
| Resilience & concurrency | strong (OCC closed) | good | 🟡 | dry-weeks |
| Scaffolding & CLI | strong (in-proc compile checks) | good | 🟡 | dry-weeks |
| AOT / source-gen | zero baseline held | good | 🟡 | dry-weeks |
| API / DI / docs | DI + Set<T> landed | improving | 🟡 | key convention decision open; API not yet frozen |
| Performance | thresholds pass | good | 🟡 | re-baseline near RC |
| Security | strong | good | 🟡 | dry-weeks |

Every domain is currently 🟡 for the same honest reason: the code is in good shape, but the
cross-cutting gate (sustained fuzzer-dry window + real external users + a deliberate API freeze)
has not been met. **That is why nORM is `0.9.0`, not an RC.** Individual domain docs list the
specific work that turns each 🟡 → 🟢.

## Working the framework

- Pick the domain with the most open correctness risk first; correctness gates everything.
- Close items, update the domain doc's checklist and status, and only mark GREEN when the
  verification commands pass on a clean tree.
- Re-run the relevant fuzzer over a fresh seed sweep after each fix; a new kill resets the
  domain's dry-window clock (and, if it is silent-data-loss, the whole cross-cutting clock).
- Do **not** cut `-rc` when the last domain flips green — start the sustained-dry-window and
  real-user validation clock. RCs are earned, not scheduled.

## Superseded documents

The earlier `docs/v1-readiness.md`, `docs/v1-blocker-spec.md`, and `docs/v1-issue-map.md` were
written against the premature rc.1–rc.7 cadence. They remain for history but are **superseded by
this framework and `RELEASE.md`**; do not treat them as the current bar.
