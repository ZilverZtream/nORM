# The 0.9.x Production-Confidence Bar

This document defines when nORM `0.9.x` is **good enough that a company can run it
in production** — judged purely on the **intrinsic quality of the code, tests,
build, and docs**, the things the project actually controls.

It deliberately separates two axes that `RELEASE.md` and
[`v1-sharpening`](v1-sharpening/README.md) blend together:

| Axis | What it is | In our control? | Bar |
| --- | --- | --- | --- |
| **Quality (A+)** | Is the code correct, safe, robust, secure, operable, complete, and honestly documented? | **Yes** — earned by engineering | **This document** |
| **Maturity** | Has the world run it? Sustained fuzzer-dry weeks, external users, a large ecosystem. | **No** — earned by time and adoption | `RELEASE.md` + the `v1-sharpening` cross-cutting gate (the 1.0 concern) |

**A company adopting `0.9.x` is buying quality, not maturity.** They accept that it
is young and pin the version; what they cannot accept is a silent wrong answer, lost
data, a security hole, or a doc that lies. So the `0.9.x` bar is: **A+ on every
quality attribute, with zero known correctness/data-safety/security defects.**
Maturity is explicitly *out of scope here* — it is what turns an A+ `0.9.x` into
`1.0`.

## What "A+" means here

A+ is not a self-awarded grade. It is a claim a company's staff engineer can
**verify from evidence you hand them** — the test suites, the fuzzers, the release
gate, the source — without taking our word for it. Four rules make the grade real:

1. **Fail loud, never silently wrong.** The one inviolable invariant: nORM never
   returns a wrong answer or loses data quietly. An unsupported shape throws a clear,
   typed error; an ambiguous write refuses. This outranks performance, ergonomics,
   and completeness. A single silent-wrong or silent-data-loss defect drops the whole
   product below A+ until fixed — no partial credit.
2. **Verifiable, not asserted.** Every A+ claim below names the evidence that proves
   it and the command that reproduces it. "Trust us" is a C.
3. **Honest surface.** Documented behavior is *true*. Unsupported features are
   unsupported *loudly*. Every limitation is deliberate and disclosed, never a
   surprise a company discovers in production.
4. **Green tests are necessary but not sufficient.** This is the hard-won lesson.
   14,078 offline + 2,494 live tests passed on a tree that still shipped mojibake in
   source files and un-annotated AOT reflection sites — because no *unit test*
   asserts source-file encoding or trim-safety; the release **gate** does. **A+ is
   the set of gates that catch what unit tests structurally cannot** (integrity
   scans, adversarial fuzzers, doc-truth contracts, AOT analysis), not the unit
   count. A dimension is only A+ when its *gate* is green, not merely its tests.

## The rubric

Eight quality attributes. Each is A+ only when its exit criterion holds **and** the
gate that enforces it is green on a clean tree. Status is honest as of this writing;
`gap` means real work remains before the claim is true.

### A. Correctness you can trust
- **A+ criterion:** Behavior matches a trusted oracle across every provider, and the
  apparatus that proves it is comprehensive — not a spot check.
- **Evidence:** differential/oracle fuzzers (LINQ-parity vs LINQ-to-Objects, CRUD
  state machine, bulk CUD, OCC interleaving, temporal reconstruction, migration
  data-preservation); cross-provider parity suites; `Category=LiveProvider` on all
  four RDBMSs. `eng/v1-release-gate.ps1` runs the full suite ×and the live gate.
- **Status: A** — the apparatus is genuinely strong and is the product's real moat.
  It is not yet A+ *by this document's own rule* until the fuzzers run a fresh
  seed sweep with zero new kills on the exact release commit (a per-release gate, not
  a maturity window).

### B. Data safety (never lose or corrupt)
- **A+ criterion:** No tracked, bulk, or migration path can silently lose or corrupt
  a row. Optimistic concurrency never loses an update; retries never double-write;
  migrations preserve data or refuse; ambiguous writes fail loud.
- **Evidence:** OCC lost-update tests, retry-write-invariant tests, migration
  data-preservation fuzzer, composite-key/partial-column UPDATE alignment tests, the
  CRUD state-machine fuzzer's 40+ recorded kills (all closed).
- **Status: A+** — this is the strongest dimension; the failure mode is loud by
  design and heavily fuzzed. Keep it A+ by running the state-machine and preservation
  fuzzers per release.

### C. Robustness / no sharp edges
- **A+ criterion:** Adverse conditions degrade cleanly — connection loss, timeouts,
  cancellation, high concurrency, large payloads, hostile inputs — with no data-race,
  deadlock, or pathological allocation cliff.
- **Evidence:** `Category=…Stress` / `AdversarialConcurrency` suites, adaptive
  timeout, cancellation-honoring `SaveChangesAsync`, bounded caches, the write-path
  allocation work. Live gate exercises real round-trips.
- **Status: A** — broad stress coverage exists; A+ requires one deliberate
  adversarial sweep per subsystem (not just the standing suites) recorded per
  release.

### D. Security
- **A+ criterion:** Injection-resistant by construction (parameterized everywhere,
  raw-SQL boundaries explicit and documented), tenant isolation enforced on *every*
  read and write path, credentials never logged, log redaction on by policy.
- **Evidence:** `docs/raw-sql-security.md`, `docs/stored-procedure-security.md`,
  `docs/multi-tenancy-security.md`, `docs/logging-redaction.md`; tenant-boundary and
  RLS tests; the context-pool tenant-session-key reset (this session).
- **Status: A** — the boundaries are designed and tested. A+ requires a documented
  threat-model pass and an injection-resistance audit that a security reviewer can
  read as a single artifact.

### E. Operability
- **A+ criterion:** An ops team can run it: structured logging + interceptors +
  diagnostics, migration safety (advisory-locked, data-preserving, dry-runnable),
  connection/pool management, and typed, actionable errors.
- **Evidence:** `docs/interceptors.md`, `docs/production-operations.md`,
  `docs/exception-taxonomy.md`, `docs/retry-policy.md`, migration advisory-lock tests,
  `AddNormPool` pooling.
- **Status: A** — the surface exists and is documented; A+ requires verifying the
  exception taxonomy is exhaustive (every failure maps to a documented typed error)
  and that the operations doc matches current behavior after the doc pass.

### F. API completeness & fail-loud honesty
- **A+ criterion:** The surface a company needs is present and coherent (EF-Core
  parity where it counts); every *un*supported shape fails loud with a message that
  says what to do instead — never a silent wrong result or an opaque crash.
- **Evidence:** `PublicApiSnapshotTests`, `PublicApiClassificationTests`,
  `NamespacePolicyContractTests`; the LINQ-parity fuzzer's deterministic
  unsupported-handling; `docs/linq-support.md` coverage matrix.
- **Status: A** — parity is broad and the fail-loud discipline is real. A+ requires
  the frozen surface to be intentional (the API-freeze decision) and a sweep that
  proves no probed `NormUnsupportedFeatureException` masks a shape that *should* work.

### G. Documentation truth
- **A+ criterion:** Every claim in every document is *true and verifiable*; no
  aspirational or stale statement; a company can learn and operate nORM from the docs
  alone. Contract tests pin the load-bearing claims.
- **Evidence:** `DocumentationContractTests` (forbids hyperbole, pins the LINQ matrix),
  `ChangeTrackingContractDocTests`, `ScaffoldingContractDoc*`,
  `MigrationRenameDocContractTests`, `ChangelogVersionContractTests`.
- **Status: gap** — docs accreted during rapid development; contract tests cover only
  a subset. This is the active **"clean and fix every document"** pass: every doc
  audited for accuracy against current behavior, every code sample compiled or
  pinned, every version/number current. **Until that pass completes, this dimension
  is not A+**, and by rule 3 the product is not A+.

### H. Build & supply-chain integrity
- **A+ criterion:** The artifact a company installs is clean and honest: correct
  package metadata + symbols + Source Link, source free of encoding corruption,
  AOT/trim honesty (zero un-annotated reflection against an empty baseline), a
  reproducible end-to-end release gate.
- **Evidence:** `eng/v1-release-gate.ps1` (build, encoding scan, AOT-zero scan,
  API snapshot, package-consumer + CLI smoke, live gate, full suite, benchmarks,
  pack, validate, RC manifest); `PackageConsumerIntegrationTests`.
- **Status: A (was gap this session)** — the gate exists and now passes end to end.
  This session it caught **real** integrity defects a green test tree hid: 11 files
  with mojibake/replacement characters, and 12 un-annotated AOT reflection sites
  against the zero baseline. Both fixed and re-validated. A+ holds only as long as the
  full gate is green on the exact release commit — which is exactly why it is run per
  release, not per date.

## Honest current standing

Weighing the rubric truthfully:

- **The hard dimensions are genuinely strong.** Correctness (A), data safety (A+),
  and robustness (A) — the attributes a company most fears in a young ORM — are where
  nORM is *most* credible. The correctness *method* — differential fuzzing against a
  LINQ-to-Objects oracle plus cross-provider parity checks — is more rigorous than the
  example-based testing most ORMs ship with; that is a verifiable difference in
  engineering, not a marketing claim.
- **The gaps are honest and specific, not vague "needs polish."** Two dimensions are
  not yet A+: **G (documentation truth)** — the doc pass is not done — and the
  per-release *gate discipline* on A/C/D/E/F (the sweeps and audits that turn a strong
  A into a defensible A+). H was a real gap this session and is now closed.
- **The lesson is baked in.** The encoding and AOT defects are the reason this bar
  refuses to equate "tests pass" with A+. A+ is the gates, run on the release commit,
  plus the per-dimension audits below.

**Verdict:** the intrinsic quality is a strong **A** today, with a **credible, short
path to A+** — the remaining work is bounded and named, not open-ended. Nothing here
is blocked on maturity, external users, or time; it is all engineering we control.

## The work to reach A+ (checkable)

- [ ] **G — documentation truth pass.** Audit every `docs/*.md`, `README.md`, and CLI
      README for accuracy against current behavior; compile or pin every code sample;
      make every version/number/claim current; extend contract tests to the
      load-bearing claims that are not yet pinned. *(The active task.)*
- [ ] **A — per-release correctness gate.** Fresh differential/oracle seed sweep on
      the release commit with zero new kills, recorded in the release evidence.
- [ ] **C — adversarial sweep per subsystem.** One deliberate hostile-condition pass
      per subsystem (not just the standing stress suites), recorded.
- [ ] **D — security artifact.** A single threat-model + injection-resistance write-up
      a security reviewer can read end to end.
- [ ] **E — exception-taxonomy completeness.** Prove every failure path maps to a
      documented typed error; reconcile `docs/production-operations.md` with behavior.
- [ ] **F — API intentionality.** The deliberate API-freeze decision, plus a sweep
      proving no `NormUnsupportedFeatureException` hides a shape that should work.
- [ ] **H — keep the gate green** on the exact release commit; treat any new gate
      finding (like this session's) as an A+ blocker, not a nuisance.

When every box is checked and `eng/v1-release-gate.ps1` is green on the release
commit, `0.9.x` is **honestly A+**: a company can adopt it on intrinsic merit and
pin the version, accepting only the maturity risk that no amount of engineering can
retire — only real-world use can.

## Relationship to 1.0

This bar and the 1.0 bar are additive, not competing:

```
A+ intrinsic quality (this document)         → 0.9.x a company can run in production
      +  sustained fuzzer-dry weeks
      +  external real-world validation        → 1.0.0 (RELEASE.md, v1-sharpening)
      +  deliberate API freeze
```

A company that adopts an A+ `0.9.x` is doing exactly what pre-1.0 versioning is for:
using solid software early, pinning the version, and reporting back. **Their usage is
what earns the maturity half — so shipping an honestly-A+ `0.9.x` is how `1.0`
eventually gets earned, not a detour from it.**
