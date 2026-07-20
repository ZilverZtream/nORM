# World-Class Fuzzing — Design Vision

> **Status:** Active initiative (opened 2026-07-20). This document is the durable, authoritative record of
> the fuzzing overhaul. Part 1 is the project owner's vision, preserved verbatim. Part 2 is the additional
> "beyond the vision" enhancements committed to during design. Part 3 is the phased implementation plan and
> live status. See also the measurement core under `tests/Fuzzing/`.

---

## Part 1 — Owner's vision (verbatim)

### Where the Current Approach Falls Short

**1. Seed count is being used as a proxy for exploration**

The LINQ fuzzer uses System.Random, a fixed recursive grammar, bounded depth, and fixed selection
probabilities. For example, boolean trees stop beyond depth three and choose from eight leaf families. The
standard run then evaluates fixed case counts for every seed.

This is reproducible and valuable, but it provides no feedback that seed 516,200 reached any translator state
that seeds 502,000–516,199 did not. The dry log's 14,200 LINQ seeds and 35,500 bulk seeds are impressive
operational evidence, but they are volume metrics rather than exploration metrics.

*Improvement* — Instrument and retain inputs based on:
- Translator branch and block coverage.
- Expression-node and operator-transition coverage.
- Query-plan feature tuples, such as: set-op + temporal + include; group-join + tenant-filter + nullable-key;
  compiled-query + converter + correlated-subquery.
- Generated SQL AST node coverage.
- Provider capability and fallback-path coverage.
- Materializer shape coverage.
- Change-tracker state transitions.
- Exception outcome classes.

A seed should enter the permanent corpus when it contributes a new coverage edge or semantic feature
combination, not merely because it happened to run.

**2. There is no general-purpose shrinker**

The suite reports a seed, case number, and expression, which is good diagnostics, but a seed still reproduces
the entire generation history. The generated case may contain irrelevant predicates, joins, projections, data
rows, prior writes, or state-machine steps.

A world-class system should turn "seed 602775 fails after a long relationship sequence" into something close
to "Add A, attach B, re-parent B, remove A, save." The current project manually pins kills
effectively—the relationship-machine kill was converted into fixed regression coverage—but automatic
reduction would make diagnosis faster and make the resulting regression substantially clearer.

*Improvement* — Build structural shrinkers for:
- **Expression trees**: replace a binary expression with either operand where type-correct; remove Where,
  Select, OrderBy, Skip, Take, Distinct, or set-operation layers; reduce constants toward 0, 1, -1, empty
  string, null, minimum/maximum, and boundary timestamps; replace nested predicates with true, false, or one
  child; reduce navigation depth and projection members.
- **Schemas**: remove unrelated tables, columns, indexes, converters, relationships, and annotations.
- **Datasets**: delta-debug rows and columns while retaining failure.
- **State-machine histories**: delete chunks of operations; remove redundant saves/context resets; simplify
  entity values and key topology.
- **Fault schedules**: remove injected faults and interleavings that are not required.

Every discovered kill should automatically emit: original seed and generator version; serialized original
case; minimized case; standalone regression-test source or replay fixture; SQL and query plan; provider/server
version. This is probably the single highest-leverage developer-experience improvement.

**3. "Unsupported" can conceal declining coverage**

The harness explicitly considers a clean NormUnsupportedFeatureException a valid outcome. That is appropriate
when the feature is genuinely unsupported: returning a clear exception is safer than returning incorrect data.
However, without an acceptance budget, generator evolution or a regression could cause nORM to reject a growing
percentage of generated cases while the fuzzer remains green.

*Improvement* — Classify every case as: Correctly executed; Correctly rejected by a documented support rule;
Unexpectedly rejected; Unexpected exception; Timeout/hang; Wrong result; Non-deterministic result. Then gate:
supported-case execution rate by generator family; unsupported rate against a checked-in baseline; every
rejection against a stable reason code—not exception-message text; coverage of both the supported and
explicitly unsupported contracts; "previously accepted case became unsupported" as a regression unless
approved. A green run should mean "correct and still capable," not only "never silently wrong."

**4. The primary dataset is still small and mostly static**

The central parity dataset has 40 deterministic rows. This is a strong edge-case fixture, but query shape and
data distribution interact. Some defects need: zero rows, one row, or exactly two rows; large duplicate groups;
all-null or no-null columns; extreme skew and hot keys; composite and nullable keys; decimal precision
boundaries; floating-point NaN, infinities, signed zero, and subnormals where supported; Unicode normalization,
combining marks, surrogate boundaries, RTL text, embedded NULs, and long strings; values around timestamp
precision and time-zone transitions; larger cardinalities that alter query plans or force buffering.

*Improvement* — Generate schema + data + query together, while retaining a curated edge-value dictionary. Do
not replace the current fixture; combine it with: boundary-biased generators; distribution generators
(uniform, Zipfian/skewed, all-equal, all-distinct, mostly-null); cardinality classes (0, 1, 2, small,
moderate); pairwise and higher-order boundary combinations; provider-aware representable-value generators. The
oracle must normalize only documented provider differences. It must not normalize away actual semantic defects.

**5. Live-provider fuzzing is broad but comparatively shallow**

The live harness admirably reuses generated shapes against MySQL, PostgreSQL, and SQL Server. But the live run
uses two fixed seeds and reduced case counts for the exposed families. That is a solid smoke/parity gate, not
yet exhaustive continuous provider fuzzing.

*Improvement* — Operate two tiers:
- **Per-PR tier**: SQLite plus a small, coverage-selected live corpus; every historical kill; cases touching
  changed translator/provider code.
- **Continuous/nightly fleet**: sharded fuzz jobs across all providers; fresh randomized seeds and corpus
  mutations; multiple supported server versions and configuration variants; locale, collation, time-zone, SQL
  mode, and isolation-level matrices; long-lived and fresh database modes; periodic container/server restarts
  and transient network faults.

For world-class provider confidence, test not merely "PostgreSQL," but relevant combinations such as server
version, collation, time zone, extension availability, and connection settings.

**6. The oracle portfolio should be diversified**

LINQ-to-Objects is a strong semantic oracle for many query operations, but it is not authoritative for every
relational behavior: null and collation rules can differ; ordering is undefined without an explicit complete
key; decimal and floating-point behavior differs among providers; transactions, isolation, concurrency,
computed columns, defaults, triggers, and temporal features have no direct LINQ-to-Objects equivalent. The
suite already works around one such limitation by excluding string ordering because provider collation and
current-culture comparison differ. That is honest, but world-class fuzzing should turn many exclusions into
explicit relational oracles.

*Improvement* — Use several independent oracle classes:
- LINQ-to-Objects for well-defined CLR semantics.
- Metamorphic relations, for example: `q.Where(true) == q`; `q.Where(p).Where(q) == q.Where(x => p(x) &&
  q(x))`; `Count(predicate) == Where(predicate).Count()`; `OrderBy(k).ThenBy(uniqueKey).Skip(n).Take(m)` equals
  the corresponding materialized slice; insert then read equals intended state; transaction rollback restores
  the prior database snapshot; compiled and non-compiled execution agree; tracking and no-tracking agree on
  values; cached and uncached execution agree absent intervening writes.
- Cross-plan differential execution: fast path versus general translator; batched versus fallback bulk path;
  split versus single query; source-generated versus reflection materializer.
- Cross-provider comparison after applying only explicit mobility rules.
- Cross-ORM comparison, optionally against EF Core or hand-written SQL for selected portable subsets.
- Database-native reference SQL generated independently from the nORM translator.

Multiple independent oracles reduce the risk that the generator and oracle share the same mistaken assumption.

### A World-Class Architecture

**A. Create a typed, serializable fuzz IR** describing: model/schema; rows; query or command; provider
capabilities; context options; execution mode; operation history; fault/concurrency schedule; expected
semantic contract. Benefits: the same case can target SQLite, live providers, compiled queries, caches, and
fast paths; cases can be serialized into a corpus; structural mutation and shrinking become straightforward;
generator-version changes no longer make old seeds unreplayable; failure artifacts are self-contained. A seed
alone is not a durable artifact: changing one `rng.Next()` call can change every subsequent choice. Serialized
cases should be the durable reproduction contract; seeds should be metadata.

**B. Add structure-aware mutation** — start with valid corpus cases and mutate them: insert or remove query
operators; swap operator order; add a correlated subquery or navigation; make a property nullable; add a
converter; replace a scalar key with a composite key; add temporal or tenant scope; move a filter between
source and join; toggle compiled/tracked/cached/split execution; change constants to boundary values;
duplicate or delete rows; add a save, rollback, retry, or context reset. Validity-preserving mutations will
reach deeper states than regenerating most cases from scratch.

**C. Add coverage-guided scheduling** — collect lightweight coverage or custom semantic counters; reward cases
producing new translator/plan/provider states; mutate high-value cases more frequently; periodically minimize
the corpus without losing coverage; maintain separate corpora per domain and a cross-feature corpus.
Compiler-style fuzzing techniques are particularly applicable because nORM translates one structured
language—expression trees—into another—SQL.

**D. Treat hangs and resource blowups as first-class bugs** — add strict per-case limits for: translation
time; execution time; SQL length; expression depth and node count; result cardinality; allocations and peak
memory; recursive graph traversal; retry count; connection and command lifetime. Track complexity
relationships (doubling expression-tree size should not produce 10× or 100× translation time unless explicitly
understood). This catches algorithmic denial-of-service and pathological query-generation behavior.

**E. Build deterministic concurrency exploration** — random sleeps are not enough for OCC, transactions,
retry, and caching. Introduce scheduler-controlled yield points around: read-before-write; command
creation/execution; transaction begin/commit/rollback; concurrency-token validation; cache
lookup/store/invalidate; change detection and state transitions; retry boundaries. Then systematically
enumerate or coverage-guide short interleavings. Combine with state-machine shrinking. A minimal deterministic
schedule is vastly more useful than "failed once under stress."

**F. Verify the fuzzer itself with mutation testing** — deliberately inject product defects such as: omit
tenant predicates from one join path; reverse null inequality; drop an update column; reuse a stale compiled
parameter; skip cache invalidation; shift a temporal boundary; disable an OCC check; corrupt a materializer
ordinal; ignore rollback or retry idempotency. Measure whether the appropriate fuzz family kills each mutant
and how quickly. The meaningful KPI is not only "zero product kills" but also: what percentage of
representative seeded defects does the fuzzing system detect within its PR and nightly budgets?

### Recommended Implementation Order

- **Phase 1 — Make current fuzzing measurable**: add a common `FuzzCaseResult` with outcome taxonomy; record
  attempted/executed/unsupported/timed-out/failed counts per family; add semantic feature counters and
  unsupported-rate baselines; serialize every failure case alongside its seed; publish machine-readable run
  manifests from CI; version generators and record repository commit, runtime, provider, and server versions.
- **Phase 2 — Add shrinking and durable regression artifacts**: expression-tree shrinkers; row/schema delta
  debugging; operation-sequence shrinking for state machines; emit minimized replay fixtures automatically;
  add a checked-in corpus directory with metadata and deduplication.
- **Phase 3 — Introduce the common typed IR**: extract the existing LINQ grammar into a typed query AST;
  represent model/data/context configuration alongside the query; compile the IR independently into
  LINQ-to-Objects, nORM expression trees, and optional reference SQL; migrate one family at a time.
- **Phase 4 — Coverage-guided continuous fuzzing**: add branch or semantic coverage feedback; retain
  coverage-increasing cases; add structure-aware corpus mutations; shard persistent workers by feature and
  provider; deduplicate failures by minimized form and stack/plan signature; run short change-aware sessions
  on PRs and long sessions nightly.
- **Phase 5 — Oracle diversification and deterministic concurrency**: add metamorphic query properties;
  fast-path/general-path and compiled/non-compiled comparisons; systematic concurrency scheduling;
  cross-provider and selected cross-ORM oracles; mutation testing of the product and fuzz harness.

---

## Part 2 — Beyond the vision (additional enhancements committed to)

1. **The IR is a bisection oracle, not just a corpus format.** Tag every retained case with the
   `(commit, coverage-edge)` that first minted it. When nightly coverage *shrinks*, the harness auto-`git
   bisect`s the corpus and names the commit that dropped the edge — coverage regressions become attributed,
   not merely detected.
2. **Code-derived support contract.** Roslyn-enumerate every `NormUnsupportedFeatureException` throw-site and
   require each to carry a stable reason code. *Unexpectedly-rejected* = a throw with no registered code;
   *contract drift* = a code that appears/disappears. The taxonomy self-maintains and cannot silently grow.
3. **Two-sided shrinking (delta-to-nearest-pass).** Minimize the *diff* between the failing case and the
   nearest passing corpus case, not just the failing case. The emitted regression reads "these two
   near-identical queries diverge only in X" — the clearest possible diagnosis, and a free metamorphic pair.
4. **Oracle voting with automatic oracle-blame.** Oracles vote with confidence; on disagreement the harness
   ranks which oracle is probably wrong (LINQ-to-Objects on undefined ordering = low confidence), so we do not
   chase oracle bugs as product bugs.
5. **Coverage-guided schedules, not just inputs.** Apply new-edge reward to *interleavings* — the deterministic
   scheduler's yield-point permutations enter the corpus when they hit a new change-tracker / transaction
   state-transition edge (AFL-for-schedules).
6. **A snapshot-diff VM as the write oracle.** For CRUD/OCC/transaction cases (no LINQ-to-Objects equivalent),
   an in-memory relational reference model applies the same op history; the invariant is "committed DB == VM
   state; rolled-back == prior snapshot." Turns the entire write path into a metamorphic oracle.
7. **Mutation-kill-rate as a standing gate KPI.** A rotating set of Roslyn-seeded product mutants must die
   within the PR budget; "% killed within budget" is a tracked release metric — the fuzzer's teeth are
   measured every run, not assumed.

---

## Part 3 — Phased status

- **Phase 1 (measurable):** measurement core landed — `FuzzOutcome` taxonomy, `FuzzCaseResult` durable
  artifact, `FuzzRunManifest` (per-family tallies, unsupported rate, feature frontier, failure dedup, JSON),
  and `FuzzSupportContract` gate (allowed reason codes, unsupported ceilings, required-feature frontier). Gate
  fails on silent defect / undocumented rejection / unsupported-rate breach / frontier shrink. Remaining:
  retrofit the live LINQ + CRUD fuzzers to emit `FuzzCaseResult`s and publish the manifest from CI; add reason
  codes to `NormUnsupportedFeatureException` throw-sites (enables the code-derived contract).
- **Phase 2 (shrinking):** greedy tree minimizer + Zeller ddmin sequence reducer + failure-signature-preserving
  shrink landed (`tests/Fuzzing/Shrinker.cs`). Remaining: expression-tree and schema reductions over the IR;
  auto-emit minimized replay fixtures; checked-in corpus directory.
- **Phases 3–5:** not yet started (typed IR → coverage-guided continuous → oracle diversification + deterministic
  concurrency + mutation testing).

**Sequencing decision (owner, 2026-07-20):** land the in-flight data-loss fixes first, then build Phase 1, and
fold the remaining data-loss findings in as the harness's first real corpus with auto-minimized regressions.
