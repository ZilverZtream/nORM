# Domain 1 — Query & LINQ translation

**Scope:** LINQ expression tree → SQL translation (ETSV predicates, SCV projections), the set
of supported query shapes, and deterministic behaviour for unsupported shapes across all four
providers.

## 1.0 exit criteria

- [ ] The LINQ-parity differential fuzzer (seeded shapes vs LINQ-to-Objects oracle) runs **dry
      for a sustained window** (weeks of active dev, zero new correctness kills). (NH-0101: dry on
      the current tree - 351 tests; the sustained multi-week window is the cross-cutting bar, open.)
- [ ] Every supported LINQ shape is enumerated in `docs/linq-support.md` and covered by a
      live cross-provider parity test; the matrix matches the code.
- [ ] Unsupported shapes fail **deterministically and identically** across providers with a
      typed `NormUnsupportedFeatureException` — never a silent wrong result or client-eval
      surprise (unless `ClientEvaluationPolicy` explicitly allows it).
- [ ] Ordinal string semantics verified on every provider (equality, IN, joins, GroupBy,
      Distinct, set-ops, StartsWith/EndsWith/Contains) — no culture/collation drift.
- [ ] Plan cache correctness: closure values never bake into a cached plan; every replay path
      (plan cache + pooled command cache) parameterises correctly.

## Current confidence

Strong. The parity fuzzer has driven kills #42–#60 (correlated First/Last/ElementAt with
predicate+projection, ordered-scalar chains, tail-paging-after-window, GroupJoin segmentation,
converter-through-subquery, decimal-lexical aggregates, negated-null row loss). Ordinal string
equality and case-sensitivity campaigns are closed. Closure/SCV plan-cache baking is fixed.

## Open items

- [~] Sustain the parity-fuzzer dry window; log every seed sweep and its range. (NH-0101 recorded
      a 351-test dry run on the current tree; the multi-week window itself is calendar time.)
- [x] Confirm `docs/linq-support.md` and `docs/linq-support-coverage.md` still match the code
      after the DI/Set<T> additions. (Confirmed 2026-07-16: the doc-contract suite (55 tests,
      matrix<->coverage cross-check) is green; the one gap was that the matrix never stated the
      `Set<T>()` alias applies to every row - an intro note now says both entry points are the
      same queryable.)
- [~] Audit remaining `NormUnsupportedFeatureException` throw sites: each must be reachable,
      tested, and documented (no accidental silent fallbacks). Per feedback, prefer implementing
      over throw-pinning where a shape is portable. (Audited 2026-07-16: 152 sites — 150 genuine
      guards (savepoint types, grouped/aggregated bulk CUD, strict-mobility boundaries,
      override-me provider-hook defaults, culture-sensitive string modes); 2 defensive
      fallbacks reachable only from third-party providers (String.cs IndexOf/Replace
      TranslateFunction-null tails — kept as fail-loud extension-point guards). ONE portable gap
      found and IMPLEMENTED: char-needle `Contains`/`StartsWith`/`EndsWith` (+
      `Contains(char, StringComparison)`) previously translated only on SQLite and threw on all
      three servers; now registered on the shared string handlers, pinned fast + live.
      REMAINING: negative contract tests for the untested guard throws — non-constant
      StringComparison modes (Compare/IndexOf/Replace), SQLite ignore-case Replace,
      DateTime.ParseExact unsupported format.)

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~LinqParityFuzzTests"`
- Live parity: the live provider gate (`eng/v1-release-gate.ps1`) cross-provider parity phase.
- `dotnet test tests/ --filter "FullyQualifiedName~DocumentationContract"` (LINQ support matrix links).

## Risks

Culture/collation on string ORDER BY keys is deliberately excluded from the oracle (culture vs
collation); any future ORDER-BY-string work must not regress the ordinal WHERE/join guarantees.
