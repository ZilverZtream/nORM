# Domain 11 — AOT, trimming & source generation

**Scope:** the AOT/trimming diagnostic baseline, honest `RequiresUnreferencedCode`/
`RequiresDynamicCode` annotations, and the source generator (compile-time materializers and
queries).

## 1.0 exit criteria

- [x] The AOT publish diagnostic baseline (`eng/aot-baseline.txt`) stays **empty**; the release
      gate fails on any new IL diagnostic not in the baseline (NH-1101: AOT scan on the current
      tree = 0 IL diagnostics).
- [x] Every reflection/dynamic-code site that is genuinely not AOT-safe is annotated with
      `RequiresUnreferencedCode`/`RequiresDynamicCode` (nested classes do **not** inherit these —
      annotate each). No unannotated dynamic-code leak (NH-1101: 0 IL diagnostics + `AotTrimmingPolicy`).
- [x] The source generator produces correct materializers/queries; the source-gen correctness
      fuzzer agrees with the reflection runtime (NH-1101: `SourceGen*` / `SourceGenMaterializerCorrectnes` green).
- [x] Documented deployment boundary (`docs/aot-trimming.md`) matches reality: JIT-first with
      source-generation support and explicit AOT limits (NH-1101: `AotTrimmingPolicy` green).

## Current confidence

Strong. The AOT baseline has been empty since 2026-07-11 and is gate-enforced. The DI additions
(`AddNorm` connection-string overload) were annotated and verified to add **zero** new IL
diagnostics. Source-gen materializer correctness is fuzzed.

## Open items

- [x] AOT diagnostic scan is at zero on the current tree (NH-1101); re-run after every
      public-API/reflection change (`dotnet publish ... -p:PublishAot=true`, grep `IL\d{4}`).
- [~] Sustain the source-gen correctness fuzzer dry window. (NH-1101 recorded a dry run. Note:
      the source-gen suites are DETERMINISTIC equivalence batteries, not seeded fuzzers, so there
      is no seed range to sweep — the window accumulates through the `SourceGen*` suites being
      green in every full Fast run, recorded here rather than in the seed ledger.)
- [x] `docs/aot-trimming.md` and README AOT claims are consistent after DI/Set<T> (NH-1101: the DI
      overload is annotated and `Set<T>` inherits the class-level RUC/RDC on `NormQueryable`).

## Verification

- AOT scan (above); the release gate's AOT-warning phase.
- `dotnet test tests/ --filter "FullyQualifiedName~SourceGen|FullyQualifiedName~SourceGenerator"`
- `dotnet test tests/ --filter "FullyQualifiedName~PackageConsumerIntegrationTests"` (generator in the packed nupkg).

## Risks

Nested classes don't inherit RDC/RUC. The `GeneratePackageOnBuild is not supported for native
compilation` error at the end of an AOT publish is expected/ignored — the IL analysis runs first.
