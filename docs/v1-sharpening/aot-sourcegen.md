# Domain 11 — AOT, trimming & source generation

**Scope:** the AOT/trimming diagnostic baseline, honest `RequiresUnreferencedCode`/
`RequiresDynamicCode` annotations, and the source generator (compile-time materializers and
queries).

## 1.0 exit criteria

- [ ] The AOT publish diagnostic baseline (`eng/aot-baseline.txt`) stays **empty**; the release
      gate fails on any new IL diagnostic not in the baseline.
- [ ] Every reflection/dynamic-code site that is genuinely not AOT-safe is annotated with
      `RequiresUnreferencedCode`/`RequiresDynamicCode` (nested classes do **not** inherit these —
      annotate each). No unannotated dynamic-code leak.
- [ ] The source generator produces correct materializers/queries; the source-gen correctness
      fuzzer agrees with the reflection runtime.
- [ ] Documented deployment boundary (`docs/aot-trimming.md`) matches reality: JIT-first with
      source-generation support and explicit AOT limits.

## Current confidence

Strong. The AOT baseline has been empty since 2026-07-11 and is gate-enforced. The DI additions
(`AddNorm` connection-string overload) were annotated and verified to add **zero** new IL
diagnostics. Source-gen materializer correctness is fuzzed.

## Open items

- [ ] Keep the AOT diagnostic scan at zero after every public-API/reflection change (run
      `dotnet publish src/nORM.csproj -c Release -r linux-x64 --self-contained -p:PublishAot=true`
      and grep `IL\d{4}`).
- [ ] Sustain the source-gen correctness fuzzer dry window.
- [ ] Confirm `docs/aot-trimming.md` and README AOT claims match after DI/Set<T>.

## Verification

- AOT scan (above); the release gate's AOT-warning phase.
- `dotnet test tests/ --filter "FullyQualifiedName~SourceGen|FullyQualifiedName~SourceGenerator"`
- `dotnet test tests/ --filter "FullyQualifiedName~PackageConsumerIntegrationTests"` (generator in the packed nupkg).

## Risks

Nested classes don't inherit RDC/RUC. The `GeneratePackageOnBuild is not supported for native
compilation` error at the end of an AOT publish is expected/ignored — the IL analysis runs first.
