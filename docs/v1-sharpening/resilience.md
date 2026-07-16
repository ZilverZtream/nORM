# Domain 9 â€” Resilience & concurrency

**Scope:** transient-failure retry, retry-write invariants, and optimistic concurrency control
(`[Timestamp]`/rowversion) across batched, direct, and bulk writes.

## 1.0 exit criteria

- [ ] The OCC interleaving fuzzer runs **dry for a sustained window** with zero new kills; no
      lost updates under concurrent writers. (NH-0901: dry on the current tree - 70 tests, no lost
      updates; the sustained multi-week window is open.)
- [ ] Client-managed concurrency token (8-byte) is correct across batched/direct/bulk on every
      provider that lacks native rowversion. (NH-0901: correct across write shapes on SQLite + OCC
      conflict matrix; live PostgreSQL / MySQL matrix deferred to the live provider gate.)
- [x] Retry-write invariants hold under fault injection: db-generated keys rolled back on a
      retried `SaveChanges` are reset; a single write is **never** retried past commit-attempted
      (NH-0901; `SaveChangesFaultInjectionAtomicity` green, cross-ref NH-0201).
- [ ] Retry policy (backoff + jitter) is bounded and documented; deadlock-resilient SaveChanges
      behaves correctly on SQL Server (error 1205). (NH-0901: retry policy documented
      (`docs/retry-policy.md`); live SQL Server deadlock path deferred to the live provider gate.)

## Current confidence

Strong. `[Timestamp]` OCC was silently losing updates; nORM now client-manages the token across
all write shapes on all providers â€” fully closed. The OCC oracle itself was corrected to treat
value-unchanged mutations as non-writes (a version oracle must not count no-ops as conflicts).

## Open items

- [~] Sustain the OCC fuzzer dry window; record interleaving seed ranges. (NH-0901 recorded a
      70-test dry run; env-directed sweeps now feed `docs/v1-sharpening/fuzzer-dry-log.md` via
      `NORM_OCC_FUZZ_SWEEP` and `NORM_RETRY_FUZZ_SWEEP` ("start:count") — seeds 1102000+ and
      1202000+ swept dry 2026-07-16; the sustained window accumulates in the log.)
- [x] Explicit fault-injection tests for the retry-write invariants exist and pass (NH-0901,
      `SaveChangesFaultInjectionAtomicity`).
- [x] Verify deadlock-resilient path on live SQL Server: the 2026-07-16 Category=LiveProvider run (1867/1867 green on all three servers) included the live deadlock tests.
- [x] Read-path retry interplay (NH-0902): read retries were DEAD CODE - NormException wrapping
      defeated the retry strategy filter and the fast path bypassed the strategy entirely; both
      fixed and the retry-x-cache, idempotence, fail-loud, and timeout-wiring contracts pinned
      (`ResilienceRetryCacheContractTests`, `ResilienceFailLoudAndTimeoutContractTests`).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~OccInterleavingFuzzTests"`
- `dotnet test tests/ --filter "FullyQualifiedName~Retry|FullyQualifiedName~Concurrency"`
- `docs/optimistic-concurrency.md`, `docs/retry-policy.md`.

## Risks

A version/OCC oracle must treat value-unchanged mutations as non-writes, or it manufactures false
conflicts. Retrying past commit-attempted is silent data loss â€” the invariant is absolute.
