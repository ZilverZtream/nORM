# Domain 9 — Resilience & concurrency

**Scope:** transient-failure retry, retry-write invariants, and optimistic concurrency control
(`[Timestamp]`/rowversion) across batched, direct, and bulk writes.

## 1.0 exit criteria

- [ ] The OCC interleaving fuzzer runs **dry for a sustained window** with zero new kills; no
      lost updates under concurrent writers.
- [ ] Client-managed concurrency token (8-byte) is correct across batched/direct/bulk on every
      provider that lacks native rowversion.
- [ ] Retry-write invariants hold under fault injection: db-generated keys rolled back on a
      retried `SaveChanges` are reset; a single write is **never** retried past commit-attempted
      (either would be silent data loss / duplicate).
- [ ] Retry policy (backoff + jitter) is bounded and documented; deadlock-resilient SaveChanges
      behaves correctly on SQL Server (error 1205).

## Current confidence

Strong. `[Timestamp]` OCC was silently losing updates; nORM now client-manages the token across
all write shapes on all providers — fully closed. The OCC oracle itself was corrected to treat
value-unchanged mutations as non-writes (a version oracle must not count no-ops as conflicts).

## Open items

- [ ] Sustain the OCC fuzzer dry window; record interleaving seed ranges.
- [ ] Add/confirm explicit fault-injection tests for the retry-write invariants above.
- [ ] Verify deadlock-resilient path on live SQL Server.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~OccInterleavingFuzzTests"`
- `dotnet test tests/ --filter "FullyQualifiedName~Retry|FullyQualifiedName~Concurrency"`
- `docs/optimistic-concurrency.md`, `docs/retry-policy.md`.

## Risks

A version/OCC oracle must treat value-unchanged mutations as non-writes, or it manufactures false
conflicts. Retrying past commit-attempted is silent data loss — the invariant is absolute.
