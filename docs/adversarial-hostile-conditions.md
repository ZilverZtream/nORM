# Adversarial Hostile-Conditions Record

This is the reviewer-readable record for A+ dimension **C**: a deliberate adversarial
hostile-condition sweep of each subsystem, *beyond* the standing stress and parity suites.
Where the standing suites prove nORM is correct under ordinary use, the sweeps below prove
it stays correct — or fails **loud**, never silently wrong — under the inputs and schedules an
attacker or an unlucky edge case would produce.

Each subsystem lists the hostile conditions exercised, the test files that prove them, and a
one-line reproduce command. The companion security-side record is
[security-threat-model.md](security-threat-model.md) (dimension D); the correctness-fuzzing
method and its dry-run log are in [v1-sharpening/fuzzer-dry-log.md](v1-sharpening/fuzzer-dry-log.md).

> Guiding invariant: **no silent data loss and no silent wrong result.** Every sweep asserts
> either an exact round-trip / correct result, or a deterministic loud failure — never a quiet
> corruption.

## C1 — Text & value fidelity (write → read → predicate)

Hostile conditions: embedded NUL (C-string truncation), all-NUL / leading / trailing NUL,
zero-width joiner/non-joiner, RTL-override BiDi controls, mid-string BOM, decomposed graphemes
(NFC-normalization), astral-plane surrogate pairs, 100k-char strings, 1 MB blobs; decimal
17th-significant-digit boundaries and scale variants, `float`/`double` NaN / ±Infinity /
subnormal / signed-zero / Max / Min, Guid canonical form, nullable NULL semantics.

- Proving tests: `TextEncodingAdversarialFidelityTests` (SQLite: insert, tracked update, WHERE
  bind), `TextEncodingAdversarialCrossProviderLiveTests` (MySQL utf8mb4 / PostgreSQL /
  SQL Server NVARCHAR: insert, tracked update, and an embedded-NUL round-trip-or-throw
  invariant), `TextAndBlobRoundTripContractTests`, `BulkDecimalFidelityContractTests`,
  `BulkFloatingPointFidelityContractTests`, `BulkIntegerFidelityContractTests`,
  `BulkGuidFidelityContractTests`, `BulkNullableFidelityContractTests`,
  `BulkTemporalFidelityContractTests`, `DecimalStorageFormatFuzzTests`,
  `ValueFidelityRoundTripProbeTests`, `NullableValueFidelityProbeTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~Fidelity|FullyQualifiedName~TextEncodingAdversarial"`

## C2 — Query translation

Hostile conditions: generated LINQ shapes differentially checked against a LINQ-to-Objects
oracle; pathological join depth / WHERE-condition / parameter counts against the admission
limits; deeply nested and mixed operator shapes; adversarial string/collation/ordinal edges.

- Proving tests: `LinqParityFuzzTests` + `LinqParityFuzzLiveTests` (differential vs
  LINQ-to-Objects across the shape battery), `TranslationFuzzTests`,
  `AdversarialQueryComplexityTests` (admission-limit boundaries), `AdversarialMultiShapeStressTests`,
  `LinqQueryStressTests`, `CorrelatedSubqueryConverterFuzzTests`, and the coverage-guided
  differentials under `tests/Fuzzing/` (QueryIr / Join / set-op / GroupBy / HAVING).
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~LinqParityFuzz|FullyQualifiedName~TranslationFuzz|FullyQualifiedName~AdversarialQueryComplexity"`

## C3 — Write path & change tracking

Hostile conditions: randomized insert/update/delete/save op histories checked against an
in-memory snapshot-diff oracle over the authoritative raw-SQL DB state; collection
add/remove/re-parent churn; many-to-many link/unlink edit sequences; owned-collection replace
semantics; modify-after-insert and repeated-edit-under-transaction sequences.

- Proving tests: `CrudStateMachineFuzzTests` + `CrudStateMachineFuzzLiveTests`,
  `ChangeTrackerStressTests`, `CollectionRemovalReparentFuzzTests`, `ManyToManyWriteFuzzTests`,
  and the write-scenario differentials under `tests/Fuzzing/` (`WriteScenario*`).
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~CrudStateMachineFuzz|FullyQualifiedName~ChangeTrackerStress|FullyQualifiedName~ManyToManyWriteFuzz"`

## C4 — Concurrency & optimistic concurrency (OCC)

Hostile conditions: interleaved multi-writer OCC histories that must throw
`DbConcurrencyException` exactly when a token is stale (a missed throw is a silent lost update);
mixed nullable/non-null token races; parallel execution and compiled-query contention;
lock-step multi-writer stress on live servers.

- Proving tests: `OccInterleavingFuzzTests`, `MixedTokenOccFuzzTests`,
  `ConcurrencyExecutionStressTests`, `CompiledQueryConcurrencyStressTests`,
  `AdversarialConcurrencyTests`, `LockStepProviderParityStressTests`, and the OCC differentials
  under `tests/Fuzzing/` (`OccScenario*`).
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~Occ|FullyQualifiedName~ConcurrencyExecutionStress"`

## C5 — Bulk operations

Hostile conditions: bulk staging under concurrent load; randomized bulk CRUD checked against an
oracle; type-fidelity of every CLR type through the bulk transfer path (including NaN-fails-loud
and sub-second temporal precision).

- Proving tests: `BulkConcurrencyStressTests`, `BulkCudOracleFuzzTests`, the `Bulk*Fidelity*`
  suite, `BulkCudContainsRebindLiveTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~BulkCudOracleFuzz|FullyQualifiedName~BulkConcurrencyStress|FullyQualifiedName~BulkFidelity"`

## C6 — Migration

Hostile conditions: stress over migration sequences; partial-failure recovery; live DDL parity
and destructive-operation guards; temporal history DDL under migration.

- Proving tests: `MigrationStressTests`, `TemporalMigrationContractTests`,
  `TemporalMigrationLiveBehaviourTests`, `LiveProviderMigrationDdlParityTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~MigrationStress|FullyQualifiedName~TemporalMigration"`

## C7 — Temporal (AsOf reconstruction)

Hostile conditions: reconstruct every historical version at every checkpoint with deliberate
clock-gap delays that stress trigger-written window precision (single-table and relational
graphs); temporal type round-trips; temporal reads under transactions and tenant scope.

- Proving tests: `LiveProviderTemporalReconstructionFuzzTests`,
  `TemporalTypeRoundTripContractTests`, `TemporalUnderTransactionTests`,
  `TenantTemporalAsOfIsolationContractTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~TemporalReconstructionFuzz|FullyQualifiedName~TemporalTypeRoundTrip"`

## C8 — Multi-tenancy (isolation boundary)

Hostile conditions: forged / cross-tenant values in every query shape and navigation path;
relational-fuzz tenant isolation; fail-closed on missing/forged tenant; tenant scoping of bulk,
write, projection, temporal, and cache paths.

- Proving tests: `AdversarialTenantFuzzTests`, `AdversarialTenantExpressionFuzzTests`,
  `AdversarialTenantNavigationShapeTests`, `AdversarialTenantObjectMatrixTests`,
  `AdversarialBulkTenantTests`, `TenantIsolationRelationalFuzzTests`, `TenantIsolationStressTests`,
  `TenantFailClosedTests`, `TenantQueryShapeSweepContractTests`, `TenantResultCacheIsolationContractTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~AdversarialTenant|FullyQualifiedName~TenantIsolation|FullyQualifiedName~TenantFailClosed"`

## C9 — Caching

Hostile conditions: parallel set/get with no corruption; fault injection into the cache path;
staleness fuzzing (a cache must never return a stale or cross-context row); divergent-model
cache isolation; LRU eviction under contention.

- Proving tests: `CacheContentionTests`, `CacheFaultInjectionStressTests`, `CacheStalenessFuzzTests`,
  `ConcurrentLruCacheStressTests`, `DivergentModelCacheStressTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~Cache" `

## C10 — Connection & retry fault injection

Hostile conditions: transient faults injected before and after execution across nonquery /
scalar / reader commands and at arbitrary positions in a write batch; retry must be exactly-once
(no lost, duplicated, or half-applied batch) or fail loud; verified cross-provider.

- Proving tests: `RetryFaultInjectionTests`, `RetryExactlyOnceCrossProviderLiveTests`,
  `CacheFaultInjectionStressTests`.
- Reproduce: `dotnet test tests --filter "FullyQualifiedName~RetryFaultInjection|FullyQualifiedName~RetryExactlyOnce"`

## Coverage-guided fuzzing harness

Beyond the per-subsystem sweeps above, `tests/Fuzzing/` is a coverage-guided, shrinking,
multi-oracle harness with a serializable IR, a code-derived support contract (every
`NormUnsupportedFeatureException` throw-site carries a stable reason code), and durable
regression artifacts. It has found and fixed six real correctness bugs the 15k-test suite
missed. See [v1-sharpening/fuzzer-dry-log.md](v1-sharpening/fuzzer-dry-log.md) and the fuzzing
vision in `docs/fuzzing/`.

## Residual / not in scope

- Deterministic scheduler-controlled concurrency exploration (enumerating short interleavings)
  is future work; the current concurrency sweeps use randomized and lock-step multi-writer
  stress, which has been sufficient to surface the write-path races fixed to date.
- Byte-level driver-protocol fuzzing (malformed wire packets) is the database driver's
  responsibility, not nORM's.

## Reviewer's one-command check

Run the adversarial and fuzz sweeps (SQLite; live-provider cases skip without `NORM_TEST_*`):

```
dotnet test tests --filter "FullyQualifiedName~Adversarial|FullyQualifiedName~Fuzz|FullyQualifiedName~Fidelity|FullyQualifiedName~Stress"
```
