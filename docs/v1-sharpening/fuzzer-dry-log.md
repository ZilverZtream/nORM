# Fuzzer dry-window log

Chronological, dated evidence toward the RELEASE.md "fuzzers dry for a sustained window" bar.
Every entry is a REAL run on the stated tree; kills are recorded as loudly as dry runs.

| Date (UTC) | Fuzzer | Range / scope | Result |
| --- | --- | --- | --- |
| 2026-07-16 | CacheStalenessFuzzTests (EXTENDED: +join, +window shapes) | seeds 0-119 x 5 shapes | DRY |
| 2026-07-16 | LiveProvider full suite (all 3 servers) | 1867 tests | GREEN |
| 2026-07-16 | Temporal live (parity + reconstruction + migration behavioural, normtest) | 26 tests | GREEN |
| 2026-07-16 | LinqParityFuzzTests (env-directed sweep, full shape battery ~1300 cases/seed) | seeds 502000-503799 (1800) | DRY |
| 2026-07-16 | CrudStateMachineFuzzTests (env-directed sweep, all 3 machines/seed) | seeds 602000-602749 (750) | DRY |
| 2026-07-16 | **CrudStateMachineFuzzTests KILL at seed 602775** (relationship machine): `Remove()` threw "Entity graph exceeds maximum depth of 10" on a LEGAL re-parenting chain of distinct instances (stale collection membership fixup deliberately leaves behind) — the validator's fixed depth cap was a false-positive tripwire; termination was already guaranteed by the visited set. Cap removed, killing seed pinned as InlineData + `DeepEntityGraphValidationContractTests`. Range 602750-602899 re-swept clean post-fix. | seed 602775 | KILL -> FIXED |
| 2026-07-16 | **Adversarial-probe KILL (Include x AsOf era mixing)**: eager loads under AsOf read the LIVE tables while the root reconstructed history — historical roots returned post-timestamp child values/rows. Fixed: the plan carries the AsOf timestamp and every include level reads through the same history window; M2M + AsOf fails loud (association tables unversioned). Pinned `IncludeAsOfConsistencyContractTests` + `ManyToManyTemporalContractTests`. | adversarial probe | KILL -> FIXED |
| 2026-07-16 | CrudStateMachineFuzzTests (post-fix re-sweep + batches 7-9) | seeds 602750-603349 (600) | DRY |
| 2026-07-16 | TemporalHistoryReconstructionFuzzTests (env-directed sweep, 10 rounds/seed) | seeds 702000-702359 (360) | DRY |
| 2026-07-16 | BulkCudOracleFuzzTests (env-directed sweep, 150 steps/seed) | seeds 802000-806499 (4500) | DRY |
| 2026-07-16 | SqliteMigrationDataPreservationFuzzTests (env-directed sweep, 60 cases/seed) | seeds 902000-904699 (2700) | DRY |
| 2026-07-16 | CacheStalenessFuzzTests (env-directed sweep, 5 shapes/seed) | seeds 1002000-1004399 (2400) | DRY |
| 2026-07-16 | MixedTokenOccFuzzTests (env-directed sweep, 3-context interleaving/seed) | seeds 1102000-1103199 (1200) | DRY |
| 2026-07-16 | RetryFaultInjectionFuzzTests (env-directed sweep, fault machine/seed) | seeds 1202000-1203199 (1200) | DRY |
