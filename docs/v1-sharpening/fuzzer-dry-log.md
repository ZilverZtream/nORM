# Fuzzer dry-window log

Chronological, dated evidence toward the RELEASE.md "fuzzers dry for a sustained window" bar.
Every entry is a REAL run on the stated tree; kills are recorded as loudly as dry runs.

| Date (UTC) | Fuzzer | Range / scope | Result |
| --- | --- | --- | --- |
| 2026-07-16 | CacheStalenessFuzzTests (EXTENDED: +join, +window shapes) | seeds 0-119 x 5 shapes | DRY |
| 2026-07-16 | LiveProvider full suite (all 3 servers) | 1867 tests | GREEN |
| 2026-07-16 | Temporal live (parity + reconstruction + migration behavioural, normtest) | 26 tests | GREEN |
| 2026-07-16 | LinqParityFuzzTests (env-directed sweep, full shape battery ~1300 cases/seed) | seeds 502000-502399 (400) | DRY |
| 2026-07-16 | CrudStateMachineFuzzTests (env-directed sweep, all 3 machines/seed) | seeds 602000-602299 (300) | DRY |
| 2026-07-16 | TemporalHistoryReconstructionFuzzTests (env-directed sweep, 10 rounds/seed) | seeds 702000-702079 (80) | DRY |
| 2026-07-16 | BulkCudOracleFuzzTests (env-directed sweep, 150 steps/seed) | seeds 802000-802999 (1000) | DRY |
| 2026-07-16 | SqliteMigrationDataPreservationFuzzTests (env-directed sweep, 60 cases/seed) | seeds 902000-902599 (600) | DRY |
| 2026-07-16 | CacheStalenessFuzzTests (env-directed sweep, 5 shapes/seed) | seeds 1002000-1002299 (300) | DRY |
| 2026-07-16 | MixedTokenOccFuzzTests (env-directed sweep, 3-context interleaving/seed) | seeds 1102000-1102149 (150) | DRY |
| 2026-07-16 | RetryFaultInjectionFuzzTests (env-directed sweep, fault machine/seed) | seeds 1202000-1202149 (150) | DRY |
