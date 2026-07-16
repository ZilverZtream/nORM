# Fuzzer dry-window log

Chronological, dated evidence toward the RELEASE.md "fuzzers dry for a sustained window" bar.
Every entry is a REAL run on the stated tree; kills are recorded as loudly as dry runs.

| Date (UTC) | Fuzzer | Range / scope | Result |
| --- | --- | --- | --- |
| 2026-07-16 | CacheStalenessFuzzTests (EXTENDED: +join, +window shapes) | seeds 0-119 x 5 shapes | DRY |
| 2026-07-16 | LiveProvider full suite (all 3 servers) | 1867 tests | GREEN |
| 2026-07-16 | Temporal live (parity + reconstruction + migration behavioural, normtest) | 26 tests | GREEN |
| 2026-07-16 | LinqParityFuzzTests (env-directed sweep, full shape battery ~1300 cases/seed) | seeds 502000-502199 (200) | DRY |
| 2026-07-16 | CrudStateMachineFuzzTests (env-directed sweep, all 3 machines/seed) | seeds 602000-602149 (150) | DRY |
