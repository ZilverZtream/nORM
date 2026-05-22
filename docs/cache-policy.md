# Cache Policy

nORM uses bounded caches for hot metadata, query plans, materializers, dynamic
types, and prepared commands. v1 treats cache behavior as an operational
contract: shared caches must have size limits, per-context caches must be cleared
when the context is disposed, and supported public caches must expose basic
diagnostics.

## Shared Runtime Caches

| Cache | Lifetime | Bound | Diagnostics / Clear |
| --- | --- | --- | --- |
| Query plan cache | process-wide | adaptive LRU, 100 to 10,000 entries, one-hour TTL, max 64 MB budget | internal hit/miss/eviction counters through `ConcurrentLruCache`; cleared on high memory load |
| Dynamic table type cache | process-wide | LRU, 1,000 entries | internal bounded cache; new schema signatures produce new entries |
| Compiled materializer store | process-wide | LRU, 500 entries | public `Count`, `Hits`, `Misses`, `Evictions`, `HitRate`, and `Clear()` |
| Materializer factory caches | process-wide | bounded LRU where row-shape caches are used; type/conversion caches are keyed by CLR metadata | internal cache statistics are exposed to tests |
| SQL Server bulk key table schema cache | process-wide | LRU, 100 entries | internal bounded cache |

## Per-Context Caches

| Cache | Lifetime | Bound / Clear |
| --- | --- | --- |
| Mapping cache | `DbContext` instance | cleared by disposing the context |
| Fast-path SQL cache | `DbContext` instance | cleared with the context |
| Prepared insert command cache | `DbContext` instance | disposed and cleared by `DbContext.Dispose` / `DisposeAsync` |
| Fast-path prepared command cache | `DbContext` instance | disposed and cleared by `DbContext.Dispose` / `DisposeAsync` |
| Query-provider pooled plan/count commands | `NormQueryProvider` instance | disposed and cleared when the provider is disposed |

## User Result Cache

`DbContextOptions.UseInMemoryCache()` installs the built-in query result cache
provider. Query result cache entries are opt-in through `Cacheable(...)` and use
`DbContextOptions.CacheExpiration`, which defaults to five minutes. Bulk and
write paths invalidate table tags after mutation.

## Operational Guidance

Long-running processes should use short-lived `DbContext` instances. Shared
process-wide caches are bounded and can be inspected where they are public API.
For source-generated materializers, `CompiledMaterializerStore.Clear()` is the
supported manual reset hook for tests, tenant/model reload scenarios, and
diagnostic maintenance windows.

## Release Gate Evidence

The v1 RC gate runs a dedicated cache memory bounds step:

- `CacheMemoryBoundReleaseGateTests.ConcurrentLruCache_AdversarialChurn_StaysBoundedAndEvicts`
- `CacheMemoryBoundReleaseGateTests.BoundedCache_AdversarialChurn_StaysBelowMaxSize`
- `CacheMemoryBoundReleaseGateTests.CompiledMaterializerStore_TableNameChurn_StaysBoundedAndObservable`
- `CacheFaultInjectionStressTests.BoundedCacheEvictionUnderContention_30Tasks_SizeBounded_ValuesCorrect`
- `ConcurrentLruCacheStressTests`
- `CacheLockConcurrencyTests`

These tests intentionally churn beyond cache capacity and assert entry counts,
eviction counters, and latest-entry usability. They are count-based rather than
timing-based so release evidence is stable across developer machines and CI
agents.
