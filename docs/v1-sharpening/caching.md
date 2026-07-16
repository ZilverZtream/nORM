# Domain 8 â€” Caching

**Scope:** bounded cache policy (lifetimes, limits, diagnostics), cache tag correctness, and
invalidation so a cached read is never stale after a write to any table it depends on.

## 1.0 exit criteria

- [ ] A cacheable query tags **every** table it reads (including correlated subqueries), so a
      write to any of them invalidates the cached result â€” no silent stale reads.
- [ ] Cache is bounded (documented max size / eviction); memory cannot grow unbounded.
- [ ] Cache keys include tenant + relevant discriminators; no cross-tenant or cross-parameter
      bleed.
- [ ] Direct, batched, and bulk writes all invalidate the correct tags.

## Current confidence

Strong. A silent stale-read was closed: a cacheable query with a correlated subquery tagged only
the root table, so a child write left a stale cached result. Fixed via ambient referenced-table
scope so `plan.Tables` covers all read tables. Found by an adversarial probe after ~10 passing
probes â€” a reminder that cache-tag coverage needs adversarial, not happy-path, testing.

## Open items

- [x] Differential cache-staleness fuzzer added (NH-0801): random shape x random write vs a fresh
      uncached oracle over the same DB; dry over 120 seeds and teeth-proven (a sabotage variant
      fails on a genuinely stale cache). Covers correlated COUNT/SUM + parent-scalar shapes and
      insert/delete/update writes. (RESOLVED 2026-07-16: the fuzzer now generates explicit JOIN-projection and ROW_NUMBER window shapes too - 5 shapes x 120 seeds, dry.)
- [x] Verify bulk-write invalidation covers all touched tables: every write route (direct/batched SaveChanges, BulkInsert/Update/Delete, ExecuteUpdate/ExecuteDelete) individually pinned by `CacheWriteRouteInvalidationContractTests` (NH-0802).
- [x] Confirm bounded-cache limits and diagnostics match `docs/cache-policy.md`: NormMemoryCacheProvider 10,240-entry bound matches; churn coverage cited to the release-gate suites (NH-0802). Also landed there: transaction-rollback cache-poisoning fix, connection-private database cache identity, non-positive-TTL contracts.
- [x] Live-database-swap cache identity (NH-0802 post-Verified cell, 2026-07-16): the key derived
      database identity from the connection STRING, which `ChangeDatabase()` leaves stale while
      repointing the live connection - a cacheable read after the swap served the previous
      database's rows. The key now also includes the live `Connection.Database` name; pinned by
      `CacheDatabaseSwapContractTests` (live, probe-before-fix verified).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~Cache"`
- `docs/cache-policy.md` matches behaviour.

## Risks

Cache correctness is adversarial: happy-path probes pass while a correlated-subquery table goes
untagged. Treat any "N probes passed" as insufficient until an adversarial probe is added.
