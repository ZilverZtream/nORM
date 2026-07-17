# Domain 7 — Multi-tenancy

**Scope:** tenant boundary enforced on **every** generated read and write path, native
database RLS integration, and cross-tenant isolation guarantees.

## 1.0 exit criteria

- [ ] No query or write path can read or mutate another tenant's rows — enforced on generated
      SELECT, INSERT, UPDATE, DELETE, bulk, and navigation/Include paths.
- [ ] A tenant-isolation fuzzer/adversarial suite finds zero cross-tenant leaks.
- [ ] Native tenant session context / RLS (`EnableNativeTenantSessionContext`) is correct per
      provider and documented; strict mobility gating is respected.
- [ ] Tenant + temporal + cache interactions preserve isolation (a cached result for tenant A is
      never served to tenant B).

## Current confidence

Strong. Tenant enforcement is applied on every generated query and write path by design; the
tenant column resolves from `Options.TenantColumnName`. Native RLS paths exist per provider.

## Open items

- [x] Adversarial cross-tenant leak sweep confirmed green (NH-0701): 157 tests across reads,
      writes, bulk, Include/nav, M2M, owned collections, compiled queries, result/plan-cache
      poisoning, fail-closed, and global-filter bypass.
- [x] Cache keys include the tenant discriminator on every cacheable path (NH-0701):
      `MultiTenantResultCachePoisoning` + `MultiTenantPlanCache` green.
- [x] **KILL 49 (found and FIXED 2026-07-17): explicit join shapes leaked across tenants —
      live.** GroupJoin's grouped inner rows (`(d, es) => es.Count()`) and the
      GroupJoin+DefaultIfEmpty LEFT-JOIN flatten dropped the inner source's injected tenant
      Where — the root rewrite wraps Join/GroupJoin inner sources, but only the INNER-Join
      translator pushed inner WHERE conditions into the JOIN ON clause. Cross-join
      SelectMany's inner visibility renderer applied global filters without the tenant
      predicate (the inner source sits inside the collection-selector lambda, which the
      top-level rewrite never reaches). Fixed: GroupJoin and the left-join flatten push the
      extracted inner conditions into the LEFT JOIN ON (a filtered-out inner row reads as
      unmatched, so outer rows keep empty groups instead of leaking), and the SelectMany
      inner renderer uses the full visibility predicate. Pinned in
      `TenantJoinAsOfConsistencyContractTests`.
- [x] **KILL 48 (found and FIXED 2026-07-17): navigation subqueries leaked across tenants —
      live, not temporal.** The NH-0701 sweep missed the correlated navigation-aggregate
      emitters: `d.Emps.Count()` in a projection counted a FOREIGN tenant's dependent that
      shared the foreign-key value; the nav Sum/Min/Max/Avg emitters (projection and
      predicate side) applied NO visibility filters at all (soft-delete included); the
      two-hop count applied none; and a forged FK exposed another tenant's principal VALUES
      through nav-scalar reads. Fixed via `GlobalFilterFragment.CombineWithTenant` applied
      at every navigation-subquery emitter — count/Any/All, scalar aggregates on both
      sides, both hops of the two-hop, and all principal-read paths (a cross-tenant
      principal reads as MISSING, exactly like a soft-deleted one). Pinned by
      `TenantJoinAsOfConsistencyContractTests` (era-windowed shapes under AsOf, live
      aggregates, and forged-FK probes on both tenants' sides).
- [x] Confirm native RLS behaviour on live SQL Server / PostgreSQL / MySQL. (Closed 2026-07-16:
      `LiveProviderNativeTenantSecurityTests` 4/4 non-vacuous on the live servers — SQL Server's
      RLS policy blocks direct cross-tenant access, PostgreSQL's policy blocks when the role is
      subject to RLS, and the native session context is written on both RLS-capable providers.
      MySQL is a DESIGN EXCEPTION, not a gap: the engine has no row-level security, so
      `SupportsNativeTenantSessionContext` stays false there and nORM's generated-path tenant
      enforcement (the NH-0701 adversarial sweep above) is the isolation mechanism.)

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~Tenant"`
- `docs/multi-tenancy-security.md`, `docs/tenant-boundary.md`, `docs/tenant-database-native-rls.md`.

## Risks

Multi-tenancy is a security boundary, not just a filter — a single unfiltered path is a
data-disclosure defect, not an ergonomics gap. It gates the cross-cutting "zero known defects".
