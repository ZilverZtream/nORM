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
- [ ] Confirm native RLS behaviour on live SQL Server / PostgreSQL / MySQL (deferred to the live
      provider gate; needs credentials).

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~Tenant"`
- `docs/multi-tenancy-security.md`, `docs/tenant-boundary.md`, `docs/tenant-database-native-rls.md`.

## Risks

Multi-tenancy is a security boundary, not just a filter — a single unfiltered path is a
data-disclosure defect, not an ergonomics gap. It gates the cross-cutting "zero known defects".
