# Domain 12 â€” Public API, DI & documentation (DX)

**Scope:** the public API surface and its freeze, hosting/DI integration, EF-Core-parity
ergonomics, and documentation completeness. This domain gates the cross-cutting "API frozen +
documented" bar.

## 1.0 exit criteria

- [ ] **API freeze:** no planned breaking changes; `PublicApi.Shipped.txt` is intentional and
      reviewed; every public member has XML docs and at least one test; every public namespace is
      classified in `docs/namespace-policy.md`.
- [ ] **Hosting integration:** `AddNorm` / `AddNorm<TContext>` / `AddNormFactory<TContext>` +
      `INormDbContextFactory<TContext>` with correct scoped lifetime and disposal. *(Done.)*
- [ ] **Entry-point parity:** `context.Set<T>()` alias for `Query<T>()`. *(Done.)*
- [x] **Write mental-model documented** (NH-1201): `docs/write-model.md` covers direct-write
      (`InsertAsync`/â€¦) vs tracked (`Add` + `SaveChangesAsync`) vs bulk/set-based â€” when to use
      which, a decision table, and the mixing-modes reconciliation.
- [x] **Key convention decision made**: EF-parity Id convention ADOPTED (user decision 2026-07-16) and implemented in the mapper - a property named "Id" or "<Type>Id" becomes the primary key when no explicit [Key]/HasKey is configured; explicit configuration always wins, and the mapper now agrees with the assembly-driven snapshot builder. Original scope: (see open items) â€” implement per the decision or document
      the explicit-key requirement as deliberate.
- [ ] Getting-started + per-area docs exist and match the code; the naming story (published as
      `TheNorm`, namespace `nORM`) is clear.

## Current confidence

Improving. DI integration and `Set<T>` landed this cycle (governance-clean: snapshot, namespace
policy, docfx pages, tests). API-snapshot/classification/namespace/documentation contract tests
enforce the surface continuously.

## Open items

- [x] **DECISION MADE:** EF-Core-style primary-key convention adopted and shipped (see above). Original text: adopt an EF-Core-style primary-key convention
      (auto-detect `Id` / `<Type>Id`) or keep explicit `[Key]`/fluent only. This changes nORM's
      documented explicit-keys philosophy and can affect legitimately keyless entities that have
      an `Id` column, so it is **not** a safe unilateral change â€” needs an explicit call.
- [~] Consider `AddDbContextPool`-style pooling (requires context-reset semantics) â€” EVALUATED
      2026-07-16 (measurement harness: `ContextConstructionCostProbeTests`). Findings: a fresh
      context per query costs ~0.283 ms/op vs ~0.013 ms/op on a warm reused context (2-entity
      model, trivial query, in-memory SQLite) â€” pooling would save ~0.27 ms/request and the gap
      GROWS with model size because `TableMapping` reflection, prepared-command and fast-path SQL
      caches are all per-context by design. Reset inventory for a future pool lease: MUST clear
      ChangeTracker/identity map, refuse leases holding a live transaction, and re-apply/clear the
      native tenant session key (`_nativeTenantSessionAppliedKey` is a cross-tenant leak hazard);
      MUST KEEP the warm per-context caches (that is the point) and the context's own connection â€”
      pooling connection+context as a unit sidesteps the cross-database cache-identity hazard the
      caching matrix fixed. DECISION: implementation deferred to a dedicated effort (reset audit +
      tenant/transaction leak stress tests are enterprise-grade work); crucially the feature is
      API-ADDITIVE (`AddNormPool<TContext>` + an internal reset â€” no existing member changes), so
      **pooling does NOT block the API freeze** â€” which resolves this item's part of the freeze
      risk below.
- [x] Direct-vs-tracked write-model doc written (NH-1201): `docs/write-model.md`, linked from README.
- [ ] Freeze the API: declare no-more-breaking-changes and lock the baseline.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~PublicApiSnapshot|FullyQualifiedName~PublicApiClassification|FullyQualifiedName~NamespacePolicyContract|FullyQualifiedName~DocumentationContract"`
- `docs/public-api-policy.md`, `docs/namespace-policy.md`.

## Risks

The API freeze is a one-way door for 1.0 â€” every deferred decision (key convention, pooling,
`Set<T>` vs `Query<T>` naming) must be resolved *before* the freeze, not after.
