# Domain 12 — Public API, DI & documentation (DX)

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
      (`InsertAsync`/…) vs tracked (`Add` + `SaveChangesAsync`) vs bulk/set-based — when to use
      which, a decision table, and the mixing-modes reconciliation.
- [ ] **Key convention decision made** (see open items) — implement per the decision or document
      the explicit-key requirement as deliberate.
- [ ] Getting-started + per-area docs exist and match the code; the naming story (published as
      `TheNorm`, namespace `nORM`) is clear.

## Current confidence

Improving. DI integration and `Set<T>` landed this cycle (governance-clean: snapshot, namespace
policy, docfx pages, tests). API-snapshot/classification/namespace/documentation contract tests
enforce the surface continuously.

## Open items

- [ ] **DECISION NEEDED (product direction):** adopt an EF-Core-style primary-key convention
      (auto-detect `Id` / `<Type>Id`) or keep explicit `[Key]`/fluent only. This changes nORM's
      documented explicit-keys philosophy and can affect legitimately keyless entities that have
      an `Id` column, so it is **not** a safe unilateral change — needs an explicit call.
- [ ] Consider `AddDbContextPool`-style pooling (requires context-reset semantics) — evaluate.
- [x] Direct-vs-tracked write-model doc written (NH-1201): `docs/write-model.md`, linked from README.
- [ ] Freeze the API: declare no-more-breaking-changes and lock the baseline.

## Verification

- `dotnet test tests/ --filter "FullyQualifiedName~PublicApiSnapshot|FullyQualifiedName~PublicApiClassification|FullyQualifiedName~NamespacePolicyContract|FullyQualifiedName~DocumentationContract"`
- `docs/public-api-policy.md`, `docs/namespace-policy.md`.

## Risks

The API freeze is a one-way door for 1.0 — every deferred decision (key convention, pooling,
`Set<T>` vs `Query<T>` naming) must be resolved *before* the freeze, not after.
