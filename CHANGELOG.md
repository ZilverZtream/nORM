# Changelog

All notable changes to nORM are tracked here. Release entries should follow
semantic versioning and separate breaking changes, features, fixes, performance,
security, and documentation.

## Unreleased

### Breaking / API

- Public API is under v1 review and may still change before `1.0.0`. New public
  surface is pinned by `tests/PublicApi.Shipped.txt` and reviewed in
  `docs/public-api-policy.md`.

### Features

- Ordered / top-N `Include`: ordered and `Take`/`Skip`-limited navigation
  collections translate to a `ROW_NUMBER()` window (EF Core parity).
- EF Core-parity fluent property verbs: `IsRequired`, `IsRowVersion`,
  `HasColumnType`, `HasDefaultValue` / `HasDefaultValueSql`, `HasComment`, and
  `ValueGeneratedOnAdd` / `ValueGeneratedNever` / `ValueGeneratedOnAddOrUpdate`.
- Reusable entity configuration via `IEntityTypeConfiguration<TEntity>` with
  `ModelBuilder.ApplyConfiguration` / `ApplyConfigurationsFromAssembly`, and an
  overridable `OnModelCreating`.
- `AddNormPool<TContext>` dependency-injection context pooling (EF Core
  `AddDbContextPool` parity), resetting per-request and tenant state on return
  and refusing to pool a context holding a live transaction.
- Strongly-typed `Entry<TEntity>` with compile-time-checked lambda property
  access, and a `ChangeTracker.TrackGraph<TState>` hot-path overload.
- Incremental migration diffing now detects `HasColumnType` store-type and
  `HasComment` column-comment changes across all providers.
- Broader value-converter coverage: `Count`/bool-member predicates, `IN`-list
  elements, `GroupBy` keys, scalar `Min`/`Max`, and computed `SetProperty`
  values now bind converted provider values; `Guid` columns materialize from
  string or blob storage; the generic materializer fails loud instead of
  silently defaulting on a conversion it cannot perform.
- Table-per-hierarchy discriminator filtering on the read fast paths and for
  navigations to TPH-derived types, plus fluent `HasForeignKey` with an
  inherited key.

### Performance

- Fixed `SaveChanges` over-allocating the SQL buffer for small saves (batch size
  is now capped to the actual entity count), bringing tracked-update allocation
  from ~3.4x EF Core to parity.
- Single-group fast-path for the common `SaveChanges` shape and an early return
  from the topological sort for a single mapping.
- Consolidated the plan-cache fingerprint and parameter-value expression-tree
  walks into a single pass on the runtime query path.
- Pooled prepared-command reuse and a cached plan on the single-row read fast
  path.
- Added fair comparative single Update/Delete and set-based ExecuteUpdate/
  ExecuteDelete benchmarks alongside the existing read/insert matrix.

### Security

- Documented multi-tenancy, logging redaction, raw SQL, and vulnerability
  reporting boundaries.
- Context pooling clears the applied native tenant-session key on return so a
  pooled context never inherits the previous lease's tenant scope.

### Documentation

- Added v1 operational and release-governance docs.
- Documented the fluent model-configuration verbs, context pooling, graph
  tracking, and the strongly-typed entry in `README.md` and
  `docs/change-tracking.md`.
