# Change Tracking Semantics

`ChangeTracker` is the v1 contract for how nORM observes mutations on materialized entities
and decides what to write at `SaveChangesAsync` time. This document is the supportable surface;
internal optimizations (identity-map sharding, snapshot pooling) may evolve without breaking it.

## Tracking Modes

| Mode | When applied | What is tracked |
| --- | --- | --- |
| `Tracked` (default) | Entity materialized via `ctx.Query<T>()` or `Attach()` | Property snapshot + identity map entry. `DetectChanges` diffs current vs snapshot at `SaveChanges` time. |
| `AsNoTracking` | `query.AsNoTracking()` | No snapshot, no identity map entry. Mutations are invisible to `SaveChanges`. |
| `AsNoTrackingWithIdentityResolution` | `query.AsNoTrackingWithIdentityResolution()` | No mutation tracking, but duplicate rows for the same PK collapse to one CLR instance. |

`SaveChanges` only inspects tracked entities. Entities materialized via `AsNoTracking` must be
re-attached (`ctx.Attach(entity)`) to participate in writes.

## Entity States

| State | How reached | What `SaveChanges` does |
| --- | --- | --- |
| `Unchanged` | Just materialized from a query or `Attach()` | Nothing |
| `Added` | `ctx.Insert(entity)` / `ctx.Add(entity)` | `INSERT`, then populates DB-generated keys |
| `Modified` | Property mutation on a tracked entity, or `ctx.Update(entity)` | `UPDATE` against PK + concurrency token (where applicable) |
| `Deleted` | `ctx.Remove(entity)` | `DELETE` against PK + concurrency token (where applicable) |
| `Detached` | `ctx.Detach(entity)` or never tracked | Nothing |

`DetectChanges` runs implicitly inside `SaveChangesAsync` so applications don't need to call it
explicitly. Calling it manually is supported for diagnostic tooling.

## Attach / Detach

- `ctx.Attach(entity)` inserts the entity into the identity map with state `Unchanged` and
  captures the current property values as the snapshot. Subsequent mutations promote it to
  `Modified` at `SaveChanges` time.
- `ctx.Attach(entity)` validates: the entity must not already be tracked under a conflicting
  PK. The PK must be readable (no shadow-only key).
- `ctx.Detach(entity)` removes the entity from the identity map and discards its snapshot.
  Pending mutations against the detached instance are lost.

## Primary Key Mutation

- Mutating the PK of an `Unchanged` or `Modified` tracked entity is **rejected at SaveChanges
  time** with `NormUsageException`. The original key is captured in the snapshot and any
  `UPDATE`/`DELETE` WHERE predicate uses `OriginalKey`, so a silent PK change would corrupt the
  write.
- For composite keys, mutating *any* PK component is treated as a PK mutation.
- DB-generated default keys (`0`/`Guid.Empty`/empty string) are NOT considered "set" until
  `SaveChanges` populates them; they are a separate identity-map state and don't collide with
  pre-existing rows.

## Immutable Entities and Constructor-Bound Properties

- Properties without a setter (init-only, get-only) are read-only to the tracker. They appear
  in the snapshot but cannot transition the entity to `Modified` through mutation.
- Entities materialized through a public constructor with parameters matching column names
  (positional anonymous-type style or DTO ctor) populate read-only properties at materialization
  time. They are tracked normally for any settable property.
- Records (`record class`) with `init` setters are fully supported when materialized via the
  matching constructor; their values are snapshot but `with`-expression copies are NOT tracked
  (the copy is a new uncontrolled instance).

## Shadow Properties

- Columns declared via fluent `HasShadowProperty<T>(name)` exist in the snapshot and participate
  in `UPDATE`/`DELETE` predicates, but have no corresponding CLR property.
- Shadow property access is via `ctx.Entry(entity).Property(name).CurrentValue`.
- Tenant columns are a shadow property special case: nORM injects them automatically based on
  the configured `TenantProvider` and excludes them from explicit `Update`/`Insert` mutations.

## Owned Types

- An owned reference (`HasOne(o => o.Address).WithOwner()`) shares its parent's identity-map
  key. Mutating an owned property promotes the *parent* entity to `Modified`.
- Owned collections (`HasMany(o => o.Lines).WithOwner()`) have separate identity-map entries
  per element; their state is independent of the parent.
- Deleting the parent cascades to owned entities at the database level (FK ON DELETE CASCADE).

## Relationship Fixup

- When a navigation property is materialized via `Include`/`ThenInclude`, the tracker links the
  principal and dependent identity-map entries automatically (relationship fixup).
- When a new dependent is added via `principal.Children.Add(child)`, the tracker promotes
  `child` to `Added` and sets the FK column from the principal's PK (or DB-generated default if
  the principal is also `Added`).
- Removing from a collection alone does NOT delete the dependent; call `ctx.Remove(child)`
  explicitly for cascade behavior controlled by the application.

## Notification Tracking

- Entities implementing `INotifyPropertyChanged` are tracked without snapshot diffing: every
  property change immediately marks the entity as `Modified`. This is faster for high-churn
  entities but requires the entity type to implement the interface correctly.
- Mixed notification + snapshot tracking in the same context is supported; the tracker chooses
  per entity based on interface detection.

## Concurrency Tokens

See `docs/optimistic-concurrency.md`. Briefly: tracked entities with a `[ConcurrencyCheck]` or
`[Timestamp]` column include that column in `UPDATE`/`DELETE` WHERE predicates. The original
token value is read from the snapshot, not the current entity, so a token mutation between
materialization and `SaveChanges` produces a correct conflict detection.

## Cancellation

`SaveChangesAsync(CancellationToken)` honors cancellation between batches. Cancellation
mid-batch lets the underlying transaction roll back, leaving entity state unchanged in memory
(snapshots are not updated on a failed write).

## Graph Tracking

`ChangeTracker.TrackGraph(object rootEntity, Action<EntityEntryGraphNode> callback)` walks an
untracked object graph reachable from `rootEntity` and invokes `callback` once per node, letting
the application decide each entity's state (EF Core parity for disconnected-graph attach):

- Traversal is depth-first over discovered navigation properties; each already-visited entity is
  visited once (identity by reference), so cycles terminate.
- The callback receives an `EntityEntryGraphNode` exposing the node's `Entry`, the `SourceEntry`
  it was reached from, and the `InboundNavigation` name. A node whose state the callback leaves
  `Detached` stops traversal into its children, matching EF Core.
- `TrackGraph<TState>(object rootEntity, TState state, Func<EntityEntryGraphNode<TState>, bool>
  callback)` threads a caller-supplied `state` value through every node and uses the callback's
  `bool` return to decide whether to keep descending - the hot-path overload that avoids a
  closure capture per graph.

## Strongly-typed Entry&lt;TEntity&gt;

`ctx.Entry<TEntity>(entity)` returns an `EntityEntry<TEntity>` - the generic form of
`ctx.Entry(object)`. It exposes the same surface (`State`, `Entity`, `CurrentValues`,
`OriginalValues`, `GetDatabaseValues(Async)`, `Reload(Async)`, `Reference`/`Collection`/
`Navigation`, `Property(name)`) plus a lambda accessor `Property(e => e.Name)` so property
access is compile-time checked instead of stringly-typed. `Entry` on the generic entry returns
the underlying non-generic `EntityEntry` when an untyped handle is needed.

## Tested Contract

`tests/ChangeTrackingSemanticsTests.cs`, `tests/PreciseChangeTrackingTests.cs`,
`tests/ConstructorBoundEntityTrackingTests.cs`, `tests/ShadowPropertyTests.cs`,
`tests/OwnedTypesTests.cs`, `tests/PKMutationTests.cs`, `tests/ChangeTrackerNotificationTests.cs`,
`tests/ChangeTrackerTrackGraphTests.cs`, `tests/ChangeTrackerTrackGraphStateTests.cs`,
`tests/EntityEntryOfTTests.cs`,
and the cancellation/concurrency suites mechanically enforce the rules above.
`ChangeTrackingContractDocTests` asserts this document continues to describe the supported
v1 behavior.
