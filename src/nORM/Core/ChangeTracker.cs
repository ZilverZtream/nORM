using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Reflection;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Mapping;
using RefComparer = System.Collections.Generic.ReferenceEqualityComparer;
#nullable enable
namespace nORM.Core
{
    /// <summary>
    /// Keeps track of entity instances and monitors their state so that changes can be
    /// persisted to the database. The tracker manages identity resolution and
    /// coordinates change detection for entities attached to a <see cref="DbContext"/>.
    /// </summary>
    public sealed partial class ChangeTracker
    {
        private readonly ConcurrentDictionary<object, EntityEntry> _entriesByReference = new(RefComparer.Instance);
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, EntityEntry>> _entriesByKey = new();
        private readonly ConcurrentDictionary<EntityEntry, byte> _nonNotifyingEntries = new();
        private readonly ConcurrentDictionary<EntityEntry, byte> _dirtyNonNotifyingEntries = new();
        private readonly ConcurrentDictionary<EntityEntry, byte> _dirtyEntries = new();
        private readonly DbContextOptions _options;

        // Synchronizes Track and Remove operations to prevent TOCTOU races between
        // reference-check, PK-lookup, and dictionary mutation sequences.
        private readonly object _trackLock = new object();

        /// <summary>
        /// Maximum depth for cascade-delete graph traversal. 10 levels accommodates
        /// legitimate entity hierarchies while catching infinite cycles early.
        /// </summary>
        private const int MaxCascadeDepth = 10;

        // Hash seed primes for CompositeKey.GetHashCode (Bernstein-style polynomial hash).
        private const int HashSeed = 17;
        private const int HashMultiplier = 23;

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeTracker"/> class using the
        /// specified context options.
        /// </summary>
        /// <param name="options">Options that influence change-tracking behavior.</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
        public ChangeTracker(DbContextOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            _options = options;
        }

        /// <summary>
        /// The <see cref="DbContext"/> that owns this tracker, bound once during context construction so
        /// entries can reach it (e.g. <see cref="EntityEntry.Reload"/> re-queries through it). Null only
        /// before the owning context finishes constructing.
        /// </summary>
        internal DbContext? Context { get; private set; }

        /// <summary>Associates this tracker with its owning context. Called once by the DbContext constructor.</summary>
        internal void BindContext(DbContext context) => Context = context;

        /// <summary>
        /// Begins tracking the specified entity instance and associates it with the
        /// provided <paramref name="mapping"/>. If the entity is already being tracked
        /// either by reference or by its primary key, the existing <see cref="EntityEntry"/>
        /// is returned and its state updated.
        /// </summary>
        /// <param name="entity">The entity instance to track.</param>
        /// <param name="state">The initial state to assign to the entity.</param>
        /// <param name="mapping">Mapping information for the entity type.</param>
        /// <returns>The <see cref="EntityEntry"/> representing the tracked entity.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="entity"/> or <paramref name="mapping"/> is <c>null</c>.</exception>
        internal EntityEntry Track(object entity, EntityState state, TableMapping mapping)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(mapping);

            // Lock the entire operation to prevent TOCTOU between reference check and PK operations.
            // Prevents two threads from tracking the same entity concurrently and causing state corruption.
            lock (_trackLock)
            {
                // Fast path: entity already tracked by reference
                if (_entriesByReference.TryGetValue(entity, out var existingEntry))
                {
                    // Only update state if not downgrading from a "dirty" state to Unchanged
                    // This prevents races where Added/Modified entities get reset to Unchanged
                    if (!(existingEntry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted
                          && state == EntityState.Unchanged))
                    {
                        existingEntry.SetStateInternal(state);
                        // When explicitly marking as Modified (ctx.Update), prevent DetectChanges
                        // from reverting the state to Unchanged when no scalar properties changed.
                        if (state == EntityState.Modified)
                            existingEntry.MarkExplicitlyModified();
                    }

                    // If the entity now has a primary key that wasn't tracked, add it
                    var existingPk = GetPrimaryKeyValue(entity, existingEntry.Mapping);
                    if (existingPk != null)
                    {
                        var typeEntries = _entriesByKey.GetOrAdd(
                            existingEntry.Mapping.Type,
                            _ => new ConcurrentDictionary<object, EntityEntry>());
                        typeEntries.TryAdd(existingPk, existingEntry);
                    }
                    return existingEntry;
                }

                var pk = GetPrimaryKeyValue(entity, mapping);

                // Check-and-insert by PK under the outer lock, preventing TOCTOU races
                // between reference-check, PK-lookup, and dictionary mutation sequences.
                if (pk != null)
                {
                    var typeEntries = _entriesByKey.GetOrAdd(
                        mapping.Type,
                        _ => new ConcurrentDictionary<object, EntityEntry>());

                    // Attempt PK-based lookup before creating a new entry to avoid constructing
                    // an EntityEntry that we immediately discard. Side effects (_entriesByReference
                    // and _nonNotifyingEntries mutations) happen AFTER successful TryAdd, never inside
                    // a GetOrAdd factory — factories may execute concurrently in ConcurrentDictionary
                    // even when the outer lock serialises writers, and side-effectful factories are
                    // fragile (the losing factory's side effects run but its result is discarded,
                    // causing dangling entries in _nonNotifyingEntries).
                    if (!typeEntries.TryGetValue(pk, out var pkEntry))
                    {
                        var newEntry = state == EntityState.Unchanged && !_options.EagerChangeTracking
                            ? CreateLazyEntry(entity, mapping)
                            : new EntityEntry(entity, state, mapping, _options, this);

                        // When explicitly tracking as Modified (ctx.Update on a new-to-context entity),
                        // prevent DetectChanges from reverting the state to Unchanged when no scalar
                        // properties differ from the freshly-taken snapshot (they never will because the
                        // snapshot is captured at the same moment as the entity's current values).
                        if (state == EntityState.Modified)
                            newEntry.MarkExplicitlyModified();

                        if (typeEntries.TryAdd(pk, newEntry))
                        {
                            // We won the insertion race — register the side-effect collections.
                            _entriesByReference.TryAdd(entity, newEntry);
                            if (entity is not INotifyPropertyChanged)
                                _nonNotifyingEntries.TryAdd(newEntry, 0);
                            return newEntry;
                        }

                        // Lost insertion race (concurrent writer with same PK despite outer lock —
                        // should not occur in single-threaded use; defensive for unexpected re-entry).
                        // Fall through to re-read the winning entry.
                        pkEntry = typeEntries[pk];
                    }

                    // An entry with this PK already existed (or was just inserted by another writer).
                    if (!ReferenceEquals(pkEntry.Entity, entity))
                    {
                        if (state == EntityState.Added)
                        {
                            if (pkEntry.State == EntityState.Deleted)
                            {
                                // Replace-in-place: keep the pending DELETE and track the new
                                // instance as this key's insert. SaveChanges hoists same-key
                                // deletes before inserts, so the batch is DELETE then INSERT.
                                // The new entry takes over the identity-map slot so PK lookups
                                // resolve to the live instance; the deleted entry remains
                                // reference-tracked and therefore still executes.
                                var replacementEntry = new EntityEntry(entity, EntityState.Added, mapping, _options, this);
                                typeEntries[pk] = replacementEntry;
                                _entriesByReference.TryAdd(entity, replacementEntry);
                                if (entity is not INotifyPropertyChanged)
                                    _nonNotifyingEntries.TryAdd(replacementEntry, 0);
                                return replacementEntry;
                            }
                            // Adopting the state onto the OTHER instance's entry would insert
                            // that instance's values and silently discard this one's.
                            throw new InvalidOperationException(
                                $"Cannot add an instance of '{mapping.Type.Name}' with key '{pk}': another instance with " +
                                "the same key is already being tracked. Mutate the tracked instance, or remove it (and " +
                                "save) before adding the replacement.");
                        }
                        if (state == EntityState.Modified)
                        {
                            // Update with a detached instance over a tracked key: adopting
                            // Modified onto the OTHER instance's entry leaves its unchanged
                            // values in place — change detection then finds no diff and the
                            // detached instance's values silently vanish (lost update).
                            throw new InvalidOperationException(
                                $"Cannot update an instance of '{mapping.Type.Name}' with key '{pk}': another instance with " +
                                "the same key is already being tracked. Mutate the tracked instance instead, or detach it " +
                                "before attaching the replacement values.");
                        }
                        // Different CLR instance with same PK — update state but don't downgrade
                        // from a dirty state to Unchanged.
                        if (!(pkEntry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted
                              && state == EntityState.Unchanged))
                        {
                            pkEntry.SetStateInternal(state);
                        }
                        // Note: the new `entity` reference is NOT added to _entriesByReference here;
                        // the winner's reference remains the canonical tracked instance for this PK.
                        // Callers that need reference-based lookup should use the returned entry's Entity.
                    }

                    return pkEntry;
                }

                // No PK - only track by reference
                var entry = state == EntityState.Unchanged && !_options.EagerChangeTracking
                    ? CreateLazyEntry(entity, mapping)
                    : new EntityEntry(entity, state, mapping, _options, this);

                if (_entriesByReference.TryAdd(entity, entry))
                {
                    // Successfully added - set up additional tracking
                    if (entity is not INotifyPropertyChanged)
                        _nonNotifyingEntries.TryAdd(entry, 0);
                    return entry;
                }

                // Under the lock this path is unreachable in normal single-threaded usage of DbContext.
                // It would only be reached if Clear() or another Remove() ran concurrently between the
                // earlier TryGetValue (line above fast-path) and this TryAdd. Retrieve the winner entry.
                if (_entriesByReference.TryGetValue(entity, out var raceEntry))
                {
                    // Don't downgrade from dirty states
                    if (!(raceEntry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted
                          && state == EntityState.Unchanged))
                    {
                        raceEntry.SetStateInternal(state);
                    }
                    return raceEntry;
                }

                // Defensive fallback: the entity was removed between TryAdd and TryGetValue.
                // Re-add the entry we already constructed.
                _entriesByReference.TryAdd(entity, entry);
                if (entity is not INotifyPropertyChanged)
                    _nonNotifyingEntries.TryAdd(entry, 0);
                return entry;
            }
        }

        /// <summary>
        /// Creates a lightweight tracking entry that postpones expensive change
        /// detection setup until it is explicitly required. This is used when an
        /// entity is attached in the <see cref="EntityState.Unchanged"/> state and
        /// eager change tracking is disabled.
        /// </summary>
        /// <param name="entity">The entity to create the entry for.</param>
        /// <param name="mapping">Mapping information for the entity type.</param>
        /// <returns>A lazily-initialized <see cref="EntityEntry"/> instance.</returns>
        private EntityEntry CreateLazyEntry(object entity, TableMapping mapping)
        {
            // Minimal entry that defers property change setup
            return new EntityEntry(entity, EntityState.Unchanged, mapping, _options, this, lazy: true);
        }

        /// <summary>
        /// Removes an entity from the change tracker, optionally cascading the removal
        /// to related entities that are configured for cascade delete.
        /// </summary>
        /// <param name="entity">The entity instance to stop tracking.</param>
        /// <param name="cascade">If <c>true</c>, related entities configured with cascade
        /// delete will also be detached.</param>
        /// <exception cref="ArgumentNullException"><paramref name="entity"/> is <c>null</c>.</exception>
        internal void Remove(object entity, bool cascade = false)
        {
            ArgumentNullException.ThrowIfNull(entity);
            lock (_trackLock)
            {
                if (_entriesByReference.TryRemove(entity, out var entry))
                {
                    entry.DetachEntity();
                    _nonNotifyingEntries.TryRemove(entry, out _);
                    _dirtyNonNotifyingEntries.TryRemove(entry, out _);
                    _dirtyEntries.TryRemove(entry, out _);
                    // EntityEntry.OriginalKey stores composite keys as object?[] (CaptureKey shape),
                    // but _entriesByKey is keyed by tuples/(CompositeKey) (GetPrimaryKeyValue shape).
                    // Convert OriginalKey to the lookup shape; fall back to reading current PK when null.
                    var pk = entry.OriginalKey != null
                        ? ToLookupKey(entry.OriginalKey, entry.Mapping)
                        : GetPrimaryKeyValue(entity, entry.Mapping);
                    if (pk != null && _entriesByKey.TryGetValue(entry.Mapping.Type, out var typeEntries))
                    {
                        typeEntries.TryRemove(pk, out _);
                        if (typeEntries.IsEmpty)
                            _entriesByKey.TryRemove(entry.Mapping.Type, out _);
                    }
                    if (cascade)
                    {
                        CascadeDelete(entity, entry.Mapping);
                    }
                }
            }
        }

        // EntityEntry.CaptureKey returns object?[] for composite keys, but _entriesByKey
        // uses the same shape as GetPrimaryKeyValue (ValueTuple for 2/3 keys, CompositeKey for >3).
        // This method converts from the array shape to the dictionary-compatible shape.
        private static object? ToLookupKey(object? originalKey, TableMapping mapping)
        {
            if (originalKey == null) return null;
            if (mapping.KeyColumns.Length <= 1) return originalKey; // single key - same shape
            if (originalKey is not object?[] arr) return originalKey; // already in lookup shape
            if (arr.Length == 2) return (arr[0], arr[1]);
            if (arr.Length == 3) return (arr[0], arr[1], arr[2]);
            return new CompositeKey(arr);
        }

        /// <summary>
        /// Traverses the entity graph starting from <paramref name="rootEntity"/> using
        /// breadth-first search and detaches any dependent entities marked for cascade
        /// deletion. This ensures that related entities do not remain tracked when their
        /// parent is removed. The graph is traversed in two phases: discovery then removal,
        /// to avoid mutating the tracker while walking navigation properties.
        /// </summary>
        /// <param name="rootEntity">The root entity being deleted.</param>
        /// <param name="rootMapping">Mapping information for the root entity.</param>
        /// <remarks>
        /// Caller must already hold <see cref="_trackLock"/>. Phase-2 removals use
        /// <see cref="RemoveUnlocked"/> to avoid re-entrant lock acquisition.
        /// Cycles in the entity graph (bidirectional references) are silently skipped via
        /// the <c>visited</c> set — not thrown as exceptions — because bidirectional 1:N
        /// relationships (Parent → Children, Child.Parent back-ref) are common and should
        /// not be treated as errors.
        /// </remarks>
        private void CascadeDelete(object rootEntity, TableMapping rootMapping)
        {
            // Collect all entities to delete first, then remove them to prevent premature removal
            // from blocking discovery of descendants during graph traversal.
            var queue = new Queue<(object Entity, TableMapping Mapping, int Depth)>();
            // Use reference equality to avoid false cycle-detection when entities override Equals/GetHashCode.
            var visited = new HashSet<object>(RefComparer.Instance);
            var toRemove = new List<object>();

            queue.Enqueue((rootEntity, rootMapping, 0));
            visited.Add(rootEntity);

            // Phase 1: Discover all entities in the cascade delete graph
            while (queue.Count > 0)
            {
                var (entity, mapping, depth) = queue.Dequeue();

                // Only add to removal list if this is not the root (root is already being removed by caller
                // in Remove() before CascadeDelete is invoked).
                if (depth > 0)
                    toRemove.Add(entity);

                // Stop expanding children when max depth is reached
                if (depth >= MaxCascadeDepth)
                    continue;

                foreach (var relation in mapping.Relations.Values)
                {
                    if (!relation.CascadeDelete)
                        continue;
                    object? navValue;
                    try
                    {
                        navValue = relation.NavProp.GetValue(entity);
                    }
                    catch (TargetInvocationException ex)
                    {
                        // The getter itself threw - log and skip this navigation property
                        _options.Logger?.LogWarning(ex,
                            "Failed to read navigation property {NavProp} on entity type {EntityType} during cascade delete. Skipping this relation.",
                            relation.NavProp.Name, entity.GetType().Name);
                        continue;
                    }
                    catch (MemberAccessException ex)
                    {
                        // Access denied (e.g., security restriction) - log and skip
                        _options.Logger?.LogWarning(ex,
                            "Access denied reading navigation property {NavProp} on entity type {EntityType} during cascade delete. Skipping this relation.",
                            relation.NavProp.Name, entity.GetType().Name);
                        continue;
                    }
                    if (navValue is IEnumerable collection && navValue is not string)
                    {
                        // Wrap enumeration in try/catch: collections may throw on GetEnumerator()
                        // or MoveNext() (e.g. disposed lazy-load proxy, collection requiring DB access).
                        // Partial traversal is safer than aborting cascade entirely.
                        IEnumerator? enumerator = null;
                        try
                        {
                            enumerator = collection.GetEnumerator();
                            while (enumerator.MoveNext())
                            {
                                var child = enumerator.Current;
                                if (child == null)
                                    continue;

                                // Skip already-visited entities to handle circular references gracefully
                                if (!visited.Add(child))
                                    continue;

                                if (_entriesByReference.TryGetValue(child, out var childEntry))
                                {
                                    // Only cascade-detach a child that genuinely belongs to this
                                    // principal: its FK still references the principal, or it is Added
                                    // (never persisted, linked only by navigation). A persisted child
                                    // re-parented by a deliberate FK edit can linger in this now-stale
                                    // collection; detaching it would silently strip its change tracking
                                    // and drop any later mutation.
                                    var belongs = childEntry.State == EntityState.Added;
                                    if (!belongs)
                                    {
                                        belongs = true;
                                        for (var i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
                                        {
                                            if (!Equals(relation.ForeignKeys[i].Getter(child), relation.PrincipalKeys[i].Getter(entity)))
                                            {
                                                belongs = false;
                                                break;
                                            }
                                        }
                                    }
                                    if (belongs)
                                        queue.Enqueue((child, childEntry.Mapping, depth + 1));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _options.Logger?.LogWarning(ex,
                                "Failed to enumerate collection navigation property {NavProp} on entity type {EntityType} during cascade delete. Skipping this collection.",
                                relation.NavProp.Name, entity.GetType().Name);
                        }
                        finally
                        {
                            (enumerator as IDisposable)?.Dispose();
                        }
                    }
                    else if (navValue != null)
                    {
                        // Skip already-visited entities to handle circular/bidirectional references gracefully.
                        // Bidirectional 1:N relationships (Parent → Child.Parent back-ref) commonly produce
                        // cycles; throwing here would break legitimate entity graphs. Log at debug level so
                        // the traversal decision is visible without polluting production logs.
                        if (!visited.Add(navValue))
                        {
                            _options.Logger?.LogDebug(
                                "Cycle detected in cascade delete at depth {Depth}: entity type {EntityType} already visited. Skipping.",
                                depth, navValue.GetType().Name);
                            continue;
                        }

                        if (_entriesByReference.TryGetValue(navValue, out var childEntry))
                        {
                            queue.Enqueue((navValue, childEntry.Mapping, depth + 1));
                        }
                    }
                }
            }

            // Phase 2: Remove all collected entities. Uses RemoveUnlocked because the
            // caller (Remove) already holds _trackLock.
            foreach (var entityToRemove in toRemove)
            {
                RemoveUnlocked(entityToRemove);
            }
        }

        /// <summary>
        /// Internal removal that does NOT acquire <see cref="_trackLock"/>. Used by
        /// <see cref="CascadeDelete"/> which is called while the lock is already held.
        /// </summary>
        private void RemoveUnlocked(object entity)
        {
            if (_entriesByReference.TryRemove(entity, out var entry))
            {
                entry.DetachEntity();
                _nonNotifyingEntries.TryRemove(entry, out _);
                _dirtyNonNotifyingEntries.TryRemove(entry, out _);
                _dirtyEntries.TryRemove(entry, out _);
                var pk = entry.OriginalKey != null
                    ? ToLookupKey(entry.OriginalKey, entry.Mapping)
                    : GetPrimaryKeyValue(entity, entry.Mapping);
                if (pk != null && _entriesByKey.TryGetValue(entry.Mapping.Type, out var typeEntries))
                {
                    typeEntries.TryRemove(pk, out _);
                    if (typeEntries.IsEmpty)
                        _entriesByKey.TryRemove(entry.Mapping.Type, out _);
                }
            }
        }

        /// <summary>
        /// Gets an enumeration of the <see cref="EntityEntry"/> instances currently
        /// tracked by the context.
        /// </summary>
        /// <remarks>
        /// Returns a snapshot of the underlying collection. Assigning <see cref="EntityEntry.State"/>
        /// on a returned entry performs the corresponding tracker transition (the same as the
        /// <see cref="DbContext"/> Add/Update/Remove/Attach APIs), so it is safe to drive state from here.
        /// </remarks>
        // Ordered by attach sequence so SaveChanges batches (and autoincrement key assignment)
        // are deterministic in Add/Attach order — ConcurrentDictionary enumeration order is
        // identity-hash based and varies between runs.
        public IEnumerable<EntityEntry> Entries => _entriesByReference.Values.OrderBy(e => e.AttachSequence);

        /// <summary>
        /// A human-readable snapshot of the tracked entities for debugging (EF Core
        /// <c>ChangeTracker.DebugView</c> parity): <c>DebugView.ShortView</c> lists one line per entity
        /// (type, key, state) and <c>DebugView.LongView</c> adds each property's value. Reflects the tracker's
        /// current state; call <see cref="DetectChanges()"/> first to refresh states against pending edits.
        /// </summary>
        public DebugView DebugView => new(this);

        /// <summary>
        /// Returns the tracked <see cref="EntityEntry"/> for the given entity instance, or null if not tracked.
        /// </summary>
        internal EntityEntry? GetEntryOrDefault(object entity) =>
            _entriesByReference.TryGetValue(entity, out var entry) ? entry : null;

        /// <summary>
        /// Returns the tracked entry for the given entity type and primary key value,
        /// or null. Relationship fixup uses this to re-point a stale navigation at
        /// the principal a deliberately edited FK now references.
        /// </summary>
        internal EntityEntry? GetEntryByKey(Type entityType, object key) =>
            _entriesByKey.TryGetValue(entityType, out var typeEntries)
                && typeEntries.TryGetValue(key, out var entry)
                ? entry
                : null;

        /// <summary>
        /// Builds the identity-map lookup key from explicit key values (as supplied to
        /// <c>FindAsync</c>), producing the exact shape <see cref="GetPrimaryKeyValue"/> stores
        /// from a tracked entity: the raw boxed value for a single key, a <see cref="ValueTuple"/>
        /// for two or three columns, and a <see cref="CompositeKey"/> beyond that. Returns null
        /// when the arity does not match or any component is null (a null component can never
        /// identify a tracked row), signalling the caller to skip the identity-map short circuit.
        /// </summary>
        internal static object? BuildLookupKey(TableMapping mapping, IReadOnlyList<object?> keyValues)
        {
            if (keyValues.Count != mapping.KeyColumns.Length)
                return null;
            for (var i = 0; i < keyValues.Count; i++)
                if (keyValues[i] is null)
                    return null;

            switch (mapping.KeyColumns.Length)
            {
                case 1:
                    return keyValues[0];
                case 2:
                    return (keyValues[0], keyValues[1]);
                case 3:
                    return (keyValues[0], keyValues[1], keyValues[2]);
                default:
                    var values = new object?[keyValues.Count];
                    for (var i = 0; i < keyValues.Count; i++)
                        values[i] = keyValues[i];
                    return new CompositeKey(values);
            }
        }

        /// <summary>
        /// Forces detection of changes made to tracked entities since they were loaded or last saved,
        /// updating each entry's <see cref="EntityEntry.State"/> — matching EF Core's
        /// <c>ChangeTracker.DetectChanges()</c>. SaveChanges runs this automatically; call it explicitly
        /// only when you need up-to-date states before then (e.g. before <see cref="HasChanges"/> or
        /// enumerating <see cref="Entries"/>).
        /// </summary>
        public void DetectChanges()
            => DetectChangesCore(allNonNotifying: true);

        /// <summary>
        /// Targeted detection that scans only entities explicitly marked dirty via <see cref="MarkDirty"/>
        /// (plus INPC entities), rather than every tracked entity. Not part of the public EF-style surface;
        /// exercised directly by change-tracking tests.
        /// </summary>
        internal void DetectChangesDirtyOnly()
            => DetectChangesCore(allNonNotifying: false);

        /// <summary>
        /// Runs change detection and returns whether any tracked entity is pending insert, update, or
        /// delete — matching EF Core's <c>ChangeTracker.HasChanges()</c>. Use it to skip a no-op
        /// <c>SaveChanges</c>.
        /// </summary>
        public bool HasChanges()
        {
            DetectChangesCore(allNonNotifying: true);
            foreach (var entry in _entriesByReference.Values)
                if (entry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
                    return true;
            return false;
        }

        /// <summary>
        /// Accepts all changes: after running change detection, every Added or Modified entry becomes
        /// <see cref="EntityState.Unchanged"/> with its current values as the new baseline, and every Deleted
        /// entry is detached — matching EF Core's <c>ChangeTracker.AcceptAllChanges</c>. Use it to tell the
        /// tracker that the tracked entities now mirror the database after persisting them out of band (for
        /// example via raw SQL). Each entry is processed by its own state, without cascading.
        /// </summary>
        public void AcceptAllChanges()
        {
            DetectChangesCore(allNonNotifying: true);
            // Snapshot the entries: detaching a Deleted entry mutates the underlying tracked collection.
            foreach (var entry in Entries.ToList())
            {
                switch (entry.State)
                {
                    case EntityState.Deleted:
                        if (entry.Entity is { } deleted)
                            Remove(deleted, cascade: false);
                        break;
                    case EntityState.Added:
                    case EntityState.Modified:
                        entry.AcceptChanges();
                        break;
                }
            }
        }

        /// <summary>
        /// Detects changes in all tracked non-INotifyPropertyChanged entities by
        /// comparing current property values against original snapshots, plus any
        /// INPC entities that were explicitly marked dirty. Called automatically by
        /// SaveChanges.
        /// </summary>
        /// <remarks>
        /// This method performs snapshot-based comparison of all non-INPC tracked
        /// entities, iterating through all properties and comparing current values
        /// against original snapshots. This is O(entities x properties) complexity.
        /// Entities implementing <see cref="System.ComponentModel.INotifyPropertyChanged"/>
        /// are NOT scanned here unless they were explicitly marked dirty - their
        /// changes are tracked incrementally via property-change events.
        ///
        /// Performance considerations:
        /// - Avoid calling SaveChanges() in tight loops with many tracked entities
        /// - Consider using batch operations (BulkInsertAsync, BulkUpdateAsync) for bulk changes
        /// - For read-only queries, use AsNoTracking() to avoid change tracking overhead
        /// - If tracking thousands of entities, consider periodically detaching unchanged entities
        /// </remarks>
        internal void DetectAllChanges()
            => DetectChangesCore(allNonNotifying: true);

        private void DetectChangesCore(bool allNonNotifying)
        {
            List<(object Entity, Exception Exception)>? failures = null;
            var source = allNonNotifying ? _nonNotifyingEntries.Keys : (IEnumerable<EntityEntry>)_dirtyNonNotifyingEntries.Keys;

            foreach (var entry in source)
            {
                var entity = entry.Entity;
                if (entity != null)
                {
                    try { entry.DetectChanges(); }
                    catch (Exception ex)
                    {
                        failures ??= new List<(object Entity, Exception Exception)>();
                        failures.Add((entity, ex));
                        _options.Logger?.LogError(ex,
                            "Error detecting changes for entity {EntityType}. Entity will be skipped for this SaveChanges operation.",
                            entity.GetType().Name);
                    }
                }
                else
                {
                    // Entry has been detached without going through Remove() — evict from stale sets
                    // so they are not visited on every future DetectAllChanges cycle.
                    _nonNotifyingEntries.TryRemove(entry, out _);
                    _dirtyNonNotifyingEntries.TryRemove(entry, out _);
                }
            }

            foreach (var entry in _dirtyEntries.Keys)
            {
                var entity = entry.Entity;
                if (entity != null)
                {
                    try { entry.DetectChanges(); }
                    catch (Exception ex)
                    {
                        failures ??= new List<(object Entity, Exception Exception)>();
                        failures.Add((entity, ex));
                        _options.Logger?.LogError(ex,
                            "Error detecting changes for entity {EntityType}. Entity will be skipped for this SaveChanges operation.",
                            entity.GetType().Name);
                    }
                }
                else
                {
                    // Entry has been detached without going through Remove() — evict from dirty set.
                    _dirtyEntries.TryRemove(entry, out _);
                }
            }

            _dirtyNonNotifyingEntries.Clear();
            _dirtyEntries.Clear();

            if (failures is { Count: > 0 })
            {
                throw new AggregateException(
                    "DetectChanges encountered errors.",
                    failures.ConvertAll(f => f.Exception));
            }
        }

        /// <summary>
        /// Marks the specified <see cref="EntityEntry"/> as requiring change detection
        /// on the next call to <see cref="DetectChanges"/>. Non-notifying entities are
        /// tracked separately to ensure their changes are discovered.
        /// </summary>
        /// <param name="entry">The entry to mark as dirty.</param>
        /// <remarks>
        /// Thread-safety assumption: This method uses ConcurrentDictionary.TryAdd which
        /// is individually thread-safe, but the ContainsKey-then-TryAdd sequence is NOT
        /// atomic. A concurrent Attach could add the entry to _nonNotifyingEntries after
        /// the ContainsKey check, causing the entry to be routed to _dirtyEntries instead.
        /// This is acceptable because DetectChangesCore processes BOTH collections, so
        /// the entry will still be detected - it may just take a full DetectAllChanges
        /// cycle rather than a targeted DetectChanges cycle.
        /// </remarks>
        internal void MarkDirty(EntityEntry entry)
        {
            ArgumentNullException.ThrowIfNull(entry);
            entry.UpgradeToFullTracking();
            if (_nonNotifyingEntries.ContainsKey(entry))
            {
                _dirtyNonNotifyingEntries.TryAdd(entry, 0);
            }
            else
            {
                _dirtyEntries.TryAdd(entry, 0);
            }
        }

        /// <summary>
        /// Removes all tracked entity entries and resets the change tracker to an empty state.
        /// </summary>
        /// <remarks>
        /// This method is not synchronized with <see cref="_trackLock"/>. Since
        /// <see cref="DbContext"/> is single-threaded by design, callers must not invoke
        /// <see cref="Clear"/> concurrently with <see cref="Track"/> or <see cref="Remove"/>.
        /// </remarks>
        public void Clear()
        {
            _entriesByReference.Clear();
            _entriesByKey.Clear();
            _nonNotifyingEntries.Clear();
            _dirtyNonNotifyingEntries.Clear();
            _dirtyEntries.Clear();
        }

        /// <summary>
        /// Returns <c>true</c> when <paramref name="value"/> represents the CLR default
        /// for common DB-generated key types (0 for integer types, <see cref="Guid.Empty"/>
        /// for GUIDs, <c>null</c> for any type).
        /// </summary>
        internal static bool IsDefaultKeyValue(object? value, Type type)
        {
            if (value is null) return true;
            var underlying = Nullable.GetUnderlyingType(type) ?? type;
            if (underlying == typeof(int)   || underlying == typeof(long)  ||
                underlying == typeof(short) || underlying == typeof(byte) ||
                underlying == typeof(uint)  || underlying == typeof(sbyte) ||
                underlying == typeof(ushort))
                return Convert.ToInt64(value) == 0L;
            // Use Convert.ToUInt64 rather than a direct (ulong) cast: the value is boxed as its
            // actual runtime type (e.g. boxed uint, boxed ulong), and an unbox cast to ulong fails
            // with InvalidCastException when the boxed type differs from ulong.
            if (underlying == typeof(ulong))
                return Convert.ToUInt64(value) == 0UL;
            if (underlying == typeof(Guid))
                return (Guid)value == Guid.Empty;
            return false;
        }

        /// <summary>
        /// After a DB-generated key has been assigned to an entity (post-INSERT),
        /// adds the entity to the key-based identity map so subsequent lookups by PK
        /// find the correct entry rather than creating a duplicate.
        /// </summary>
        /// <param name="entity">The entity whose key was just assigned by the database.</param>
        /// <param name="mapping">Mapping information for the entity type.</param>
        /// <exception cref="ArgumentNullException"><paramref name="entity"/> or <paramref name="mapping"/> is <c>null</c>.</exception>
        internal void ReindexAfterInsert(object entity, TableMapping mapping)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(mapping);
            if (!_entriesByReference.TryGetValue(entity, out var entry)) return;
            var pk = GetPrimaryKeyValue(entity, mapping);
            if (pk == null) return;
            var typeDict = _entriesByKey.GetOrAdd(mapping.Type,
                _ => new ConcurrentDictionary<object, EntityEntry>());
            typeDict.TryAdd(pk, entry);
            PropagateGeneratedKeyToChildren(entity, mapping);
            ResolvePendingReferenceKeyFixups(entity);
        }

        // Dependents whose reference navigation points at a principal with a still-default
        // DB-generated key. The collection direction walks the principal's collections in
        // PropagateGeneratedKeyToChildren; a reference navigation has no back-collection to
        // walk, so the pair is recorded at fixup time and resolved when the principal's
        // INSERT hydrates its key.
        private List<(object Principal, object Dependent, Column ForeignKey, Column PrincipalKey)>? _pendingReferenceKeyFixups;

        /// <summary>
        /// Defers a dependent's FK assignment until the referenced principal's DB-generated
        /// key is hydrated by its INSERT (see <see cref="ResolvePendingReferenceKeyFixups"/>).
        /// </summary>
        internal void RegisterPendingReferenceKeyFixup(object principal, object dependent, Column foreignKey, Column principalKey)
            => (_pendingReferenceKeyFixups ??= new()).Add((principal, dependent, foreignKey, principalKey));

        /// <summary>Drops all deferred FK assignments; called when relationship fixup restarts and after a save completes.</summary>
        internal void ClearPendingReferenceKeyFixups() => _pendingReferenceKeyFixups = null;

        private void ResolvePendingReferenceKeyFixups(object insertedPrincipal)
        {
            var pending = _pendingReferenceKeyFixups;
            if (pending == null || pending.Count == 0)
                return;
            for (int i = pending.Count - 1; i >= 0; i--)
            {
                var (principal, dependent, foreignKey, principalKey) = pending[i];
                if (!ReferenceEquals(principal, insertedPrincipal))
                    continue;
                foreignKey.Setter(dependent, principalKey.Getter(insertedPrincipal));
                pending.RemoveAt(i);
            }
        }

        /// <summary>
        /// Reverses a <see cref="ReindexAfterInsert"/> that ran during a SaveChanges attempt which
        /// was subsequently rolled back. Removes the now-stale key-based identity-map index created
        /// for the rolled-back key and restores the entity's pre-insert key column values (default
        /// for a first attempt). Without this, the retry sees a non-default DB-generated key, the
        /// "skip already-inserted" guard treats the entity as persisted, and the row is silently
        /// dropped even though its INSERT was rolled back.
        /// </summary>
        /// <param name="entity">The Added entity whose key was assigned by the failed attempt.</param>
        /// <param name="mapping">Mapping information for the entity type.</param>
        /// <param name="originalKeyValues">The key column values captured before the attempt ran.</param>
        internal void RollbackGeneratedKeyAssignment(object entity, TableMapping mapping, object?[] originalKeyValues)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(originalKeyValues);

            // Drop the identity-map entry keyed by the rolled-back (non-default) key so a later
            // lookup does not resolve to an entity whose row no longer exists.
            var currentPk = GetPrimaryKeyValue(entity, mapping);
            if (currentPk != null && _entriesByKey.TryGetValue(mapping.Type, out var typeDict))
                typeDict.TryRemove(currentPk, out _);

            // Restore the pre-attempt key values so the retry re-inserts the row.
            for (int i = 0; i < mapping.KeyColumns.Length && i < originalKeyValues.Length; i++)
                mapping.KeyColumns[i].Setter(entity, originalKeyValues[i]);
        }

        /// <summary>
        /// After a principal with a DB-generated key is inserted and its key hydrated, updates the
        /// foreign key of every still-Added dependent reachable through the principal's collection
        /// navigations to the newly generated key value. Relationship fixup (see
        /// DbContext.FixupNavigationChildren) sets a child's FK from the principal's PK at
        /// change-detection time, but a DB-generated principal key is still default then; this closes
        /// the gap so a whole new object graph saved in one call links up correctly.
        /// </summary>
        internal void PropagateGeneratedKeyToChildren(object principal, TableMapping mapping)
        {
            if (mapping.Relations.Count == 0)
                return;

            foreach (var relation in mapping.Relations.Values)
            {
                if (relation.NavProp.GetValue(principal) is not System.Collections.IEnumerable collection || collection is string)
                    continue;

                foreach (var child in collection)
                {
                    if (child == null)
                        continue;
                    var childEntry = GetEntryOrDefault(child);
                    if (childEntry == null || childEntry.State != EntityState.Added)
                        continue;
                    for (int i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
                        relation.ForeignKeys[i].Setter(child, relation.PrincipalKeys[i].Getter(principal));
                }
            }
        }

        /// <summary>
        /// Extracts the primary key value for the given entity using the provided mapping.
        /// Supports both single-column keys and composite keys.
        /// </summary>
        /// <param name="entity">The entity instance from which to read the key.</param>
        /// <param name="mapping">Mapping information describing the key columns.</param>
        /// <returns>
        /// The key value, a composite key object when multiple key columns exist, or
        /// <c>null</c> if the entity type does not define a primary key or a DB-generated
        /// key column is still at its default value (not yet assigned by the database).
        /// </returns>
        /// <remarks>
        /// Composite key default detection uses OR semantics: if ANY DB-generated column
        /// in the composite key is at its default value (0 for int, Guid.Empty for Guid,
        /// etc.), the entire key is treated as unassigned (returns null). This prevents
        /// identity map collisions when multiple new entities share partially-assigned
        /// composite keys. The trade-off is that a legitimately assigned composite key
        /// where one DB-generated column happens to hold its type's default value will
        /// also be treated as unassigned - but this is vanishingly rare in practice since
        /// DB-generated values (IDENTITY, NEWID) never produce defaults.
        /// </remarks>
        private static object? GetPrimaryKeyValue(object entity, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 1)
            {
                var col   = mapping.KeyColumns[0];
                var value = col.Getter(entity);
                // DB-generated key at its default means "not yet assigned" - don't key the map
                if (col.IsDbGenerated && IsDefaultKeyValue(value, col.Prop.PropertyType))
                    return null;
                return value;
            }

            if (mapping.KeyColumns.Length == 2)
            {
                var col0 = mapping.KeyColumns[0]; var col1 = mapping.KeyColumns[1];
                var v0 = col0.Getter(entity);     var v1 = col1.Getter(entity);
                // Unassigned DB-generated composite key: treat as null (not yet in identity map).
                if ((col0.IsDbGenerated && IsDefaultKeyValue(v0, col0.Prop.PropertyType)) ||
                    (col1.IsDbGenerated && IsDefaultKeyValue(v1, col1.Prop.PropertyType)))
                    return null;
                return (v0, v1);
            }

            if (mapping.KeyColumns.Length == 3)
            {
                var col0 = mapping.KeyColumns[0]; var col1 = mapping.KeyColumns[1]; var col2 = mapping.KeyColumns[2];
                var v0 = col0.Getter(entity);     var v1 = col1.Getter(entity);     var v2 = col2.Getter(entity);
                // Unassigned DB-generated composite key: treat as null (not yet in identity map).
                if ((col0.IsDbGenerated && IsDefaultKeyValue(v0, col0.Prop.PropertyType)) ||
                    (col1.IsDbGenerated && IsDefaultKeyValue(v1, col1.Prop.PropertyType)) ||
                    (col2.IsDbGenerated && IsDefaultKeyValue(v2, col2.Prop.PropertyType)))
                    return null;
                return (v0, v1, v2);
            }

            if (mapping.KeyColumns.Length > 3)
            {
                // Fallback to array for larger composite keys (rare)
                var values = new object?[mapping.KeyColumns.Length];
                for (int i = 0; i < mapping.KeyColumns.Length; i++)
                {
                    var col = mapping.KeyColumns[i];
                    var v = col.Getter(entity);
                    // Unassigned DB-generated key component: entity not yet in identity map.
                    if (col.IsDbGenerated && IsDefaultKeyValue(v, col.Prop.PropertyType))
                        return null;
                    values[i] = v;
                }
                return new CompositeKey(values);
            }

            return null;
        }

        /// <summary>
        /// Represents a composite primary key composed of more than three columns.
        /// Provides structural equality and a stable hash code for use as a dictionary key.
        /// </summary>
        private sealed class CompositeKey : IEquatable<CompositeKey>
        {
            private readonly object?[] _values;
            private readonly int _cachedHashCode;

            public CompositeKey(object?[] values)
            {
                ArgumentNullException.ThrowIfNull(values);
                _values = values;
                _cachedHashCode = ComputeHashCode(values);
            }

            /// <summary>
            /// Determines whether this composite key is equal to another composite key instance.
            /// </summary>
            /// <param name="other">The other <see cref="CompositeKey"/> to compare with.</param>
            /// <returns><c>true</c> if both keys contain equivalent values in the same order; otherwise, <c>false</c>.</returns>
            public bool Equals(CompositeKey? other)
            {
                if (other is null || other._values.Length != _values.Length)
                    return false;
                for (int i = 0; i < _values.Length; i++)
                {
                    if (!Equals(_values[i], other._values[i]))
                        return false;
                }
                return true;
            }

            /// <summary>
            /// Determines whether the specified object is equal to the current composite key.
            /// </summary>
            /// <param name="obj">The object to compare with this key.</param>
            /// <returns><c>true</c> if <paramref name="obj"/> is a <see cref="CompositeKey"/> with the same values; otherwise, <c>false</c>.</returns>
            public override bool Equals(object? obj) => Equals(obj as CompositeKey);

            /// <summary>
            /// Returns the cached hash code computed at construction time.
            /// </summary>
            /// <returns>An integer hash code representing the composite key.</returns>
            public override int GetHashCode() => _cachedHashCode;

            private static int ComputeHashCode(object?[] values)
            {
                unchecked
                {
                    var hash = HashSeed;
                    foreach (var value in values)
                    {
                        hash = hash * HashMultiplier + (value?.GetHashCode() ?? 0);
                    }
                    return hash;
                }
            }

            /// <summary>
            /// Returns a human-readable representation of the composite key for diagnostics.
            /// </summary>
            public override string ToString()
                => $"CompositeKey({string.Join(", ", _values)})";
        }
    }
}
