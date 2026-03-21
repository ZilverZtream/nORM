using System;
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
    public sealed class ChangeTracker
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
                        existingEntry.State = state;
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
                            : new EntityEntry(entity, state, mapping, _options, MarkDirty);

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
                        // Different CLR instance with same PK — update state but don't downgrade
                        // from a dirty state to Unchanged.
                        if (!(pkEntry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted
                              && state == EntityState.Unchanged))
                        {
                            pkEntry.State = state;
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
                    : new EntityEntry(entity, state, mapping, _options, MarkDirty);

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
                        raceEntry.State = state;
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
            return new EntityEntry(entity, EntityState.Unchanged, mapping, _options, MarkDirty, lazy: true);
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
        /// Returns a snapshot of the underlying collection. The <see cref="EntityEntry"/>
        /// objects themselves are mutable; callers should not modify entry state (such as
        /// <see cref="EntityEntry.State"/>) directly, as this bypasses change-tracker
        /// invariants. Use <see cref="DbContext"/> APIs to transition entity state.
        /// </remarks>
        public IEnumerable<EntityEntry> Entries => _entriesByReference.Values;

        /// <summary>
        /// Returns the tracked <see cref="EntityEntry"/> for the given entity instance, or null if not tracked.
        /// </summary>
        internal EntityEntry? GetEntryOrDefault(object entity) =>
            _entriesByReference.TryGetValue(entity, out var entry) ? entry : null;

        /// <summary>
        /// Detects changes only for entities that were explicitly marked dirty via
        /// <see cref="MarkDirty"/>. For snapshot-based detection of all non-INPC entities,
        /// use <see cref="DetectAllChanges"/> (called internally from SaveChanges).
        /// </summary>
        internal void DetectChanges()
            => DetectChangesCore(allNonNotifying: false);

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
        private static bool IsDefaultKeyValue(object? value, Type type)
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
