using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Linq;
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

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeTracker"/> class using the
        /// specified context options.
        /// </summary>
        /// <param name="options">Options that influence change-tracking behavior.</param>
        public ChangeTracker(DbContextOptions options)
        {
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
        internal EntityEntry Track(object entity, EntityState state, TableMapping mapping)
        {
            // Fast path: entity already tracked by reference
            if (_entriesByReference.TryGetValue(entity, out var existingEntry))
            {
                existingEntry.State = state;
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
            // If not found by reference, check by primary key
            if (pk != null && _entriesByKey.TryGetValue(mapping.Type, out var existingTypeEntries) &&
                existingTypeEntries.TryGetValue(pk, out var existing))
            {
                existing.State = state;
                return existing;
            }
            // Create new entry only when needed
            var entry = state == EntityState.Unchanged && !_options.EagerChangeTracking
                ? CreateLazyEntry(entity, mapping)
                : new EntityEntry(entity, state, mapping, _options, MarkDirty);
            if (_entriesByReference.TryAdd(entity, entry))
            {
                // Successfully added - set up additional tracking
                if (entity is not INotifyPropertyChanged)
                    _nonNotifyingEntries.TryAdd(entry, 0);
                if (pk != null)
                {
                    var typeEntries = _entriesByKey.GetOrAdd(
                        mapping.Type,
                        static _ => new ConcurrentDictionary<object, EntityEntry>());
                    typeEntries.TryAdd(pk, entry);
                }
                return entry;
            }
            // Another thread added it between check and add
            if (_entriesByReference.TryGetValue(entity, out var raceEntry))
            {
                raceEntry.State = state;
                return raceEntry;
            }
            // Fallback: return the one we created
            return entry;
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
        private const int MaxCascadeDepth = 100;
        /// <summary>
        /// Removes an entity from the change tracker, optionally cascading the removal
        /// to related entities that are configured for cascade delete.
        /// </summary>
        /// <param name="entity">The entity instance to stop tracking.</param>
        /// <param name="cascade">If <c>true</c>, related entities configured with cascade
        /// delete will also be detached.</param>
        internal void Remove(object entity, bool cascade = false)
        {
            if (_entriesByReference.TryRemove(entity, out var entry))
            {
                entry.DetachEntity();
                _nonNotifyingEntries.TryRemove(entry, out _);
                _dirtyNonNotifyingEntries.TryRemove(entry, out _);
                _dirtyEntries.TryRemove(entry, out _);
                var pk = GetPrimaryKeyValue(entity, entry.Mapping);
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

        /// <summary>
        /// Recursively traverses the entity graph starting from
        /// <paramref name="rootEntity"/> and detaches any dependent entities marked
        /// for cascade deletion. This ensures that related entities do not remain
        /// tracked when their parent is removed.
        /// </summary>
        /// <param name="rootEntity">The root entity being deleted.</param>
        /// <param name="rootMapping">Mapping information for the root entity.</param>
        private void CascadeDelete(object rootEntity, TableMapping rootMapping)
        {
            var queue = new Queue<(object Entity, TableMapping Mapping, int Depth)>();
            var visited = new HashSet<object>(RefComparer.Instance);
            queue.Enqueue((rootEntity, rootMapping, 0));
            visited.Add(rootEntity);
            while (queue.Count > 0)
            {
                var (entity, mapping, depth) = queue.Dequeue();
                if (depth >= MaxCascadeDepth)
                    continue;
                foreach (var relation in mapping.Relations.Values)
                {
                    if (!relation.CascadeDelete)
                        continue;
                    var navValue = relation.NavProp.GetValue(entity);
                    if (navValue is IEnumerable collection)
                    {
                        foreach (var child in collection)
                        {
                            if (child != null && visited.Add(child) &&
                                _entriesByReference.TryGetValue(child, out var childEntry))
                            {
                                queue.Enqueue((child, childEntry.Mapping, depth + 1));
                                Remove(child, false);
                            }
                        }
                    }
                    else if (navValue != null && visited.Add(navValue) &&
                             _entriesByReference.TryGetValue(navValue, out var childEntry))
                    {
                        queue.Enqueue((navValue, childEntry.Mapping, depth + 1));
                        Remove(navValue, false);
                    }
                }
            }
        }
        /// <summary>
        /// Gets an enumeration of the <see cref="EntityEntry"/> instances currently
        /// tracked by the context.
        /// </summary>
        public IEnumerable<EntityEntry> Entries => _entriesByReference.Values;
        /// <summary>
        /// Forces change detection for all entities that have been marked as dirty,
        /// updating their <see cref="EntityState"/> based on current property values.
        /// </summary>
        /// <remarks>
        /// PERFORMANCE WARNING (TASK 12): This method is called automatically on every SaveChanges() call.
        /// It performs snapshot-based comparison of ALL tracked entities, iterating through all properties
        /// and comparing current values against original snapshots. This is O(entities × properties) complexity.
        ///
        /// Performance considerations:
        /// - Avoid calling SaveChanges() in tight loops with many tracked entities
        /// - Consider using batch operations (InsertBulkAsync, UpdateBulkAsync) for bulk changes
        /// - For read-only queries, use AsNoTracking() to avoid change tracking overhead
        /// - If tracking thousands of entities, consider periodically detaching unchanged entities
        ///
        /// This design matches Entity Framework Core semantics where change detection is snapshot-based
        /// and happens automatically. For entities implementing INotifyPropertyChanged, the overhead is
        /// reduced as changes are tracked incrementally.
        /// </remarks>
        internal void DetectChanges()
        {
            var dirtyNonNotifyingSnapshot = _dirtyNonNotifyingEntries.Keys.ToArray();
            foreach (var entry in dirtyNonNotifyingSnapshot)
            {
                if (entry.Entity != null)
                    entry.DetectChanges();
            }
            _dirtyNonNotifyingEntries.Clear();
            var dirtySnapshot = _dirtyEntries.Keys.ToArray();
            foreach (var entry in dirtySnapshot)
            {
                if (entry.Entity != null)
                    entry.DetectChanges();
            }
            _dirtyEntries.Clear();
        }

        /// <summary>
        /// Marks the specified <see cref="EntityEntry"/> as requiring change detection
        /// on the next call to <see cref="DetectChanges"/>. Non-notifying entities are
        /// tracked separately to ensure their changes are discovered.
        /// </summary>
        /// <param name="entry">The entry to mark as dirty.</param>
        internal void MarkDirty(EntityEntry entry)
        {
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
        public void Clear()
        {
            _entriesByReference.Clear();
            _entriesByKey.Clear();
            _nonNotifyingEntries.Clear();
            _dirtyNonNotifyingEntries.Clear();
            _dirtyEntries.Clear();
        }
        /// <summary>
        /// Extracts the primary key value for the given entity using the provided mapping.
        /// Supports both single-column keys and composite keys.
        /// </summary>
        /// <param name="entity">The entity instance from which to read the key.</param>
        /// <param name="mapping">Mapping information describing the key columns.</param>
        /// <returns>
        /// The key value, a composite key object when multiple key columns exist, or
        /// <c>null</c> if the entity type does not define a primary key.
        /// </returns>
        private static object? GetPrimaryKeyValue(object entity, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 1)
                return mapping.KeyColumns[0].Getter(entity);
            if (mapping.KeyColumns.Length > 1)
            {
                var values = new object?[mapping.KeyColumns.Length];
                for (int i = 0; i < mapping.KeyColumns.Length; i++)
                {
                    values[i] = mapping.KeyColumns[i].Getter(entity);
                }
                return new CompositeKey(values);
            }
            return null;
        }
        private sealed class CompositeKey : IEquatable<CompositeKey>
        {
            private readonly object?[] _values;
            public CompositeKey(object?[] values)
            {
                _values = values;
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
            /// Computes a hash code based on the contained key values.
            /// </summary>
            /// <returns>An integer hash code representing the composite key.</returns>
            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = 17;
                    foreach (var value in _values)
                    {
                        hash = hash * 23 + (value?.GetHashCode() ?? 0);
                    }
                    return hash;
                }
            }
        }
    }
}