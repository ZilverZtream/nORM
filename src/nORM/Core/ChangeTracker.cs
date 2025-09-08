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
    public sealed class ChangeTracker
    {
        private readonly ConcurrentDictionary<object, EntityEntry> _entriesByReference = new(RefComparer.Instance);
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, EntityEntry>> _entriesByKey = new();
        private readonly ConcurrentDictionary<EntityEntry, byte> _nonNotifyingEntries = new();
        private readonly ConcurrentDictionary<EntityEntry, byte> _dirtyEntries = new();
        private readonly DbContextOptions _options;

        public ChangeTracker(DbContextOptions options)
        {
            _options = options;
        }

        internal EntityEntry Track(object entity, EntityState state, TableMapping mapping)
        {
            var pk = GetPrimaryKeyValue(entity, mapping);
            if (pk != null && _entriesByKey.TryGetValue(mapping.Type, out var existingTypeEntries) &&
                existingTypeEntries.TryGetValue(pk, out var existing))
            {
                existing.State = state;
                return existing;
            }

            // ADD LAZY TRACKING CHECK
            if (state == EntityState.Unchanged && !_options.EagerChangeTracking)
            {
                var lazy = CreateLazyEntry(entity, mapping);
                _entriesByReference[entity] = lazy;
                if (entity is not INotifyPropertyChanged)
                    _nonNotifyingEntries[lazy] = 0;
                if (pk != null)
                {
                    var typeEntries = _entriesByKey.GetOrAdd(
                        mapping.Type,
                        _ => new ConcurrentDictionary<object, EntityEntry>());
                    typeEntries[pk] = lazy;
                }

                return lazy;
            }

            var entry = new EntityEntry(entity, state, mapping, _options, MarkDirty);
            _entriesByReference[entity] = entry;
            if (entity is not INotifyPropertyChanged)
                _nonNotifyingEntries[entry] = 0;
            if (pk != null)
            {
                var typeEntries = _entriesByKey.GetOrAdd(
                    mapping.Type,
                    _ => new ConcurrentDictionary<object, EntityEntry>());
                typeEntries[pk] = entry;
            }

            return entry;
        }

        private EntityEntry CreateLazyEntry(object entity, TableMapping mapping)
        {
            // Minimal entry that defers property change setup
            return new EntityEntry(entity, EntityState.Unchanged, mapping, _options, MarkDirty, lazy: true);
        }

        private const int MaxCascadeDepth = 100;

        internal void Remove(object entity, bool cascade = false)
        {
            if (_entriesByReference.TryRemove(entity, out var entry))
            {
                entry.DetachEntity();
                _nonNotifyingEntries.TryRemove(entry, out _);
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

        public IEnumerable<EntityEntry> Entries => _entriesByReference.Values;

        internal void DetectChanges()
        {
            foreach (var kvp in _nonNotifyingEntries)
            {
                if (kvp.Value == 0)
                    continue;

                var entry = kvp.Key;
                if (entry.Entity != null)
                    entry.DetectChanges();

                _nonNotifyingEntries[entry] = 0;
            }

            foreach (var entry in _dirtyEntries.Keys)
            {
                if (entry.Entity != null)
                    entry.DetectChanges();
            }

            _dirtyEntries.Clear();
        }

        internal void MarkDirty(EntityEntry entry)
        {
            entry.UpgradeToFullTracking();
            if (_nonNotifyingEntries.ContainsKey(entry))
            {
                _nonNotifyingEntries[entry] = 1;
            }
            else
            {
                _dirtyEntries[entry] = 0;
            }
        }

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

            public override bool Equals(object? obj) => Equals(obj as CompositeKey);

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
