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

        private const int MaxCascadeDepth = 100;

        internal void Remove(object entity, bool cascade = false, int depth = 0, HashSet<object>? visited = null)
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
                    visited ??= new HashSet<object>(RefComparer.Instance);
                    visited.Add(entity);
                    CascadeDelete(entity, entry.Mapping, depth, visited);
                }
            }
        }

        private void CascadeDelete(object entity, TableMapping mapping, int depth, HashSet<object> visited)
        {
            if (depth >= MaxCascadeDepth)
                return;

            foreach (var relation in mapping.Relations.Values)
            {
                if (!relation.CascadeDelete)
                    continue;

                var navValue = relation.NavProp.GetValue(entity);
                if (navValue is IEnumerable collection)
                {
                    foreach (var child in collection)
                    {
                        if (child != null && visited.Add(child))
                            Remove(child, true, depth + 1, visited);
                    }
                }
                else if (navValue != null && visited.Add(navValue))
                {
                    Remove(navValue, true, depth + 1, visited);
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
                var hash = new HashCode();
                foreach (var value in _values)
                    hash.Add(value);
                return hash.ToHashCode();
            }
        }
    }
}
