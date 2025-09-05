using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;
using nORM.Mapping;
using RefComparer = System.Collections.Generic.ReferenceEqualityComparer;

#nullable enable

namespace nORM.Core
{
    public sealed class ChangeTracker
    {
        private readonly Dictionary<object, EntityEntry> _entriesByReference = new(RefComparer.Instance);
        private readonly Dictionary<Type, Dictionary<object, EntityEntry>> _entriesByKey = new();
        private readonly DbContextOptions _options;

        public ChangeTracker(DbContextOptions options)
        {
            _options = options;
        }

        internal EntityEntry Track(object entity, EntityState state, TableMapping mapping)
        {
            var pk = GetPrimaryKeyValue(entity, mapping);
            if (pk != null && _entriesByKey.TryGetValue(mapping.Type, out var typeEntries) &&
                typeEntries.TryGetValue(pk, out var existing))
            {
                existing.State = state;
                return existing;
            }

            var entry = new EntityEntry(entity, state, mapping, _options);
            _entriesByReference[entity] = entry;
            if (pk != null)
            {
                if (!_entriesByKey.TryGetValue(mapping.Type, out typeEntries))
                {
                    typeEntries = new Dictionary<object, EntityEntry>();
                    _entriesByKey[mapping.Type] = typeEntries;
                }
                typeEntries[pk] = entry;
            }

            return entry;
        }

        internal void Remove(object entity)
        {
            if (_entriesByReference.TryGetValue(entity, out var entry))
            {
                entry.DetachEntity();
                _entriesByReference.Remove(entity);
                var pk = GetPrimaryKeyValue(entity, entry.Mapping);
                if (pk != null && _entriesByKey.TryGetValue(entry.Mapping.Type, out var typeEntries))
                {
                    typeEntries.Remove(pk);
                    if (typeEntries.Count == 0)
                        _entriesByKey.Remove(entry.Mapping.Type);
                }
            }
        }

        public IEnumerable<EntityEntry> Entries => _entriesByReference.Values;

        internal void DetectChanges()
        {
            var entriesSnapshot = _entriesByReference.Values.ToList();
            foreach (var entry in entriesSnapshot)
            {
                if (_entriesByReference.ContainsKey(entry.Entity))
                {
                    entry.DetectChanges();
                }
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
