using System;
using System.Collections.Generic;
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
            foreach (var entry in _entriesByReference.Values)
            {
                entry.DetectChanges();
            }
        }

        private static object? GetPrimaryKeyValue(object entity, TableMapping mapping)
        {
            return mapping.KeyColumns.Length == 1
                ? mapping.KeyColumns[0].Getter(entity)
                : null; // Composite keys not yet supported
        }
    }
}
