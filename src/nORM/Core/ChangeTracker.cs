using System.Collections.Generic;
using nORM.Mapping;
using RefComparer = System.Collections.Generic.ReferenceEqualityComparer;

#nullable enable

namespace nORM.Core
{
    public sealed class ChangeTracker
    {
        private readonly Dictionary<object, EntityEntry> _entries = new(RefComparer.Instance);

        internal EntityEntry Track(object entity, EntityState state, TableMapping mapping)
        {
            if (!_entries.TryGetValue(entity, out var entry))
            {
                entry = new EntityEntry(entity, state, mapping);
                _entries[entity] = entry;
            }
            else
            {
                entry.State = state;
            }
            return entry;
        }

        internal void Remove(object entity) => _entries.Remove(entity);

        public IEnumerable<EntityEntry> Entries => _entries.Values;

        internal void DetectChanges()
        {
            foreach (var entry in _entries.Values)
            {
                entry.DetectChanges();
            }
        }
    }
}
