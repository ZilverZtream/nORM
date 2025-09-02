using System.Collections.Generic;
using System.Linq;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public sealed class EntityEntry
    {
        private readonly TableMapping _mapping;
        private readonly Dictionary<string, object?> _originalValues = new();

        public object Entity { get; }
        public EntityState State { get; internal set; }
        internal TableMapping Mapping => _mapping;

        internal EntityEntry(object entity, EntityState state, TableMapping mapping)
        {
            Entity = entity;
            State = state;
            _mapping = mapping;
            CaptureOriginalValues();
        }

        private void CaptureOriginalValues()
        {
            _originalValues.Clear();
            foreach (var col in _mapping.Columns)
            {
                _originalValues[col.PropName] = col.Getter(Entity);
            }
        }

        internal void DetectChanges()
        {
            if (State is EntityState.Added or EntityState.Deleted) return;

            foreach (var col in _mapping.Columns.Where(c => !c.IsKey && !c.IsTimestamp))
            {
                var current = col.Getter(Entity);
                var original = _originalValues.TryGetValue(col.PropName, out var o) ? o : null;
                if (!Equals(current, original))
                {
                    State = EntityState.Modified;
                    return;
                }
            }

            if (State == EntityState.Modified)
                State = EntityState.Unchanged;
        }

        internal void AcceptChanges()
        {
            CaptureOriginalValues();
            State = EntityState.Unchanged;
        }
    }
}
