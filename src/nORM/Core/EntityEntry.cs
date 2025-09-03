using System;
using System.Collections;
using System.ComponentModel;
using System.Linq;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public sealed class EntityEntry
    {
        private readonly TableMapping _mapping;
        private readonly Column[] _nonKeyColumns;
        private readonly int[] _originalHashes;
        private readonly BitArray _changedProperties;
        private readonly Func<object, int>[] _getHashCodes;
        private bool _hasNotifiedChange;

        public object Entity { get; }
        public EntityState State { get; internal set; }
        internal TableMapping Mapping => _mapping;

        internal EntityEntry(object entity, EntityState state, TableMapping mapping)
        {
            Entity = entity;
            State = state;
            _mapping = mapping;
            _nonKeyColumns = mapping.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToArray();
            _getHashCodes = new Func<object, int>[_nonKeyColumns.Length];
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                var getter = _nonKeyColumns[i].Getter;
                _getHashCodes[i] = e => getter(e)?.GetHashCode() ?? 0;
            }
            _originalHashes = new int[_nonKeyColumns.Length];
            _changedProperties = new BitArray(_nonKeyColumns.Length);
            CaptureOriginalValues();

            if (entity is INotifyPropertyChanged notify)
            {
                notify.PropertyChanged += (_, __) =>
                {
                    if (State is EntityState.Added or EntityState.Deleted) return;
                    _hasNotifiedChange = true;
                    State = EntityState.Modified;
                };
            }
        }

        private void CaptureOriginalValues()
        {
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                _originalHashes[i] = _getHashCodes[i](Entity);
                _changedProperties[i] = false;
            }
        }

        internal void DetectChanges()
        {
            if (State is EntityState.Added or EntityState.Deleted) return;
            if (_hasNotifiedChange) return;

            var hasChanges = false;
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                var currentHash = _getHashCodes[i](Entity);
                var changed = currentHash != _originalHashes[i];
                _changedProperties[i] = changed;
                hasChanges |= changed;
            }

            if (hasChanges)
                State = EntityState.Modified;
            else if (State == EntityState.Modified)
                State = EntityState.Unchanged;
        }

        internal void AcceptChanges()
        {
            CaptureOriginalValues();
            State = EntityState.Unchanged;
            _hasNotifiedChange = false;
        }

        internal void DetachEntity()
        {
            State = EntityState.Detached;
            NavigationPropertyExtensions.CleanupNavigationContext(Entity);
        }
    }
}
