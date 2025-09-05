using System;
using System.Collections;
using System.ComponentModel;
using System.Linq;
using nORM.Configuration;
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
        private readonly object?[] _originalValues;
        private readonly BitArray _changedProperties;
        private readonly Func<object, int>[] _getHashCodes;
        private readonly Func<object, object?>[] _getValues;
        private readonly DbContextOptions _options;
        private bool _hasNotifiedChange;

        public object? Entity { get; private set; }
        public EntityState State { get; internal set; }
        internal TableMapping Mapping => _mapping;

        internal EntityEntry(object entity, EntityState state, TableMapping mapping, DbContextOptions options)
        {
            Entity = entity;
            State = state;
            _mapping = mapping;
            _options = options;
            _nonKeyColumns = mapping.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToArray();
            _getHashCodes = new Func<object, int>[_nonKeyColumns.Length];
            _getValues = new Func<object, object?>[_nonKeyColumns.Length];
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                var getter = _nonKeyColumns[i].Getter;
                _getValues[i] = getter;
                _getHashCodes[i] = e => getter(e)?.GetHashCode() ?? 0;
            }
            _originalHashes = new int[_nonKeyColumns.Length];
            _originalValues = new object?[_nonKeyColumns.Length];
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
            var entity = Entity;
            if (entity is null)
            {
                DetachEntity();
                return;
            }
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                _originalHashes[i] = _getHashCodes[i](entity);
                _originalValues[i] = _getValues[i](entity);
                _changedProperties[i] = false;
            }
        }

        internal void DetectChanges()
        {
            if (State is EntityState.Added or EntityState.Deleted or EntityState.Detached) return;
            if (_hasNotifiedChange) return;

            var entity = Entity;
            if (entity is null)
            {
                DetachEntity();
                return;
            }

            var hasChanges = false;
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                bool changed;
                if (_options.UsePreciseChangeTracking)
                {
                    var currentValue = _getValues[i](entity);
                    changed = !Equals(currentValue, _originalValues[i]);
                }
                else
                {
                    var currentHash = _getHashCodes[i](entity);
                    changed = currentHash != _originalHashes[i];
                }
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
            var entity = Entity;
            if (entity != null)
                NavigationPropertyExtensions.CleanupNavigationContext(entity);
            Entity = null;
        }
    }
}
