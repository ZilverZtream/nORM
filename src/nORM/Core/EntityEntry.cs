using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public class EntityEntry
    {
        private readonly TableMapping _mapping;
        private Column[] _nonKeyColumns;
        private int[] _originalHashes;
        private object?[] _originalValues;
        private BitArray _changedProperties;
        private Func<object, int>[] _getHashCodes;
        private Func<object, object?>[] _getValues;
        private Dictionary<string, int> _propertyIndex;
        private readonly DbContextOptions _options;
        private readonly Action<EntityEntry>? _markDirty;
        private bool _hasNotifiedChange;

        public object? Entity { get; private set; }
        public EntityState State { get; internal set; }
        internal TableMapping Mapping => _mapping;

        internal EntityEntry(object entity, EntityState state, TableMapping mapping, DbContextOptions options, Action<EntityEntry>? markDirty = null, bool lazy = false)
        {
            Entity = entity;
            State = state;
            _mapping = mapping;
            _options = options;
            _markDirty = markDirty;

            if (!lazy)
            {
                InitializeTracking();
            }
            else
            {
                _nonKeyColumns = Array.Empty<Column>();
                _getHashCodes = Array.Empty<Func<object, int>>();
                _getValues = Array.Empty<Func<object, object?>>();
                _propertyIndex = new Dictionary<string, int>(StringComparer.Ordinal);
                _originalHashes = Array.Empty<int>();
                _originalValues = Array.Empty<object?>();
                _changedProperties = new BitArray(0);
            }
        }

        private void InitializeTracking()
        {
            _nonKeyColumns = _mapping.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToArray();
            _getHashCodes = new Func<object, int>[_nonKeyColumns.Length];
            _getValues = new Func<object, object?>[_nonKeyColumns.Length];
            _propertyIndex = new Dictionary<string, int>(StringComparer.Ordinal);
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                var getter = _nonKeyColumns[i].Getter;
                _getValues[i] = getter;
                _getHashCodes[i] = e => getter(e)?.GetHashCode() ?? 0;
                _propertyIndex[_nonKeyColumns[i].Prop.Name] = i;
            }
            _originalHashes = new int[_nonKeyColumns.Length];
            _originalValues = new object?[_nonKeyColumns.Length];
            _changedProperties = new BitArray(_nonKeyColumns.Length);
            CaptureOriginalValues();

            if (Entity is INotifyPropertyChanged notify)
            {
                notify.PropertyChanged += PropertyChangedHandler;
            }
        }

        private void PropertyChangedHandler(object? _, PropertyChangedEventArgs e)
        {
            if (State is EntityState.Added or EntityState.Deleted) return;

            var currentEntity = Entity;
            if (currentEntity is null) return;

            if (e.PropertyName != null && _propertyIndex.TryGetValue(e.PropertyName, out var idx))
            {
                var currentValue = _getValues[idx](currentEntity);
                var changed = !Equals(currentValue, _originalValues[idx]);
                _changedProperties[idx] = changed;
            }
            else
            {
                for (int i = 0; i < _nonKeyColumns.Length; i++)
                {
                    var currentValue = _getValues[i](currentEntity);
                    var changed = !Equals(currentValue, _originalValues[i]);
                    _changedProperties[i] = changed;
                }
            }

            var hasAnyChanges = false;
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                if (_changedProperties[i])
                {
                    hasAnyChanges = true;
                    break;
                }
            }

            _hasNotifiedChange = true;
            State = hasAnyChanges ? EntityState.Modified : EntityState.Unchanged;
            _markDirty?.Invoke(this);
        }

        internal void UpgradeToFullTracking()
        {
            if (_nonKeyColumns.Length != 0)
                return;
            InitializeTracking();
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
            UpgradeToFullTracking();
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
                    if (currentHash != _originalHashes[i])
                    {
                        changed = true;
                    }
                    else
                    {
                        // Hash collision - verify using precise comparison
                        var currentValue = _getValues[i](entity);
                        changed = !Equals(currentValue, _originalValues[i]);
                    }
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
            UpgradeToFullTracking();
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
