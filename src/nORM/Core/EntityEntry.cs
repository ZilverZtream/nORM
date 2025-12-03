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
    /// <summary>
    /// Represents the change tracking information for a single entity instance.
    /// An <see cref="EntityEntry"/> keeps the original values and state required to
    /// compute database updates when <c>SaveChanges</c> is invoked.
    /// </summary>
    /// <summary>
    /// PERFORMANCE OPTIMIZATION: Deferred initialization of tracking arrays.
    /// Arrays are null until InitializeTracking() is called, reducing memory overhead
    /// for read-only or short-lived entities by ~200-500 bytes per entity.
    /// </summary>
    public class EntityEntry
    {
        private readonly TableMapping _mapping;
        // PERFORMANCE: Use null instead of Array.Empty to truly defer allocation
        private Column[]? _nonKeyColumns;
        private int[]? _originalHashes;
        private object?[]? _originalValues;
        private BitArray? _changedProperties;
        private Func<object, int>[]? _getHashCodes;
        private Func<object, object?>[]? _getValues;
        private Dictionary<string, int>? _propertyIndex;
        private readonly DbContextOptions _options;
        private readonly Action<EntityEntry>? _markDirty;
        private bool _hasNotifiedChange;
        private bool _isInitialized;

        /// <summary>
        /// Gets the entity instance being tracked. May be <c>null</c> after the entity
        /// has been detached from the context.
        /// </summary>
        public object? Entity { get; private set; }

        /// <summary>
        /// Gets or sets the current <see cref="EntityState"/> of the tracked entity
        /// within the context.
        /// </summary>
        public EntityState State { get; internal set; }
        internal TableMapping Mapping => _mapping;

        internal EntityEntry(object entity, EntityState state, TableMapping mapping, DbContextOptions options, Action<EntityEntry>? markDirty = null, bool lazy = false)
        {
            Entity = entity;
            State = state;
            _mapping = mapping;
            _options = options;
            _markDirty = markDirty;
            _isInitialized = false;

            // PERFORMANCE: Only initialize immediately if not lazy
            // This saves ~200-500 bytes per entity for read-only scenarios
            if (!lazy)
            {
                InitializeTracking();
            }
        }

        /// <summary>
        /// Prepares the entry for change tracking by caching getters and hash-code
        /// delegates for all non-key, non-timestamp columns and capturing the
        /// entity's original values. If the entity implements
        /// <see cref="INotifyPropertyChanged"/>, the entry subscribes to change
        /// notifications to enable real-time tracking.
        /// </summary>
        /// <remarks>
        /// PERFORMANCE: Only called when tracking is actually needed, not on entity load.
        /// </remarks>
        private void InitializeTracking()
        {
            if (_isInitialized)
                return;

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
            _isInitialized = true;

            CaptureOriginalValues();

            if (Entity is INotifyPropertyChanged notify)
            {
                notify.PropertyChanged += PropertyChangedHandler;
            }
        }

        /// <summary>
        /// Handles <see cref="INotifyPropertyChanged.PropertyChanged"/> events raised
        /// by the tracked entity and updates the internal change tracking state.
        /// </summary>
        /// <param name="_">Unused sender parameter.</param>
        /// <param name="e">Event arguments describing the property change.</param>
        private void PropertyChangedHandler(object? _, PropertyChangedEventArgs e)
        {
            if (State is EntityState.Added or EntityState.Deleted) return;
            if (!_isInitialized) return; // PERFORMANCE: Skip if not yet initialized

            var currentEntity = Entity;
            if (currentEntity is null) return;

            if (e.PropertyName != null && _propertyIndex!.TryGetValue(e.PropertyName, out var idx))
            {
                var currentValue = _getValues![idx](currentEntity);
                var changed = !Equals(currentValue, _originalValues![idx]);
                _changedProperties![idx] = changed;
            }
            else
            {
                for (int i = 0; i < _nonKeyColumns!.Length; i++)
                {
                    var currentValue = _getValues![i](currentEntity);
                    var changed = !Equals(currentValue, _originalValues![i]);
                    _changedProperties![i] = changed;
                }
            }

            var hasAnyChanges = false;
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                if (_changedProperties![i])
                {
                    hasAnyChanges = true;
                    break;
                }
            }

            _hasNotifiedChange = true;
            State = hasAnyChanges ? EntityState.Modified : EntityState.Unchanged;
            _markDirty?.Invoke(this);
        }

        /// <summary>
        /// Ensures the entry has been fully initialized for change tracking. When a
        /// lazily-initialized entry first detects changes, this method populates the
        /// required caches and subscribes to change notifications.
        /// </summary>
        internal void UpgradeToFullTracking()
        {
            if (_isInitialized)
                return;
            InitializeTracking();
        }

        /// <summary>
        /// Captures the current property values and hash codes as the baseline for
        /// detecting future modifications.
        /// </summary>
        private void CaptureOriginalValues()
        {
            if (!_isInitialized) return; // PERFORMANCE: Skip if not initialized

            var entity = Entity;
            if (entity is null)
            {
                DetachEntity();
                return;
            }
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                _originalHashes![i] = _getHashCodes![i](entity);
                _originalValues![i] = _getValues![i](entity);
                _changedProperties![i] = false;
            }
        }

        /// <summary>
        /// Compares the current entity values against the original snapshot to update
        /// the <see cref="EntityState"/>. This method is used for entities that do not
        /// notify when properties change.
        /// </summary>
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
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                bool changed;
                if (_options.UsePreciseChangeTracking)
                {
                    var currentValue = _getValues![i](entity);
                    changed = !Equals(currentValue, _originalValues![i]);
                }
                else
                {
                    var currentHash = _getHashCodes![i](entity);
                    if (currentHash != _originalHashes![i])
                    {
                        changed = true;
                    }
                    else
                    {
                        // Hash collision - verify using precise comparison
                        var currentValue = _getValues![i](entity);
                        changed = !Equals(currentValue, _originalValues![i]);
                    }
                }
                _changedProperties![i] = changed;
                hasChanges |= changed;
            }

            if (hasChanges)
                State = EntityState.Modified;
            else if (State == EntityState.Modified)
                State = EntityState.Unchanged;
        }

        /// <summary>
        /// Accepts the current property values as the new original values and resets
        /// the change tracking state to <see cref="EntityState.Unchanged"/>.
        /// </summary>
        internal void AcceptChanges()
        {
            UpgradeToFullTracking();
            CaptureOriginalValues();
            State = EntityState.Unchanged;
            _hasNotifiedChange = false;
        }

        /// <summary>
        /// Detaches the entity from the context and removes any navigation property
        /// references that were established for change tracking or lazy loading.
        /// </summary>
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
