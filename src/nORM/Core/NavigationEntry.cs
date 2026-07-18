using System;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Represents a single navigation property of a tracked entity, exposed by
    /// <see cref="EntityEntry.Reference(string)"/>, <see cref="EntityEntry.Collection(string)"/>, and
    /// <see cref="EntityEntry.Navigation(string)"/>. Mirrors EF Core's navigation entries: explicitly
    /// load the related data with <see cref="Load"/> / <see cref="LoadAsync"/>, inspect or override
    /// whether it has been loaded via <see cref="IsLoaded"/>, and read/write the property through
    /// <see cref="CurrentValue"/>.
    /// </summary>
    public sealed class NavigationEntry
    {
        private readonly EntityEntry _entry;
        private readonly PropertyInfo _property;

        internal NavigationEntry(EntityEntry entry, PropertyInfo property)
        {
            _entry = entry;
            _property = property;
        }

        /// <summary>The navigation property's CLR name.</summary>
        public string Name => _property.Name;

        private bool IsCollection
            => _property.PropertyType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(_property.PropertyType);

        /// <summary>
        /// Whether the navigation has been loaded. Setting it records the loaded state without touching
        /// the data — e.g. mark a manually populated navigation as loaded so it is not reloaded.
        /// </summary>
        public bool IsLoaded
        {
            get => _entry.Entity != null && NavigationPropertyExtensions.IsNavigationLoaded(_entry.Entity, _property);
            set
            {
                var entity = _entry.Entity;
                if (entity != null)
                    NavigationPropertyExtensions.SetNavigationLoaded(entity, _property, value, _entry.MappedType, RequireContext());
            }
        }

        /// <summary>Gets or sets the navigation property's current value on the entity.</summary>
        public object? CurrentValue
        {
            get => _entry.Entity is { } entity ? _property.GetValue(entity) : null;
            set
            {
                if (_entry.Entity is { } entity)
                    _property.SetValue(entity, value);
            }
        }

        /// <summary>
        /// Loads the related data for this navigation from the database if it has not already been loaded.
        /// </summary>
        [RequiresDynamicCode("Loading a navigation builds a relationship query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Loading a navigation reflects over the relationship metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public async Task LoadAsync(CancellationToken ct = default)
        {
            var entity = _entry.Entity
                ?? throw new InvalidOperationException("Cannot load a navigation on a detached entry.");
            var context = RequireContext();
            if (IsCollection)
            {
                await NavigationPropertyExtensions.LoadNavigationForEntryAsync(entity, _property, _entry.MappedType, context, ct).ConfigureAwait(false);
            }
            else
            {
                await context.LoadReferenceNavigationForEntryAsync(entity, _entry.MappedType, _property, ct).ConfigureAwait(false);
                NavigationPropertyExtensions.SetNavigationLoaded(entity, _property, true, _entry.MappedType, context);
            }
        }

        /// <summary>Synchronous <see cref="LoadAsync"/>.</summary>
        [RequiresDynamicCode("Loading a navigation builds a relationship query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Loading a navigation reflects over the relationship metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public void Load()
        {
            var entity = _entry.Entity
                ?? throw new InvalidOperationException("Cannot load a navigation on a detached entry.");
            var context = RequireContext();
            if (IsCollection)
            {
                NavigationPropertyExtensions.LoadNavigationForEntry(entity, _property, _entry.MappedType, context, CancellationToken.None);
            }
            else
            {
                context.LoadReferenceNavigationForEntry(entity, _entry.MappedType, _property);
                NavigationPropertyExtensions.SetNavigationLoaded(entity, _property, true, _entry.MappedType, context);
            }
        }

        private DbContext RequireContext()
            => _entry.Context
               ?? throw new InvalidOperationException(
                   "This entry is not associated with a context. Obtain it from context.Entry(entity).");
    }
}
