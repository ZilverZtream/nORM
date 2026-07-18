using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A mutable bag of an entity's property values, exposed by <see cref="EntityEntry.CurrentValues"/>
    /// (the live entity) and <see cref="EntityEntry.OriginalValues"/> (the as-loaded baseline). It mirrors
    /// EF Core's <c>PropertyValues</c>: index by property name to read or write a value, copy a whole set of
    /// values with <see cref="SetValues(object)"/>, or clone them into a detached instance with
    /// <see cref="ToObject"/>. Writing to <see cref="EntityEntry.CurrentValues"/> updates the entity and
    /// marks it modified; writing to <see cref="EntityEntry.OriginalValues"/> overrides the tracked baseline
    /// (e.g. a concurrency token) without touching the entity.
    /// </summary>
    public sealed class PropertyValues
    {
        private readonly EntityEntry _entry;
        private readonly bool _original;

        internal PropertyValues(EntityEntry entry, bool original)
        {
            _entry = entry;
            _original = original;
        }

        /// <summary>The mapped property names these values cover, in column order.</summary>
        public IReadOnlyList<string> Properties => _entry.MappedColumns.Select(c => c.PropName).ToArray();

        /// <summary>Gets or sets the value of the named mapped property.</summary>
        /// <param name="propertyName">The CLR property name of a mapped column.</param>
        /// <exception cref="ArgumentException">The type has no mapped property with that name.</exception>
        public object? this[string propertyName]
        {
            get => _entry.ReadValue(_entry.RequireColumn(propertyName), _original);
            set => _entry.WriteValue(_entry.RequireColumn(propertyName), value, _original);
        }

        /// <summary>Gets the value of the named property converted to <typeparamref name="T"/>.</summary>
        public T? GetValue<T>(string propertyName)
        {
            var value = this[propertyName];
            if (value is null)
                return default;
            if (value is T typed)
                return typed;
            var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
            return (T)Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Copies every mapped property value from <paramref name="source"/> into this bag. The source may be
        /// another <see cref="PropertyValues"/> (e.g. copy database values onto the tracked entity while
        /// resolving a conflict) or an instance of the mapped entity type.
        /// </summary>
        /// <exception cref="ArgumentException"><paramref name="source"/> is neither a <see cref="PropertyValues"/>
        /// nor an instance of the mapped entity type.</exception>
        public void SetValues(object source)
        {
            ArgumentNullException.ThrowIfNull(source);

            if (source is PropertyValues other)
            {
                foreach (var col in _entry.MappedColumns)
                    _entry.WriteValue(col, other._entry.ReadValue(col, other._original), _original);
                return;
            }

            var mappedType = _entry.MappedType;
            if (!mappedType.IsInstanceOfType(source))
                throw new ArgumentException(
                    $"SetValues expects an instance of '{mappedType.Name}' or a {nameof(PropertyValues)}, not '{source.GetType().Name}'.",
                    nameof(source));

            foreach (var col in _entry.MappedColumns)
                _entry.WriteValue(col, col.Getter(source), _original);
        }

        /// <summary>
        /// Creates a new, untracked instance of the mapped entity type populated with these values. Requires
        /// the entity type to have a public parameterless constructor.
        /// </summary>
        [RequiresDynamicCode("Cloning an entity constructs its type at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Cloning an entity reflects over its constructor; trimming may remove it. See docs/aot-trimming.md.")]
        public object ToObject()
        {
            var instance = Activator.CreateInstance(_entry.MappedType)
                ?? throw new InvalidOperationException($"'{_entry.MappedType.Name}' has no accessible parameterless constructor.");
            foreach (var col in _entry.MappedColumns)
                col.Setter(instance, _entry.ReadValue(col, _original));
            return instance;
        }
    }
}
