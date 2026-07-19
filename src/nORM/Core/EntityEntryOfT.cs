using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A strongly-typed view over a tracked entity's <see cref="EntityEntry"/> (EF Core parity for
    /// <c>EntityEntry&lt;TEntity&gt;</c>), obtained from <see cref="DbContext.Entry{TEntity}(TEntity)"/>. It
    /// surfaces the full untyped <see cref="EntityEntry"/> API and adds a lambda-based
    /// <see cref="Property{TProperty}(Expression{Func{TEntity, TProperty}})"/> plus a typed
    /// <see cref="Entity"/>, so property state can be read or set without stringly-typed names — e.g.
    /// <c>ctx.Entry(order).Property(o =&gt; o.Total).IsModified = true</c>. It converts implicitly to
    /// <see cref="EntityEntry"/>, and the underlying entry is also available via <see cref="Entry"/>.
    /// </summary>
    /// <typeparam name="TEntity">The tracked entity's CLR type.</typeparam>
    public sealed class EntityEntry<TEntity> where TEntity : class
    {
        private readonly EntityEntry _entry;

        internal EntityEntry(EntityEntry entry) => _entry = entry;

        /// <summary>The underlying untyped <see cref="EntityEntry"/>.</summary>
        public EntityEntry Entry => _entry;

        /// <summary>Implicitly exposes the typed entry as its underlying <see cref="EntityEntry"/>.</summary>
        public static implicit operator EntityEntry(EntityEntry<TEntity> entry) => entry._entry;

        /// <summary>The tracked entity instance, typed.</summary>
        public TEntity Entity => (TEntity)_entry.Entity!;

        /// <inheritdoc cref="EntityEntry.State"/>
        public EntityState State
        {
            get => _entry.State;
            set => _entry.State = value;
        }

        /// <inheritdoc cref="EntityEntry.IsKeySet"/>
        public bool IsKeySet => _entry.IsKeySet;

        /// <inheritdoc cref="EntityEntry.CurrentValues"/>
        public PropertyValues CurrentValues => _entry.CurrentValues;

        /// <inheritdoc cref="EntityEntry.OriginalValues"/>
        public PropertyValues OriginalValues => _entry.OriginalValues;

        /// <summary>The <see cref="PropertyEntry"/> for a mapped property by name.</summary>
        public PropertyEntry Property(string propertyName) => _entry.Property(propertyName);

        /// <summary>
        /// The <see cref="PropertyEntry"/> for a mapped property selected by a strongly-typed lambda, e.g.
        /// <c>Property(o =&gt; o.Total)</c>.
        /// </summary>
        /// <param name="propertyExpression">A simple property access on the entity.</param>
        /// <exception cref="ArgumentException">The expression is not a simple property access.</exception>
        public PropertyEntry Property<TProperty>(Expression<Func<TEntity, TProperty>> propertyExpression)
        {
            ArgumentNullException.ThrowIfNull(propertyExpression);
            return _entry.Property(GetMemberName(propertyExpression));
        }

        /// <inheritdoc cref="EntityEntry.Navigation(string)"/>
        public NavigationEntry Navigation(string navigationName) => _entry.Navigation(navigationName);

        /// <inheritdoc cref="EntityEntry.Reference(string)"/>
        public NavigationEntry Reference(string navigationName) => _entry.Reference(navigationName);

        /// <inheritdoc cref="EntityEntry.Collection(string)"/>
        public NavigationEntry Collection(string navigationName) => _entry.Collection(navigationName);

        /// <inheritdoc cref="EntityEntry.Reload"/>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public void Reload() => _entry.Reload();

        /// <inheritdoc cref="EntityEntry.ReloadAsync"/>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public Task ReloadAsync(CancellationToken cancellationToken = default) => _entry.ReloadAsync(cancellationToken);

        /// <inheritdoc cref="EntityEntry.GetDatabaseValues"/>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public PropertyValues? GetDatabaseValues() => _entry.GetDatabaseValues();

        /// <inheritdoc cref="EntityEntry.GetDatabaseValuesAsync"/>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public Task<PropertyValues?> GetDatabaseValuesAsync(CancellationToken cancellationToken = default)
            => _entry.GetDatabaseValuesAsync(cancellationToken);

        private static string GetMemberName<TProperty>(Expression<Func<TEntity, TProperty>> expression)
        {
            var body = expression.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
                body = convert.Operand;
            if (body is MemberExpression member)
                return member.Member.Name;
            throw new ArgumentException(
                $"Expression '{expression}' must be a simple property access, e.g. 'e => e.Property'.", nameof(expression));
        }
    }
}
