using System;
using System.Linq.Expressions;

namespace nORM.Core
{
    /// <summary>
    /// Represents a fluent builder used to specify property assignments for bulk update operations.
    /// </summary>
    /// <remarks>
    /// v1 supports literal constants and precomputed captured local values as assignment
    /// values. Method calls, inline computed values, column-based updates, and server
    /// expressions are rejected by <c>ExecuteUpdateAsync</c>.
    /// </remarks>
    /// <typeparam name="T">Type of the entity being updated.</typeparam>
    public sealed class SetPropertyCalls<T>
    {
        /// <summary>
        /// Specifies a property to set to the given value during a bulk update.
        /// </summary>
        /// <typeparam name="TProperty">Type of the property value.</typeparam>
        /// <param name="property">Expression pointing to the property to update.</param>
        /// <param name="value">Literal or precomputed captured local value to assign.</param>
        /// <returns>The same <see cref="SetPropertyCalls{T}"/> instance for chaining.</returns>
        public SetPropertyCalls<T> SetProperty<TProperty>(Expression<Func<T, TProperty>> property, TProperty value) => this;
    }
}
