using System;
using System.Linq.Expressions;

namespace nORM.Core
{
    /// <summary>
    /// Represents a fluent builder used to specify property assignments for bulk update operations.
    /// </summary>
    /// <typeparam name="T">Type of the entity being updated.</typeparam>
    public sealed class SetPropertyCalls<T>
    {
        /// <summary>
        /// Specifies a property to set to the given value during a bulk update.
        /// </summary>
        /// <typeparam name="TProperty">Type of the property value.</typeparam>
        /// <param name="property">Expression pointing to the property to update.</param>
        /// <param name="value">New value to assign.</param>
        /// <returns>The same <see cref="SetPropertyCalls{T}"/> instance for chaining.</returns>
        public SetPropertyCalls<T> SetProperty<TProperty>(Expression<Func<T, TProperty>> property, TProperty value) => this;
    }
}
