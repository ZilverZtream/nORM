using System;
using System.Linq.Expressions;

namespace nORM.Core
{
    /// <summary>
    /// Represents a fluent builder used to specify property assignments for bulk update operations.
    /// </summary>
    /// <remarks>
    /// Two assignment forms are supported:
    ///   - Literal / precomputed value: SetProperty(x => x.Foo, 42)
    ///   - Server-side computed expression over the row being updated:
    ///     SetProperty(x => x.Counter, x => x.Counter + 1)
    /// </remarks>
    /// <typeparam name="T">Type of the entity being updated.</typeparam>
    public sealed class SetPropertyCalls<T>
    {
        /// <summary>
        /// Specifies a property to set to the given literal or captured value during a bulk update.
        /// </summary>
        /// <typeparam name="TProperty">Type of the property value.</typeparam>
        /// <param name="property">Expression pointing to the property to update.</param>
        /// <param name="value">Literal or precomputed captured local value to assign.</param>
        /// <returns>The same <see cref="SetPropertyCalls{T}"/> instance for chaining.</returns>
        public SetPropertyCalls<T> SetProperty<TProperty>(Expression<Func<T, TProperty>> property, TProperty value) => this;

        /// <summary>
        /// Specifies a property to set to a server-side expression evaluated per row. Lets
        /// callers write counter increments / arithmetic / string concatenation that
        /// references columns of the row being updated, e.g.
        /// <c>SetProperty(x => x.Counter, x => x.Counter + 1)</c>.
        /// </summary>
        /// <typeparam name="TProperty">Type of the property value.</typeparam>
        /// <param name="property">Expression pointing to the property to update.</param>
        /// <param name="valueExpression">Expression evaluated server-side that produces the assigned value.</param>
        /// <returns>The same <see cref="SetPropertyCalls{T}"/> instance for chaining.</returns>
        public SetPropertyCalls<T> SetProperty<TProperty>(Expression<Func<T, TProperty>> property, Expression<Func<T, TProperty>> valueExpression) => this;
    }
}
