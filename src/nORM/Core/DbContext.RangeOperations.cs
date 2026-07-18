using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        // These take IEnumerable<T> only (no params T[] sibling): with the type parameter inferred
        // per call, a params T[] overload would bind T to the collection type itself for an
        // AddRange(someList) call (the params element is an exact match, beating the IEnumerable<T>
        // conversion) and treat the whole list as one entity. EF Core avoids this because its
        // TEntity is fixed on the DbSet, not inferred. Callers pass a list or an array literal;
        // both bind T to the element type here.

        /// <summary>
        /// Begins tracking all of the given entities in the <see cref="EntityState.Added"/> state,
        /// matching Entity Framework Core's <c>AddRange</c>. Each entity is validated and tracked
        /// exactly as <see cref="Add{T}"/> would; they are inserted on the next <c>SaveChanges</c>.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">The entities to add (a list or array).</param>
        public void AddRange<T>(IEnumerable<T> entities) where T : class
        {
            ArgumentNullException.ThrowIfNull(entities);
            foreach (var entity in entities)
                Add(entity);
        }

        /// <summary>
        /// Starts tracking all of the given entities in the <see cref="EntityState.Unchanged"/>
        /// state, matching Entity Framework Core's <c>AttachRange</c>. Mirrors <see cref="Attach{T}"/>
        /// for each element.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">The entities to attach (a list or array).</param>
        public void AttachRange<T>(IEnumerable<T> entities) where T : class
        {
            ArgumentNullException.ThrowIfNull(entities);
            foreach (var entity in entities)
                Attach(entity);
        }

        /// <summary>
        /// Marks all of the given entities as <see cref="EntityState.Modified"/>, matching Entity
        /// Framework Core's <c>UpdateRange</c>. Mirrors <see cref="Update{T}"/> for each element.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">The entities to update (a list or array).</param>
        public void UpdateRange<T>(IEnumerable<T> entities) where T : class
        {
            ArgumentNullException.ThrowIfNull(entities);
            foreach (var entity in entities)
                Update(entity);
        }

        /// <summary>
        /// Marks all of the given entities for deletion, matching Entity Framework Core's
        /// <c>RemoveRange</c>. Mirrors <see cref="Remove{T}"/> for each element; they are deleted on
        /// the next <c>SaveChanges</c>.
        /// </summary>
        /// <typeparam name="T">CLR type of the entities.</typeparam>
        /// <param name="entities">The entities to remove (a list or array).</param>
        public void RemoveRange<T>(IEnumerable<T> entities) where T : class
        {
            ArgumentNullException.ThrowIfNull(entities);
            foreach (var entity in entities)
                Remove(entity);
        }
    }
}
