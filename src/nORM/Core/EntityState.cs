using System;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Describes the tracking state of an entity with respect to a <see cref="DbContext"/>.
    /// </summary>
    public enum EntityState
    {
        /// <summary>
        /// The entity is not being tracked by the context.
        /// </summary>
        Detached,
        /// <summary>
        /// The entity exists in the database and has not been modified.
        /// </summary>
        Unchanged,
        /// <summary>
        /// The entity has been added to the context and will be inserted on <c>SaveChanges</c>.
        /// </summary>
        Added,
        /// <summary>
        /// The entity was retrieved from the database and has since been modified.
        /// </summary>
        Modified,
        /// <summary>
        /// The entity has been marked for deletion and will be removed on <c>SaveChanges</c>.
        /// </summary>
        Deleted
    }
}
