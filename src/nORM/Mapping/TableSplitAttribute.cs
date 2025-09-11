using System;

namespace nORM.Mapping
{
    /// <summary>
    /// Indicates that an entity shares its table with another principal entity (table splitting).
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class TableSplitAttribute : Attribute
    {
        /// <summary>
        /// Gets the principal entity type with which the table is shared.
        /// </summary>
        public Type PrincipalType { get; }

        /// <summary>
        /// Initializes the attribute with the principal entity type.
        /// </summary>
        /// <param name="principalType">Type of the principal entity.</param>
        public TableSplitAttribute(Type principalType)
        {
            PrincipalType = principalType;
        }
    }
}
