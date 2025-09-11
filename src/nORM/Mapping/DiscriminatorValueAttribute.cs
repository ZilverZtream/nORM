using System;

namespace nORM.Mapping
{
    /// <summary>
    /// Specifies the discriminator value for an entity type in a table-per-hierarchy mapping.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorValueAttribute : Attribute
    {
        /// <summary>
        /// Gets the discriminator value associated with the entity type.
        /// </summary>
        public object Value { get; }

        /// <summary>
        /// Initializes the attribute with the discriminator value.
        /// </summary>
        /// <param name="value">Value stored in the discriminator column for this type.</param>
        public DiscriminatorValueAttribute(object value)
            => Value = value;
    }
}
