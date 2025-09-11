using System;

namespace nORM.Mapping
{
    /// <summary>
    /// Specifies the property that holds the discriminator value for table-per-hierarchy mappings.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorColumnAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the discriminator property.
        /// </summary>
        public string PropertyName { get; }

        /// <summary>
        /// Initializes the attribute with the discriminator property name.
        /// </summary>
        /// <param name="propertyName">Name of the property containing the discriminator value.</param>
        public DiscriminatorColumnAttribute(string propertyName)
            => PropertyName = propertyName;
    }
}
