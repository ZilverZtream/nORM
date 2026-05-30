#nullable enable
using System;

namespace nORM.Configuration
{
    /// <summary>
    /// Marks a property as participating in a named database index for migration
    /// snapshot generation.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public sealed class IndexAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexAttribute"/> class.
        /// </summary>
        /// <param name="name">The database index name.</param>
        public IndexAttribute(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Index name cannot be null or whitespace.", nameof(name));

            Name = name;
        }

        /// <summary>Gets the database index name.</summary>
        public string Name { get; }

        /// <summary>Gets or sets whether the index is unique.</summary>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Gets or sets the zero-based column order within a composite index.
        /// Single-column indexes can leave this at the default value.
        /// </summary>
        public int Order { get; set; }

        /// <summary>
        /// Gets or sets whether this key column is ordered descending within the index.
        /// </summary>
        public bool IsDescending { get; set; }
    }
}
