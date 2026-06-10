#nullable enable
using System;

namespace nORM.Configuration
{
    /// <summary>
    /// Specifies explicit null ordering for provider index keys.
    /// </summary>
    public enum IndexNullSortOrder
    {
        /// <summary>Use the provider default null ordering for the key direction.</summary>
        Default = 0,
        /// <summary>Sort null values before non-null values.</summary>
        First = 1,
        /// <summary>Sort null values after non-null values.</summary>
        Last = 2
    }

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

        /// <summary>
        /// Gets or sets whether this column is an included, non-key column in the index.
        /// </summary>
        public bool IsIncluded { get; set; }

        /// <summary>
        /// Gets or sets whether a unique PostgreSQL index treats null values as equal.
        /// </summary>
        public bool NullsNotDistinct { get; set; }

        /// <summary>
        /// Gets or sets explicit null ordering for this index key column.
        /// </summary>
        public IndexNullSortOrder NullSortOrder { get; set; }

        /// <summary>
        /// Gets or sets the provider-specific filter predicate for a filtered/partial index.
        /// </summary>
        public string? FilterSql { get; set; }
    }
}
