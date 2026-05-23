using System;

namespace nORM.Mapping
{
    /// <summary>
    /// Signals that a column has been renamed from a previous column name, allowing the schema
    /// differ to emit a RENAME COLUMN operation instead of a destructive DROP + ADD pair.
    /// </summary>
    /// <remarks>
    /// Apply this attribute to the property whose database column name has changed. The
    /// <see cref="OldName"/> value must match the column name that exists in the current
    /// (old) schema snapshot. When the differ finds a new column with this attribute whose
    /// <see cref="OldName"/> matches an old column that would otherwise be dropped, it emits
    /// a rename column entry in <c>SchemaDiff.RenamedColumns</c> rather than a drop + add pair.
    /// </remarks>
    /// <example>
    /// <code>
    /// // Old property: public string TotalCost { get; set; }
    /// // After rename:
    /// [RenameColumn("TotalCost")]
    /// public string TotalAmount { get; set; }
    /// </code>
    /// </example>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public sealed class RenameColumnAttribute : Attribute
    {
        /// <summary>The previous (old) column name as it existed in the database.</summary>
        public string OldName { get; }

        /// <summary>
        /// Initializes the attribute with the previous column name.
        /// </summary>
        /// <param name="oldName">The column name that existed before this rename.</param>
        public RenameColumnAttribute(string oldName)
        {
            if (string.IsNullOrWhiteSpace(oldName))
                throw new ArgumentException("OldName must not be null or whitespace.", nameof(oldName));
            OldName = oldName;
        }
    }
}
