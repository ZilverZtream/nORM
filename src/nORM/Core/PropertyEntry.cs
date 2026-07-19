using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Access to a single mapped property of a tracked entity — its current value, its as-attached original
    /// value, and whether it is modified — the nORM equivalent of EF Core's <c>PropertyEntry</c>. Obtained from
    /// <see cref="EntityEntry.Property(string)"/>. Writing <see cref="IsModified"/> forces the column into
    /// (<c>true</c>) or excludes it from (<c>false</c>) the next partial-column UPDATE, independent of whether
    /// its value actually changed.
    /// </summary>
    public sealed class PropertyEntry
    {
        private readonly EntityEntry _entry;
        private readonly Column _column;

        internal PropertyEntry(EntityEntry entry, Column column)
        {
            _entry = entry;
            _column = column;
        }

        /// <summary>The mapped property name.</summary>
        public string Name => _column.PropName;

        /// <summary>The property's current value on the entity. Assigning writes it back and marks the entry dirty.</summary>
        public object? CurrentValue
        {
            get => _entry.ReadValue(_column, original: false);
            set => _entry.WriteValue(_column, value, original: false);
        }

        /// <summary>
        /// The property's original (as-attached / last-saved) value. Assigning overrides the tracked baseline —
        /// e.g. resetting a concurrency token to the database value while resolving a conflict.
        /// </summary>
        public object? OriginalValue
        {
            get => _entry.ReadValue(_column, original: true);
            set => _entry.WriteValue(_column, value, original: true);
        }

        /// <summary>
        /// Whether this property is flagged modified — i.e. it will appear in the next UPDATE's SET clause.
        /// Getting reflects the tracked/computed change state; setting <c>true</c> forces the column into the
        /// UPDATE even if its value is unchanged, and <c>false</c> excludes it. Not settable for a key or
        /// concurrency-token column.
        /// </summary>
        public bool IsModified
        {
            get => _entry.GetColumnModified(_column);
            set => _entry.SetColumnModified(_column, value);
        }
    }
}
