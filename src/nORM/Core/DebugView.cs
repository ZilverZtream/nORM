using System;
using System.Globalization;
using System.Linq;
using System.Text;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A human-readable snapshot of the entities a <see cref="ChangeTracker"/> is tracking, for debugging —
    /// the nORM equivalent of EF Core's <c>ChangeTracker.DebugView</c>. <see cref="ShortView"/> is one line per
    /// tracked entity (type, primary key, state); <see cref="LongView"/> additionally lists each mapped
    /// property's current value, marks key properties with <c>PK</c>, and shows the original value of a
    /// modified property. This is a pure read of the tracker's current state — call
    /// <see cref="ChangeTracker.DetectChanges()"/> first if you want states refreshed against pending edits.
    /// </summary>
    public sealed class DebugView
    {
        private readonly ChangeTracker _tracker;

        internal DebugView(ChangeTracker tracker) => _tracker = tracker;

        /// <summary>One line per tracked entity: <c>Type {Key: value} State</c>.</summary>
        public string ShortView => Build(longView: false);

        /// <summary>Like <see cref="ShortView"/>, but each entity is followed by its mapped property values.</summary>
        public string LongView => Build(longView: true);

        /// <summary>Returns <see cref="LongView"/> so the object renders usefully in a debugger's text view.</summary>
        public override string ToString() => LongView;

        private string Build(bool longView)
        {
            var sb = new StringBuilder();
            foreach (var entry in _tracker.Entries)
            {
                if (sb.Length > 0)
                    sb.Append('\n');
                AppendHeader(sb, entry);
                if (longView)
                    AppendProperties(sb, entry);
            }
            return sb.ToString();
        }

        private static void AppendHeader(StringBuilder sb, EntityEntry entry)
        {
            var mapping = entry.Mapping;
            var current = entry.CurrentValues;
            sb.Append(mapping.Type.Name).Append(" {");
            for (int i = 0; i < mapping.KeyColumns.Length; i++)
            {
                if (i > 0) sb.Append(", ");
                var name = mapping.KeyColumns[i].PropName;
                sb.Append(name).Append(": ").Append(Format(current[name]));
            }
            sb.Append("} ").Append(entry.State);
        }

        private static void AppendProperties(StringBuilder sb, EntityEntry entry)
        {
            var mapping = entry.Mapping;
            var keyProps = mapping.KeyColumns.Select(c => c.PropName).ToHashSet(StringComparer.Ordinal);
            var current = entry.CurrentValues;
            // Original values are only meaningful for a Modified entry (Added has no baseline; Unchanged equals
            // current); reading them for other states would just repeat the current values.
            var original = entry.State == EntityState.Modified ? entry.OriginalValues : null;
            foreach (var prop in current.Properties)
            {
                var value = current[prop];
                sb.Append("\n  ").Append(prop).Append(": ").Append(Format(value));
                if (keyProps.Contains(prop))
                    sb.Append(" PK");
                if (original != null)
                {
                    var orig = original[prop];
                    if (!Equals(value, orig))
                        sb.Append(" Modified Originally ").Append(Format(orig));
                }
            }
        }

        private static string Format(object? value) => value switch
        {
            null => "<null>",
            string s => "'" + s + "'",
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "<null>"
        };
    }
}
