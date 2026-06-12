using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;
using System.Globalization;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        /// <summary>
        /// X1 fix: Detects whether the runtime mapping has fluent-only column renames that the
        /// source generator couldn't see at compile time. Returns true if any column's Name
        /// (runtime-resolved) differs from its PropName (attribute-resolved default).
        /// When true, the compiled materializer must be skipped to avoid GetOrdinal failures.
        /// </summary>
        private static bool HasFluentColumnRenames(Type targetType, TableMapping mapping)
        {
            foreach (var col in mapping.Columns)
            {
                // Column.Name is the runtime column name (from fluent config or attribute).
                // Column.PropName is the C# property name (default if no [Column] attribute).
                // The source generator uses [Column] attribute ? if present, Name == attribute value.
                // If Name != PropName AND there's no [Column] attribute on the property, it's a fluent rename.
                var prop = targetType.GetProperty(col.PropName);
                if (prop == null) continue;

                var columnAttr = prop.GetCustomAttribute(
                    typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute));

                if (columnAttr == null && col.Name != col.PropName)
                {
                    // Fluent-only rename detected: runtime column name differs from property name
                    // and there's no [Column] attribute to explain it to the source generator.
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// SG1 fix: Detects whether the mapping contains inline owned-navigation columns.
        /// Owned scalar navigations (OwnsOne) contribute columns whose <c>PropName</c> includes an
        /// owner-type prefix (e.g. <c>"Address_Street"</c>) while <c>Prop.Name</c> is just the
        /// tail (<c>"Street"</c>). The source generator cannot reconstruct the nesting needed to
        /// populate the owner navigation property, so the compiled materializer must be bypassed
        /// when such columns are present.
        /// </summary>
        private static bool HasOwnedNavigationColumns(TableMapping mapping)
        {
            foreach (var col in mapping.Columns)
            {
                // Shadow columns are internal; only check mapped CLR properties.
                if (!col.IsShadow && col.PropName != col.Prop.Name)
                    return true;
            }
            return false;
        }

        // Pre-computed type conversion delegates for better performance
    }
}
