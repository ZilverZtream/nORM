#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase))
                return BuildSequenceMetadata(obj);

            if (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase))
                return BuildSynonymMetadata(obj);

            if (string.Equals(obj.Kind, "Event", StringComparison.OrdinalIgnoreCase))
                return BuildEventMetadata(obj);

            if (string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
                || string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase))
            {
                return BuildQueryObjectMetadata(obj);
            }

            if (!string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            return ScaffoldRoutineMetadataBuilder.BuildMetadata(obj.Detail);
        }

        public static string ParseSkippedObjectProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase)) return "SQL Server";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase)) return "PostgreSQL";
            if (detail.StartsWith("MySQL", StringComparison.OrdinalIgnoreCase)) return "MySQL";
            if (detail.StartsWith("SQLite", StringComparison.OrdinalIgnoreCase)) return "SQLite";
            var space = detail.IndexOf(' ');
            return space > 0 ? detail[..space] : detail;
        }
    }
}
