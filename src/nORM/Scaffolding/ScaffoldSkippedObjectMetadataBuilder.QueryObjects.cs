#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectMetadataBuilder
    {
        private static IReadOnlyDictionary<string, object?> BuildQueryObjectMetadata(ScaffoldSkippedObjectInfo obj)
        {
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = ParseSkippedObjectProvider(obj.Detail),
                ["targetKind"] = obj.Kind,
                ["queryArtifactSupported"] = IsQueryArtifactObject(obj)
            };

            if (string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase))
            {
                var owner = InferSqliteVirtualTableShadowOwner(obj.Name);
                if (!string.IsNullOrWhiteSpace(owner))
                    metadata["shadowOf"] = owner;
            }

            return metadata;
        }

        private static bool IsQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase) && IsTableLikeSqlServerSynonym(obj.Detail));

        private static string InferSqliteVirtualTableShadowOwner(string tableName)
        {
            var suffixes = new[]
            {
                "_content",
                "_data",
                "_idx",
                "_docsize",
                "_config",
                "_segments",
                "_segdir",
                "_stat",
                "_node",
                "_parent",
                "_rowid"
            };

            foreach (var suffix in suffixes)
            {
                if (tableName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)
                    && tableName.Length > suffix.Length)
                {
                    return tableName[..^suffix.Length];
                }
            }

            return string.Empty;
        }
    }
}
