#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectMetadataBuilder
    {
        public static bool IsTableLikeSqlServerSynonym(string detail)
        {
            if (!detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return false;

            var baseType = ParseSemicolonValue(detail, "baseType");
            return string.Equals(baseType, "U", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(baseType, "V", StringComparison.OrdinalIgnoreCase);
        }

        private static IReadOnlyDictionary<string, object?> BuildSynonymMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (!obj.Detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var baseObject = ParseSemicolonValue(obj.Detail, "baseObject");
            var baseType = ParseSemicolonValue(obj.Detail, "baseType");
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = "SQL Server",
                ["queryArtifactSupported"] = IsTableLikeSqlServerSynonym(obj.Detail)
            };

            if (!string.IsNullOrWhiteSpace(baseObject))
                metadata["baseObject"] = baseObject;
            if (!string.IsNullOrWhiteSpace(baseType))
            {
                metadata["baseType"] = baseType;
                metadata["targetKind"] = baseType.ToUpperInvariant() switch
                {
                    "U" => "Table",
                    "V" => "View",
                    "P" => "Procedure",
                    "FN" or "IF" or "TF" => "Function",
                    _ => baseType
                };
            }

            return metadata;
        }
    }
}
