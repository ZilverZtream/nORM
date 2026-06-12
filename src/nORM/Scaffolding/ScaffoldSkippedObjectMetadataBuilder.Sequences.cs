#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectMetadataBuilder
    {
        private static IReadOnlyDictionary<string, object?> BuildSequenceMetadata(ScaffoldSkippedObjectInfo obj)
        {
            var provider = ParseSequenceProvider(obj.Detail);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = FormatSequenceProvider(provider),
                ["stubSupported"] = provider is "sqlserver" or "postgres",
                ["clrType"] = ScaffoldTypeNameHelper.GetTypeName(MapSequenceValueType(obj.Detail), allowNull: false)
            };

            var dataType = ParseSemicolonValue(obj.Detail, "dataType");
            if (!string.IsNullOrWhiteSpace(dataType))
                metadata["dataType"] = dataType;

            return metadata;
        }

        private static string FormatSequenceProvider(string provider)
            => provider switch
            {
                "sqlserver" => "SQL Server",
                "postgres" => "PostgreSQL",
                _ => provider
            };

        private static string ParseSequenceProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase))
                return "sqlserver";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            return string.Empty;
        }

        private static Type MapSequenceValueType(string detail)
        {
            var dataType = ParseSemicolonValue(detail, "dataType");
            var normalized = dataType.Split('(', 2)[0].Trim().ToLowerInvariant();
            return normalized switch
            {
                "tinyint" => typeof(byte),
                "smallint" => typeof(short),
                "int" or "integer" => typeof(int),
                "bigint" => typeof(long),
                "decimal" or "numeric" => typeof(decimal),
                _ => typeof(long)
            };
        }
    }
}
