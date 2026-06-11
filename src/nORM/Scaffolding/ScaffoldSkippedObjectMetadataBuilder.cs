#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSkippedObjectMetadataBuilder
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

        public static bool IsTableLikeSqlServerSynonym(string detail)
        {
            if (!detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return false;

            var baseType = ParseSemicolonValue(detail, "baseType");
            return string.Equals(baseType, "U", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(baseType, "V", StringComparison.OrdinalIgnoreCase);
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

        private static IReadOnlyDictionary<string, object?> BuildEventMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (!obj.Detail.StartsWith("MySQL event", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var values = ScaffoldSemicolonParser.Parse(obj.Detail, out _);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = "MySQL"
            };

            AddMetadataValue(metadata, values, "eventType");
            AddMetadataValue(metadata, values, "status");
            AddMetadataValue(metadata, values, "intervalValue");
            AddMetadataValue(metadata, values, "intervalField");
            AddMetadataValue(metadata, values, "executeAt");
            AddMetadataValue(metadata, values, "starts");
            AddMetadataValue(metadata, values, "ends");
            return metadata;
        }

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

        private static string ParseSemicolonValue(string detail, string key)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            return values.TryGetValue(key, out var value) ? value : string.Empty;
        }

        private static void AddMetadataValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
                metadata[key] = value;
        }
    }
}
