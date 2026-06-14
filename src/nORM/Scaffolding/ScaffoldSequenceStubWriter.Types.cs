#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSequenceStubWriter
    {
        private static string ParseSequenceProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase))
                return "sqlserver";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            return string.Empty;
        }

        private static string MapSequenceValueTypeName(string detail)
        {
            var dataType = ParseSemicolonValue(detail, "dataType");
            var normalized = dataType.Split('(', 2)[0].Trim().ToLowerInvariant();
            return normalized switch
            {
                "tinyint" => "byte",
                "smallint" => "short",
                "int" or "integer" => "int",
                "bigint" => "long",
                "decimal" or "numeric" => "decimal",
                _ => "long"
            };
        }

        private static string ParseSemicolonValue(string detail, string key)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            return values.TryGetValue(key, out var value) ? value : string.Empty;
        }
    }
}
