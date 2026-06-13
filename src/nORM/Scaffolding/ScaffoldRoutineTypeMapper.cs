#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        private static string NormalizeRoutineDataType(string dataType)
        {
            var normalized = dataType.Trim().ToLowerInvariant();
            if (ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out var postgresArrayCastType))
                return postgresArrayCastType;

            var isUnsigned = normalized.Contains(" unsigned", StringComparison.Ordinal);
            var paren = normalized.IndexOf('(');
            var baseType = paren >= 0 ? normalized[..paren].Trim() : normalized;

            if (paren >= 0 && (baseType == "array" || baseType == "user-defined" || baseType == "table type"))
            {
                var close = normalized.IndexOf(')', paren + 1);
                if (close > paren)
                {
                    var inner = normalized.Substring(paren + 1, close - paren - 1).Trim();
                    if (!string.IsNullOrWhiteSpace(inner))
                    {
                        if (baseType == "array")
                            return "array (" + inner + ")";

                        if (baseType == "table type")
                            return "table type";

                        var userDefined = inner.TrimStart('_');
                        if (userDefined is "citext" or "json" or "jsonb" or "xml" or "uuid")
                            return userDefined;
                    }
                }
            }

            normalized = isUnsigned && !baseType.EndsWith(" unsigned", StringComparison.Ordinal)
                ? baseType + " unsigned"
                : baseType;

            return NormalizeProviderAlias(normalized);
        }

        private static string NormalizeProviderAlias(string normalized)
            => normalized switch
            {
                "integer unsigned" => "int unsigned",
                "character varying" or "varying character" => "varchar",
                "national character varying" => "nvarchar",
                "character" => "char",
                "double precision" => "double",
                "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                "time without time zone" or "time with time zone" => "time",
                _ => normalized
            };
    }
}
