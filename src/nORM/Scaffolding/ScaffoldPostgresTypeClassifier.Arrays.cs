#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            if (normalized.EndsWith("[]", StringComparison.Ordinal))
                return TryNormalizePostgresArrayElementCastType(normalized[..^2].Trim(), out castType);

            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            if (open < 0)
                return false;

            var close = normalized.IndexOf(')', open + 1);
            var element = (close > open
                    ? normalized.Substring(open + 1, close - open - 1)
                    : normalized[(open + 1)..])
                .Trim()
                .TrimStart('_');

            return TryNormalizePostgresArrayElementCastType(element, out castType);
        }

        private static bool TryNormalizePostgresArrayElementCastType(string element, out string castType)
        {
            castType = element.Trim().TrimStart('_') switch
            {
                "int2" or "smallint" => "smallint[]",
                "int4" or "integer" => "integer[]",
                "int8" or "bigint" => "bigint[]",
                "float4" or "real" => "real[]",
                "float8" or "double precision" => "double precision[]",
                "numeric" or "decimal" => "numeric[]",
                "bool" or "boolean" => "boolean[]",
                "uuid" => "uuid[]",
                "text" => "text[]",
                "varchar" or "character varying" => "character varying[]",
                "bpchar" or "char" or "character" => "character[]",
                "citext" => "citext[]",
                "bytea" => "bytea[]",
                "date" => "date[]",
                "time" or "time without time zone" => "time without time zone[]",
                "timetz" or "time with time zone" => "time with time zone[]",
                "interval" => "interval[]",
                "timestamp" or "timestamp without time zone" => "timestamp without time zone[]",
                "timestamptz" or "timestamp with time zone" => "timestamp with time zone[]",
                _ => string.Empty
            };
            return castType.Length > 0;
        }

        public static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = detail.Trim().ToLowerInvariant();
            if (normalized.Contains("domain", StringComparison.OrdinalIgnoreCase))
                normalized = GetPostgresDomainProbeCastType(detail).Trim().ToLowerInvariant();

            if (TryMapPostgresArrayCastType(normalized, out arrayType))
                return true;

            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            var close = normalized.IndexOf(')', open + 1);
            var element = open >= 0 && close > open
                ? normalized.Substring(open + 1, close - open - 1).Trim()
                : string.Empty;
            element = element.TrimStart('_');

            var elementType = element switch
            {
                "int2" or "smallint" => typeof(short),
                "int4" or "integer" => typeof(int),
                "int8" or "bigint" => typeof(long),
                "float4" or "real" => typeof(float),
                "float8" or "double precision" => typeof(double),
                "numeric" or "decimal" => typeof(decimal),
                "bool" or "boolean" => typeof(bool),
                "uuid" => typeof(Guid),
                "text" or "varchar" or "character varying" or "bpchar" or "char" or "character" or "citext" => typeof(string),
                "bytea" => typeof(byte[]),
                "date" => typeof(DateOnly),
                "time" or "time without time zone" or "time with time zone" => typeof(TimeOnly),
                "interval" => typeof(TimeSpan),
                "timestamp" or "timestamp without time zone" => typeof(DateTime),
                "timestamptz" or "timestamp with time zone" => typeof(DateTimeOffset),
                _ => null
            };

            if (elementType is null)
                return false;

            arrayType = elementType.MakeArrayType();
            return true;
        }

        public static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (TryMapPostgresArrayProbeCastType(normalized, out var canonicalCastType))
                normalized = canonicalCastType;

            if (!normalized.EndsWith("[]", StringComparison.Ordinal))
                return false;

            var element = normalized[..^2].Trim();
            var elementType = element switch
            {
                "smallint" => typeof(short),
                "integer" => typeof(int),
                "bigint" => typeof(long),
                "real" => typeof(float),
                "double precision" => typeof(double),
                "numeric" => typeof(decimal),
                "boolean" => typeof(bool),
                "uuid" => typeof(Guid),
                "text" or "character varying" or "character" or "citext" => typeof(string),
                "bytea" => typeof(byte[]),
                "date" => typeof(DateOnly),
                "time without time zone" or "time with time zone" => typeof(TimeOnly),
                "interval" => typeof(TimeSpan),
                "timestamp without time zone" => typeof(DateTime),
                "timestamp with time zone" => typeof(DateTimeOffset),
                _ => null
            };

            if (elementType is null)
                return false;

            arrayType = elementType.MakeArrayType();
            return true;
        }
    }
}
