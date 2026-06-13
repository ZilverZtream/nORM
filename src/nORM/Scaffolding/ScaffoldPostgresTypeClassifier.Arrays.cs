#nullable enable
using System;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            return TryGetPostgresArrayElementType(normalized, out var element)
                   && TryNormalizePostgresArrayElementCastType(element, out castType);
        }

        private static bool TryNormalizePostgresArrayElementCastType(string element, out string castType)
        {
            element = element.Trim().TrimStart('_');
            if (TryNormalizePostgresParameterizedProbeCastType(element, out var parameterizedCastType))
            {
                castType = StripPostgresTypeArguments(parameterizedCastType) + "[]";
                return true;
            }

            castType = element switch
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

            return TryMapPostgresArrayCastType(normalized, out arrayType);
        }

        public static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (TryMapPostgresArrayProbeCastType(normalized, out var canonicalCastType))
                normalized = canonicalCastType;

            if (!TryGetPostgresArrayElementType(normalized, out var element)
                || !TryMapPostgresArrayElementClrType(element, out var elementType))
            {
                return false;
            }

            arrayType = elementType.MakeArrayType();
            return true;
        }

        private static bool TryMapPostgresArrayElementClrType(string element, out Type elementType)
        {
            elementType = typeof(object);
            element = element.Trim().TrimStart('_');
            if (TryNormalizePostgresParameterizedProbeCastType(element, out var parameterizedCastType))
                element = StripPostgresTypeArguments(parameterizedCastType);

            var mappedType = element switch
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
                "time" or "time without time zone" => typeof(TimeOnly),
                "time with time zone" or "timetz" => typeof(DateTimeOffset),
                "interval" => typeof(TimeSpan),
                "timestamp" or "timestamp without time zone" => typeof(DateTime),
                "timestamptz" or "timestamp with time zone" => typeof(DateTimeOffset),
                _ => null
            };

            if (mappedType is null)
                return false;

            elementType = mappedType;
            return true;
        }

        private static bool TryGetPostgresArrayElementType(string normalized, out string element)
        {
            element = string.Empty;
            normalized = normalized.Trim();
            if (normalized.EndsWith("[]", StringComparison.Ordinal))
            {
                element = normalized[..^2].Trim().TrimStart('_');
                return element.Length > 0;
            }

            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var afterKeyword = "array".Length;
            if (afterKeyword < normalized.Length
                && !char.IsWhiteSpace(normalized[afterKeyword])
                && normalized[afterKeyword] != '(')
            {
                return false;
            }

            var open = normalized.IndexOf('(', afterKeyword);
            if (open < 0)
                return false;

            var close = FindMatchingPostgresTypeParenthesis(normalized, open);
            if (close <= open)
                return false;

            element = normalized.Substring(open + 1, close - open - 1).Trim().TrimStart('_');
            return element.Length > 0;
        }

        private static int FindMatchingPostgresTypeParenthesis(string value, int open)
        {
            var depth = 0;
            for (var i = open; i < value.Length; i++)
            {
                if (value[i] == '(')
                {
                    depth++;
                }
                else if (value[i] == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        private static string StripPostgresTypeArguments(string typeText)
        {
            var open = typeText.IndexOf('(');
            return open < 0 ? typeText : typeText[..open].Trim();
        }
    }
}
