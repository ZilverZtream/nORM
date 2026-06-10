#nullable enable
using System;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldPostgresTypeClassifier
    {
        public static bool RequiresPostgresSchemaProbeCast(string detail)
            => TryGetPostgresSchemaProbeCastType(detail, out _);

        public static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
        {
            if (detail.Contains("DOMAIN", StringComparison.OrdinalIgnoreCase))
            {
                castType = GetPostgresDomainProbeCastType(detail);
                return true;
            }

            if (TryParsePostgresEnumValues(detail, out _))
            {
                castType = "text";
                return true;
            }

            if (detail.StartsWith("USER-DEFINED", StringComparison.OrdinalIgnoreCase))
            {
                castType = TryNormalizeSafePostgresDomainProbeCastType(detail, out var safeCastType)
                    ? safeCastType
                    : "text";
                return true;
            }

            castType = string.Empty;
            return false;
        }

        public static string GetPostgresDomainProbeCastType(string detail)
        {
            return TryGetPostgresDomainBaseTypeText(detail, out var typeText)
                && TryNormalizeSafePostgresDomainProbeCastType(typeText, out var castType)
                ? castType
                : "text";
        }

        public static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
        {
            typeText = string.Empty;
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith("DOMAIN", StringComparison.OrdinalIgnoreCase))
                return false;

            var arrowIndex = trimmed.IndexOf("->", StringComparison.Ordinal);
            if (arrowIndex < 0)
                return false;

            typeText = trimmed[(arrowIndex + 2)..].Trim();
            if (typeText.EndsWith(")", StringComparison.Ordinal))
                typeText = typeText[..^1].Trim();

            var udtSuffixIndex = typeText.LastIndexOf(" (", StringComparison.Ordinal);
            if (udtSuffixIndex >= 0
                && typeText.EndsWith(")", StringComparison.Ordinal)
                && !typeText.StartsWith("ARRAY", StringComparison.OrdinalIgnoreCase)
                && !typeText.StartsWith("ENUM", StringComparison.OrdinalIgnoreCase)
                && !typeText.StartsWith("USER-DEFINED", StringComparison.OrdinalIgnoreCase))
            {
                typeText = typeText[..udtSuffixIndex].Trim();
            }

            return !string.IsNullOrWhiteSpace(typeText);
        }

        public static string NormalizePostgresDomainProbeCastType(string typeText)
            => TryNormalizeSafePostgresDomainProbeCastType(typeText, out var castType)
                ? castType
                : "text";

        public static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
        {
            if (TryParsePostgresEnumValues(typeText, out _))
            {
                castType = "text";
                return true;
            }

            var normalized = typeText.Trim().ToLowerInvariant();
            if (TryNormalizePostgresParameterizedProbeCastType(normalized, out castType))
                return true;

            if (TryMapPostgresArrayProbeCastType(normalized, out var arrayCastType))
            {
                castType = arrayCastType;
                return true;
            }

            if (normalized.StartsWith("user-defined (", StringComparison.Ordinal)
                && normalized.EndsWith(")", StringComparison.Ordinal))
            {
                var inner = normalized.Substring("user-defined (".Length, normalized.Length - "user-defined (".Length - 1).Trim().TrimStart('_');
                castType = inner switch
                {
                    "citext" => "citext",
                    "json" => "json",
                    "jsonb" => "jsonb",
                    "xml" => "xml",
                    "uuid" => "uuid",
                    _ => string.Empty
                };

                return castType.Length > 0;
            }

            castType = normalized switch
            {
                "integer" or "int" or "int4" => "integer",
                "bigint" or "int8" => "bigint",
                "smallint" or "int2" => "smallint",
                "boolean" or "bool" => "boolean",
                "uuid" => "uuid",
                "date" => "date",
                "text" => "text",
                "citext" => "citext",
                "json" => "json",
                "jsonb" => "jsonb",
                "xml" => "xml",
                "character varying" or "varchar" => "character varying",
                "character" or "char" => "character",
                "numeric" or "decimal" => "numeric",
                "real" or "float4" => "real",
                "double precision" or "float8" => "double precision",
                "bytea" => "bytea",
                "timestamp without time zone" or "timestamp" => "timestamp without time zone",
                "timestamp with time zone" or "timestamptz" => "timestamp with time zone",
                "time without time zone" or "time" => "time without time zone",
                "time with time zone" or "timetz" => "time with time zone",
                "interval" => "interval",
                _ => string.Empty
            };

            return castType.Length > 0;
        }

        public static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            if ((TryParsePostgresTypeArguments(normalized, "character varying", out var args)
                    || TryParsePostgresTypeArguments(normalized, "varchar", out args))
                && args.Length == 1
                && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var length)
                && length > 0)
            {
                castType = "character varying(" + length.ToString(CultureInfo.InvariantCulture) + ")";
                return true;
            }

            if ((TryParsePostgresTypeArguments(normalized, "character", out args)
                    || TryParsePostgresTypeArguments(normalized, "char", out args))
                && args.Length == 1
                && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out length)
                && length > 0)
            {
                castType = "character(" + length.ToString(CultureInfo.InvariantCulture) + ")";
                return true;
            }

            if ((TryParsePostgresTypeArguments(normalized, "numeric", out args)
                    || TryParsePostgresTypeArguments(normalized, "decimal", out args))
                && (args.Length == 1 || args.Length == 2)
                && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                && precision > 0)
            {
                if (args.Length == 1)
                {
                    castType = "numeric(" + precision.ToString(CultureInfo.InvariantCulture) + ")";
                    return true;
                }

                if (int.TryParse(args[1], NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                    && scale >= 0
                    && scale <= precision)
                {
                    castType = "numeric(" + precision.ToString(CultureInfo.InvariantCulture) + "," + scale.ToString(CultureInfo.InvariantCulture) + ")";
                    return true;
                }
            }

            return false;
        }

        public static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
        {
            args = Array.Empty<string>();
            if (!normalized.StartsWith(typeName + "(", StringComparison.Ordinal)
                || !normalized.EndsWith(")", StringComparison.Ordinal))
            {
                return false;
            }

            var body = normalized.Substring(typeName.Length + 1, normalized.Length - typeName.Length - 2);
            args = body.Split(',', StringSplitOptions.TrimEntries);
            return args.Length > 0 && args.All(static arg => arg.Length > 0);
        }

        public static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
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

        public static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
        {
            if (!normalized.StartsWith("user-defined (", StringComparison.Ordinal)
                || !normalized.EndsWith(")", StringComparison.Ordinal))
            {
                return false;
            }

            var inner = normalized.Substring("user-defined (".Length, normalized.Length - "user-defined (".Length - 1).Trim().TrimStart('_');
            return inner is "citext" or "json" or "jsonb" or "xml" or "uuid";
        }

        public static bool IsSafePostgresDomainColumnType(string? detail)
            => TryGetPostgresDomainBaseTypeText(detail, out var typeText)
               && TryNormalizeSafePostgresDomainProbeCastType(typeText, out _);

        public static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
        {
            values = Array.Empty<string>();
            return TryGetPostgresDomainBaseTypeText(detail, out var typeText)
                   && TryParsePostgresEnumValues(typeText, out values);
        }

        public static bool TryParsePostgresEnumValues(string? detail, out string[] values)
        {
            values = Array.Empty<string>();
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith("ENUM (", StringComparison.OrdinalIgnoreCase) || !trimmed.EndsWith(")", StringComparison.Ordinal))
                return false;

            var colon = trimmed.IndexOf(':');
            if (colon < 0)
                return false;

            var valueList = trimmed.Substring(colon + 1, trimmed.Length - colon - 2).Trim();
            return ScaffoldSqlStringLiteralParser.TryParseSqlStringLiteralList(valueList, out values);
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
