#nullable enable
using System;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
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
    }
}
