#nullable enable
using System;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldPostgresTypeClassifier
    {
        public static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => TryNormalizePostgresLengthProbeCastType(normalized, "character varying", "varchar", "character varying", out castType)
               || TryNormalizePostgresLengthProbeCastType(normalized, "character", "char", "character", out castType)
               || TryNormalizePostgresNumericProbeCastType(normalized, out castType);

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

        private static bool TryNormalizePostgresLengthProbeCastType(
            string normalized,
            string typeName,
            string alias,
            string canonicalType,
            out string castType)
        {
            castType = string.Empty;
            if ((!TryParsePostgresTypeArguments(normalized, typeName, out var args)
                    && !TryParsePostgresTypeArguments(normalized, alias, out args))
                || args.Length != 1
                || !int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var length)
                || length <= 0)
            {
                return false;
            }

            castType = canonicalType + "(" + length.ToString(CultureInfo.InvariantCulture) + ")";
            return true;
        }

        private static bool TryNormalizePostgresNumericProbeCastType(string normalized, out string castType)
        {
            castType = string.Empty;
            if ((!TryParsePostgresTypeArguments(normalized, "numeric", out var args)
                    && !TryParsePostgresTypeArguments(normalized, "decimal", out args))
                || (args.Length != 1 && args.Length != 2)
                || !int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                || precision <= 0)
            {
                return false;
            }

            if (args.Length == 1)
            {
                castType = "numeric(" + precision.ToString(CultureInfo.InvariantCulture) + ")";
                return true;
            }

            if (!int.TryParse(args[1], NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                || scale < 0
                || scale > precision)
            {
                return false;
            }

            castType = "numeric("
                       + precision.ToString(CultureInfo.InvariantCulture)
                       + ","
                       + scale.ToString(CultureInfo.InvariantCulture)
                       + ")";
            return true;
        }
    }
}
