#nullable enable
using System;
using System.Data;
using System.Globalization;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineTypeMapper
    {
        public static (byte? Precision, byte? Scale) GetRoutineParameterPrecisionScale(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return (null, null);

            var trimmed = dataType.Trim();
            var open = trimmed.IndexOf('(');
            if (open < 0)
                return (null, null);

            var close = trimmed.IndexOf(')', open + 1);
            if (close < 0)
                return (null, null);

            var normalized = trimmed[..open].Trim().ToLowerInvariant();
            if (normalized is not ("decimal" or "numeric"))
                return (null, null);

            var parts = trimmed.Substring(open + 1, close - open - 1)
                .Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length is not (1 or 2)
                || parts.Any(static part => part.Length == 0))
                return (null, null);

            if (!byte.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var precision)
                || precision == 0)
            {
                return (null, null);
            }

            if (parts.Length == 1)
                return (precision, null);

            return byte.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var scale)
                   && scale <= precision
                ? (precision, scale)
                : (null, null);
        }

        public static string GetRoutineParameterDirection(string? mode)
            => mode?.Trim().ToUpperInvariant() switch
            {
                "INOUT" => nameof(ParameterDirection.InputOutput),
                "RETURN" => nameof(ParameterDirection.ReturnValue),
                _ => nameof(ParameterDirection.Output)
            };

        public static int? GetRoutineParameterSize(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return null;

            var trimmed = dataType.Trim();
            var open = trimmed.IndexOf('(');
            if (open < 0)
                return null;

            var close = trimmed.IndexOf(')', open + 1);
            if (close < 0)
                return null;

            var normalized = trimmed[..open].Trim().ToLowerInvariant();
            if (normalized is "character varying" or "varying character")
                normalized = "varchar";
            else if (normalized is "national character varying")
                normalized = "nvarchar";
            else if (normalized is "character")
                normalized = "char";

            if (normalized is not ("char" or "varchar" or "nchar" or "nvarchar" or "binary" or "varbinary"))
                return null;

            var value = trimmed.Substring(open + 1, close - open - 1).Trim();
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var size) && size > 0
                ? size
                : null;
        }
    }
}
