#nullable enable
using System;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqlServerAliasTypeClassifier
    {
        public static bool IsSafeSqlServerAliasColumnType(string? detail)
            => TryGetSqlServerAliasBaseTypeText(detail, out var typeText)
               && IsSafeSqlServerAliasBaseType(typeText);

        public static bool TryGetSqlServerAliasBaseTypeText(string? detail, out string typeText)
        {
            typeText = string.Empty;
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            const string prefix = "user-defined type (";
            var trimmed = detail.Trim();
            if (!trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                || !trimmed.EndsWith(")", StringComparison.Ordinal))
            {
                return false;
            }

            var body = trimmed.Substring(prefix.Length, trimmed.Length - prefix.Length - 1);
            var arrowIndex = body.LastIndexOf("->", StringComparison.Ordinal);
            if (arrowIndex < 0)
                return false;

            typeText = body[(arrowIndex + 2)..].Trim();
            return typeText.Length > 0;
        }

        public static bool IsSafeSqlServerAliasBaseType(string typeText)
        {
            var normalized = NormalizeSqlServerAliasBaseTypeName(typeText);
            return normalized is "int"
                or "bigint"
                or "smallint"
                or "tinyint"
                or "bit"
                or "decimal"
                or "numeric"
                or "money"
                or "smallmoney"
                or "float"
                or "real"
                or "date"
                or "time"
                or "datetime"
                or "datetime2"
                or "smalldatetime"
                or "datetimeoffset"
                or "uniqueidentifier"
                or "sysname"
                or "char"
                or "varchar"
                or "nchar"
                or "nvarchar"
                or "text"
                or "ntext"
                or "xml"
                or "binary"
                or "varbinary"
                or "image";
        }

        public static bool TryMapSqlServerAliasBaseClrType(string? detail, out Type type)
        {
            if (!TryGetSqlServerAliasBaseTypeText(detail, out var typeText)
                || !TryMapSqlServerAliasBaseClrTypeName(typeText, out type))
            {
                type = typeof(object);
                return false;
            }

            return true;
        }

        public static bool TryMapSqlServerAliasBaseClrTypeName(string? typeText, out Type type)
        {
            type = typeof(object);
            if (string.IsNullOrWhiteSpace(typeText))
                return false;

            var normalized = NormalizeSqlServerAliasBaseTypeName(typeText);
            type = normalized switch
            {
                "int" => typeof(int),
                "bigint" => typeof(long),
                "smallint" => typeof(short),
                "tinyint" => typeof(byte),
                "bit" => typeof(bool),
                "decimal" or "numeric" or "money" or "smallmoney" => typeof(decimal),
                "float" => typeof(double),
                "real" => typeof(float),
                "date" => typeof(DateOnly),
                "time" => typeof(TimeOnly),
                "datetime" or "datetime2" or "smalldatetime" => typeof(DateTime),
                "datetimeoffset" => typeof(DateTimeOffset),
                "uniqueidentifier" => typeof(Guid),
                "sysname" => typeof(string),
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "xml" => typeof(string),
                "binary" or "varbinary" or "image" => typeof(byte[]),
                _ => typeof(object)
            };

            return type != typeof(object);
        }

        public static string NormalizeSqlServerAliasBaseTypeName(string typeText)
        {
            var normalized = typeText.Trim().ToLowerInvariant();
            var parenIndex = normalized.IndexOf('(');
            if (parenIndex >= 0)
                normalized = normalized[..parenIndex].Trim();

            return normalized;
        }

        public static int? GetSqlServerAliasBaseMaxLength(string? detail)
            => TryGetSqlServerAliasBaseTypeText(detail, out var typeText)
                ? GetSqlServerAliasBaseMaxLengthFromTypeText(typeText)
                : null;

        public static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string typeText)
        {
            var normalized = typeText.Trim().ToLowerInvariant();
            var parenIndex = normalized.IndexOf('(');
            if (parenIndex < 0 || !normalized.EndsWith(")", StringComparison.Ordinal))
                return null;

            var baseType = normalized[..parenIndex].Trim();
            if (baseType is not ("char" or "varchar" or "nchar" or "nvarchar" or "binary" or "varbinary"))
                return null;

            var lengthText = normalized.Substring(parenIndex + 1, normalized.Length - parenIndex - 2).Trim();
            return int.TryParse(lengthText, NumberStyles.None, CultureInfo.InvariantCulture, out var length)
                   && length > 0
                   && length < int.MaxValue
                ? length
                : null;
        }
    }
}
