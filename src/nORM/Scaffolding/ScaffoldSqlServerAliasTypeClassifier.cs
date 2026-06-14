#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqlServerAliasTypeClassifier
    {
        private static readonly IReadOnlyDictionary<string, Type> SafeAliasBaseClrTypes =
            new Dictionary<string, Type>(StringComparer.Ordinal)
            {
                ["int"] = typeof(int),
                ["bigint"] = typeof(long),
                ["smallint"] = typeof(short),
                ["tinyint"] = typeof(byte),
                ["bit"] = typeof(bool),
                ["decimal"] = typeof(decimal),
                ["numeric"] = typeof(decimal),
                ["money"] = typeof(decimal),
                ["smallmoney"] = typeof(decimal),
                ["float"] = typeof(double),
                ["real"] = typeof(float),
                ["date"] = typeof(DateOnly),
                ["time"] = typeof(TimeOnly),
                ["datetime"] = typeof(DateTime),
                ["datetime2"] = typeof(DateTime),
                ["smalldatetime"] = typeof(DateTime),
                ["datetimeoffset"] = typeof(DateTimeOffset),
                ["uniqueidentifier"] = typeof(Guid),
                ["sysname"] = typeof(string),
                ["char"] = typeof(string),
                ["varchar"] = typeof(string),
                ["nchar"] = typeof(string),
                ["nvarchar"] = typeof(string),
                ["text"] = typeof(string),
                ["ntext"] = typeof(string),
                ["xml"] = typeof(string),
                ["binary"] = typeof(byte[]),
                ["varbinary"] = typeof(byte[]),
                ["image"] = typeof(byte[])
            };

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
            => SafeAliasBaseClrTypes.ContainsKey(NormalizeSqlServerAliasBaseTypeName(typeText));

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

            if (SafeAliasBaseClrTypes.TryGetValue(NormalizeSqlServerAliasBaseTypeName(typeText), out var mappedType))
            {
                type = mappedType;
                return true;
            }

            return false;
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
