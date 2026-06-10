#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldMySqlTypeClassifier
    {
        private const int MaxScaffoldedMySqlSetValueCount = 8;

        public static bool IsSafeMySqlUnsignedDecimalType(string? detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = NormalizeMySqlUnsignedTypeDetail(detail);
            return normalized is "decimal unsigned" or "numeric unsigned";
        }

        public static bool TryParseMySqlEnumValues(string? detail, out string[] values)
            => TryParseMySqlQuotedTypeValues(detail, "enum", out values);

        public static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
        {
            if (!TryParseMySqlQuotedTypeValues(detail, "set", out values)
                || values.Length == 0
                || values.Length > MaxScaffoldedMySqlSetValueCount
                || values.Any(static value => value.Length == 0 || value.Contains(','))
                || values.Distinct(StringComparer.Ordinal).Count() != values.Length)
            {
                values = Array.Empty<string>();
                return false;
            }

            return true;
        }

        public static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
        {
            values = Array.Empty<string>();
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith(typeName + "(", StringComparison.OrdinalIgnoreCase) || !trimmed.EndsWith(")", StringComparison.Ordinal))
                return false;

            var body = trimmed.Substring(trimmed.IndexOf('(') + 1, trimmed.Length - trimmed.IndexOf('(') - 2);
            var parsed = new List<string>();
            var current = new StringBuilder();
            var expectingValue = true;
            var i = 0;
            while (i < body.Length)
            {
                while (i < body.Length && char.IsWhiteSpace(body[i]))
                    i++;

                if (i >= body.Length)
                    break;

                if (!expectingValue)
                {
                    if (body[i] != ',')
                        return false;

                    expectingValue = true;
                    i++;
                    continue;
                }

                if (body[i] != '\'')
                    return false;

                i++;
                current.Clear();
                var closed = false;
                for (; i < body.Length; i++)
                {
                    var ch = body[i];
                    if (ch == '\\' && i + 1 < body.Length)
                    {
                        current.Append(body[++i]);
                        continue;
                    }

                    if (ch == '\'')
                    {
                        if (i + 1 < body.Length && body[i + 1] == '\'')
                        {
                            current.Append('\'');
                            i++;
                            continue;
                        }

                        parsed.Add(current.ToString());
                        expectingValue = false;
                        closed = true;
                        i++;
                        break;
                    }

                    current.Append(ch);
                }

                if (!closed)
                    return false;
            }

            if (expectingValue || parsed.Count == 0)
                return false;

            values = parsed.ToArray();
            return true;
        }

        public static bool TryMapMySqlUnsignedType(string? detail, out Type type)
        {
            type = typeof(object);
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = NormalizeMySqlUnsignedTypeDetail(detail);
            if (!normalized.Contains("unsigned", StringComparison.Ordinal))
                return false;

            type = normalized switch
            {
                "tinyint unsigned" => typeof(byte),
                "smallint unsigned" => typeof(ushort),
                "mediumint unsigned" or "int unsigned" or "integer unsigned" => typeof(uint),
                "bigint unsigned" => typeof(ulong),
                _ => typeof(object)
            };

            return type != typeof(object);
        }

        public static string NormalizeMySqlUnsignedTypeDetail(string detail)
        {
            var normalized = detail.Trim().ToLowerInvariant();
            var unsignedIndex = normalized.IndexOf("unsigned", StringComparison.Ordinal);
            if (unsignedIndex < 0)
                return normalized;

            var baseType = normalized[..unsignedIndex].Trim();
            var paren = baseType.IndexOf('(');
            if (paren >= 0)
                baseType = baseType[..paren].Trim();

            return baseType.Length == 0 ? normalized : baseType + " unsigned";
        }
    }
}
