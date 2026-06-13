#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldMySqlTypeClassifier
    {
        private const int MaxScaffoldedMySqlSetValueCount = 8;

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
            if (!TryGetMySqlQuotedTypeBody(detail, typeName, out var body))
                return false;

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

        private static bool TryGetMySqlQuotedTypeBody(string? detail, string typeName, out string body)
        {
            body = string.Empty;
            if (string.IsNullOrWhiteSpace(detail) || string.IsNullOrWhiteSpace(typeName))
                return false;

            var trimmed = detail.Trim();
            if (!trimmed.StartsWith(typeName, StringComparison.OrdinalIgnoreCase))
                return false;

            var open = typeName.Length;
            while (open < trimmed.Length && char.IsWhiteSpace(trimmed[open]))
                open++;

            if (open >= trimmed.Length || trimmed[open] != '(' || !trimmed.EndsWith(")", StringComparison.Ordinal))
                return false;

            body = trimmed.Substring(open + 1, trimmed.Length - open - 2);
            return true;
        }
    }
}
