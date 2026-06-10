#nullable enable
using System;
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqlStringLiteralParser
    {
        public static bool TryParseSqlStringLiteralList(string body, out string[] values)
        {
            values = Array.Empty<string>();
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
    }
}
