using System;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {
        private static string NewGuidSql()
            => "(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(6))))";

        private static string? TryTranslateParseAndGuidFunction(string name, Type declaringType, string[] args)
        {
            // Numeric Parse(string) -- common pattern where numeric values
            // are stored in a TEXT column and need integer/decimal semantics
            // for projection or downstream arithmetic. SQLite CAST AS INTEGER
            // / REAL handles the text->number conversion natively (returns 0
            // for non-numeric text, matching SQLite's coercion -- not .NET's
            // FormatException semantic but the closest SQL equivalent).
            if (declaringType == typeof(int)
                || declaringType == typeof(long)
                || declaringType == typeof(short)
                || declaringType == typeof(byte)
                || declaringType == typeof(double)
                || declaringType == typeof(float)
                || declaringType == typeof(decimal))
            {
                if (name == "Parse" && args.Length == 1)
                {
                    var sqlType = declaringType == typeof(double) || declaringType == typeof(float) || declaringType == typeof(decimal)
                        ? "REAL"
                        : "INTEGER";
                    return $"CAST({args[0]} AS {sqlType})";
                }
            }

            // bool.Parse(string) -- .NET semantics are case-insensitive
            // ("True"/"true"/"TRUE" -> true; "False"/"false"/"FALSE" ->
            // false). SQLite returns 0/1 INTEGER from a boolean expression
            // which the materializer converts to bool via the column type.
            if (declaringType == typeof(bool) && name == "Parse" && args.Length == 1)
            {
                return $"(LOWER({args[0]}) = 'true')";
            }

            // Guid.Parse(string) -- SQLite stores Guid as canonical
            // 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' text and Microsoft.Data
            // .Sqlite's GetGuid parses it directly. Identity emission.
            if (declaringType == typeof(Guid) && name == "Parse" && args.Length == 1)
            {
                return args[0];
            }

            if (declaringType == typeof(Guid) && name == nameof(Guid.NewGuid) && args.Length == 0)
            {
                return NewGuidSql();
            }

            return null;
        }

        private static string? TryTranslateNormFunction(string name, Type declaringType, string[] args)
        {
            if (declaringType != typeof(NormFunctions))
            {
                return null;
            }

            return name switch
            {
                // SQLite LIKE is case-insensitive for ASCII by default. Force the
                // case-fold explicitly so callers can rely on consistent semantics
                // when collations or PRAGMA case_sensitive_like change.
                nameof(NormFunctions.ILike) when args.Length == 2 => $"(LOWER({args[0]}) LIKE LOWER({args[1]}))",
                // Server-side primitives. SQLite datetime('now') returns
                // 'YYYY-MM-DD HH:MM:SS' in UTC; randomblob(16) returns a
                // 16-byte blob compatible with .NET Guid via the
                // GetFieldValue<Guid>() reader path; random()/9.22e18
                // squeezes the 64-bit signed result into [0, 1).
                nameof(NormFunctions.ServerUtcNow) when args.Length == 0 => "datetime('now')",
                nameof(NormFunctions.ServerNewGuid) when args.Length == 0 => NewGuidSql(),
                nameof(NormFunctions.ServerRandom) when args.Length == 0 => "(ABS(random()) / 9223372036854775808.0)",
                _ => null
            };
        }
    }
}
