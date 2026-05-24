using System;

namespace nORM.Query
{
    /// <summary>
    /// Provider-aware database functions exposed as static methods for use inside LINQ
    /// expressions. These translate to the corresponding SQL operators directly — no
    /// pattern escaping is performed, so callers control the LIKE / ILIKE pattern
    /// exactly. The runtime implementations throw because these are SQL-only.
    /// </summary>
    public static class NormFunctions
    {
        /// <summary>
        /// Pattern-matches <paramref name="value"/> against the SQL LIKE pattern
        /// <paramref name="pattern"/>. Use the underlying provider's wildcard semantics —
        /// `%` for any sequence, `_` for any single character.
        /// </summary>
        [SqlFunction("({0} LIKE {1})")]
        public static bool Like(string value, string pattern)
            => throw new InvalidOperationException(
                $"{nameof(NormFunctions)}.{nameof(Like)} can only be used inside a LINQ query translated to SQL.");

        /// <summary>
        /// Case-insensitive pattern match. PostgreSQL emits native ILIKE; SQL Server, SQLite,
        /// and MySQL emit `LOWER(value) LIKE LOWER(pattern)`. As with Like, no automatic
        /// LIKE-pattern escaping is performed — the caller controls `%` and `_`.
        /// </summary>
        public static bool ILike(string value, string pattern)
            => throw new InvalidOperationException(
                $"{nameof(NormFunctions)}.{nameof(ILike)} can only be used inside a LINQ query translated to SQL.");
    }
}
