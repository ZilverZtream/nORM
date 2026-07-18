using System;
using nORM.Query;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// A composable query whose source is a raw SQL statement rather than a mapped table. The SQL is wrapped
    /// as a derived table (<c>FROM (&lt;sql&gt;) AS t</c>) so ordinary LINQ operators — <c>Where</c>,
    /// <c>OrderBy</c>, <c>Skip</c>/<c>Take</c>, projections, <c>Count</c> — compose on top, and the entity's
    /// global/tenant filters are applied to the outer query (the raw SQL must expose the entity's columns).
    /// Produced by <see cref="DbContext.FromSqlRaw{T}(string, object[])"/> and
    /// <see cref="DbContext.FromSqlInterpolated{T}(FormattableString)"/>; the query translator recognises this
    /// root and substitutes the derived table for the mapped table's <c>FROM</c> clause.
    /// </summary>
    internal sealed class NormRawSqlQueryable<T> : NormQueryableBase<T>, INormRawSqlSource where T : class
    {
        /// <summary>The raw SQL statement that produces the source rows.</summary>
        public string RawSql { get; }

        /// <summary>The parameter values referenced positionally by <see cref="RawSql"/>.</summary>
        public object?[] RawParameters { get; }

        public NormRawSqlQueryable(DbContext ctx, string rawSql, object?[] rawParameters)
            : base(ctx)
        {
            RawSql = rawSql ?? throw new ArgumentNullException(nameof(rawSql));
            RawParameters = rawParameters ?? Array.Empty<object?>();
        }
    }
}
