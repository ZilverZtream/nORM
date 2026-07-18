#nullable enable

namespace nORM.Query
{
    /// <summary>
    /// Implemented by a query root whose source is a raw SQL statement (see
    /// <c>nORM.Core.NormRawSqlQueryable{T}</c>). The query translator recognises this at the root of the
    /// expression tree and substitutes the SQL — wrapped as a derived table — for the mapped table's
    /// <c>FROM</c> clause, so ordinary LINQ operators compose on top.
    /// </summary>
    internal interface INormRawSqlSource
    {
        /// <summary>The raw SQL statement that produces the source rows.</summary>
        string RawSql { get; }

        /// <summary>Parameter values referenced positionally (<c>@p0</c>, <c>@p1</c>, …) by <see cref="RawSql"/>.</summary>
        object?[] RawParameters { get; }
    }
}
