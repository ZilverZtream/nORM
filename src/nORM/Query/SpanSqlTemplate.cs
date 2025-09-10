using System;

namespace nORM.Query
{
    internal static class SpanSqlTemplate
    {
        public static ReadOnlySpan<char> SimpleSelect => "SELECT {0} FROM {1}";
        public static ReadOnlySpan<char> SimpleWhere => " WHERE {0} = {1}";
        public static ReadOnlySpan<char> SimpleTake => " LIMIT {0}";

        /// <summary>
        /// Constructs a simple <c>SELECT</c> statement directly into a preallocated character buffer.
        /// </summary>
        /// <param name="buffer">Destination buffer that receives the generated SQL.</param>
        /// <param name="columns">Comma-separated list of column names.</param>
        /// <param name="table">The table to select from.</param>
        /// <param name="whereClause">Optional predicate to append after a <c>WHERE</c>.</param>
        /// <returns>The number of characters written into <paramref name="buffer"/>.</returns>
        public static int BuildSimpleQuery(Span<char> buffer, ReadOnlySpan<char> columns,
            ReadOnlySpan<char> table, ReadOnlySpan<char> whereClause = default)
        {
            var writer = new SpanSqlBuilder(buffer);
            writer.AppendLiteral("SELECT ");
            writer.AppendLiteral(columns);
            writer.AppendLiteral(" FROM ");
            writer.AppendLiteral(table);

            if (!whereClause.IsEmpty)
            {
                writer.AppendLiteral(" WHERE ");
                writer.AppendLiteral(whereClause);
            }

            return writer.Position;
        }
    }
}
