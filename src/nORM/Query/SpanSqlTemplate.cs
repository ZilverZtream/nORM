using System;

namespace nORM.Query
{
    internal static class SpanSqlTemplate
    {
        public static ReadOnlySpan<char> SimpleSelect => "SELECT {0} FROM {1}";
        public static ReadOnlySpan<char> SimpleWhere => " WHERE {0} = {1}";
        public static ReadOnlySpan<char> SimpleTake => " LIMIT {0}";

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
