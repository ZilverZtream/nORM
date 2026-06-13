#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCommentDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, ScaffoldComments>> QueryScaffoldCommentsAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            var mutable = new Dictionary<string, (string? TableComment, Dictionary<string, string> ColumnComments)>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                if (string.IsNullOrWhiteSpace(tableName))
                    continue;

                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName!);
                if (!tableKeys.Contains(tableKey))
                    continue;

                if (!mutable.TryGetValue(tableKey, out var comments))
                    comments = (null, new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));

                var tableComment = NullIfWhiteSpace(Convert.ToString(reader["TableComment"]));
                if (comments.TableComment is null && tableComment is not null)
                    comments.TableComment = tableComment;

                var columnName = Convert.ToString(reader["ColumnName"]);
                var columnComment = NullIfWhiteSpace(Convert.ToString(reader["ColumnComment"]));
                if (!string.IsNullOrWhiteSpace(columnName) && columnComment is not null)
                    comments.ColumnComments[columnName!] = columnComment;

                mutable[tableKey] = comments;
            }

            var result = new Dictionary<string, ScaffoldComments>(StringComparer.OrdinalIgnoreCase);
            foreach (var pair in mutable)
            {
                if (pair.Value.TableComment is null && pair.Value.ColumnComments.Count == 0)
                    continue;

                result[pair.Key] = new ScaffoldComments(pair.Value.TableComment, pair.Value.ColumnComments);
            }

            return result;
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
