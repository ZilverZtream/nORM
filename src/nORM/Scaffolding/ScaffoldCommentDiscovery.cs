#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldCommentDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, ScaffoldComments>> GetScaffoldCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(table => TableKey(table.Schema, table.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (tableKeys.Count == 0 || ScaffoldProviderKind.IsSqlite(provider))
                return new Dictionary<string, ScaffoldComments>(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlServer(provider))
            {
                return await QueryScaffoldCommentsAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(o.schema_id) AS TableSchema,
                           o.name AS TableName,
                           CAST(table_comment.value AS nvarchar(max)) AS TableComment,
                           c.name AS ColumnName,
                           CAST(column_comment.value AS nvarchar(max)) AS ColumnComment
                    FROM sys.objects o
                    INNER JOIN sys.columns c ON c.object_id = o.object_id
                    LEFT JOIN sys.extended_properties table_comment
                      ON table_comment.major_id = o.object_id
                     AND table_comment.minor_id = 0
                     AND table_comment.name = N'MS_Description'
                    LEFT JOIN sys.extended_properties column_comment
                      ON column_comment.major_id = o.object_id
                     AND column_comment.minor_id = c.column_id
                     AND column_comment.name = N'MS_Description'
                    WHERE o.is_ms_shipped = 0
                      AND o.type IN ('U', 'V')
                    UNION ALL
                    SELECT SCHEMA_NAME(syn.schema_id) AS TableSchema,
                           syn.name AS TableName,
                           CAST(table_comment.value AS nvarchar(max)) AS TableComment,
                           c.name AS ColumnName,
                           CAST(column_comment.value AS nvarchar(max)) AS ColumnComment
                    FROM sys.synonyms syn
                    INNER JOIN sys.objects base_object
                      ON base_object.object_id = OBJECT_ID(syn.base_object_name)
                    INNER JOIN sys.columns c ON c.object_id = base_object.object_id
                    LEFT JOIN sys.extended_properties table_comment
                      ON table_comment.major_id = base_object.object_id
                     AND table_comment.minor_id = 0
                     AND table_comment.name = N'MS_Description'
                    LEFT JOIN sys.extended_properties column_comment
                      ON column_comment.major_id = base_object.object_id
                     AND column_comment.minor_id = c.column_id
                     AND column_comment.name = N'MS_Description'
                    WHERE base_object.is_ms_shipped = 0
                      AND base_object.type IN ('U', 'V')
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsPostgres(provider))
            {
                return await QueryScaffoldCommentsAsync(connection, tableKeys, """
                    SELECT n.nspname AS TableSchema,
                           cls.relname AS TableName,
                           obj_description(cls.oid, 'pg_class') AS TableComment,
                           a.attname AS ColumnName,
                           col_description(cls.oid, a.attnum) AS ColumnComment
                    FROM pg_class cls
                    INNER JOIN pg_namespace n ON n.oid = cls.relnamespace
                    INNER JOIN pg_attribute a ON a.attrelid = cls.oid
                    WHERE cls.relkind IN ('r', 'p', 'v', 'm')
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    """).ConfigureAwait(false);
            }

            if (ScaffoldProviderKind.IsMySql(provider))
            {
                return await QueryScaffoldCommentsAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema,
                           t.table_name AS TableName,
                           CASE WHEN t.table_type = 'BASE TABLE' THEN t.table_comment ELSE NULL END AS TableComment,
                           c.column_name AS ColumnName,
                           c.column_comment AS ColumnComment
                    FROM information_schema.tables t
                    INNER JOIN information_schema.columns c
                      ON c.table_schema = t.table_schema
                     AND c.table_name = t.table_name
                    WHERE t.table_schema = DATABASE()
                      AND t.table_type IN ('BASE TABLE', 'VIEW')
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, ScaffoldComments>(StringComparer.OrdinalIgnoreCase);
        }

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
