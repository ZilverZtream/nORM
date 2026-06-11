#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSkippedObjectQuery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> QuerySkippedObjectsAsync(DbConnection connection, string sql)
        {
            var objects = new List<ScaffoldSkippedObjectInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var objectName = Convert.ToString(reader["ObjectName"]);
                if (string.IsNullOrWhiteSpace(objectName))
                    continue;

                objects.Add(new ScaffoldSkippedObjectInfo(
                    NullIfWhiteSpace(Convert.ToString(reader["ObjectSchema"])),
                    objectName,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty,
                    null));
            }

            return objects;
        }

        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> AttachSkippedObjectCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldSkippedObjectInfo> objects)
        {
            if (objects.Count == 0)
                return objects;

            var comments = await GetSkippedObjectCommentsAsync(connection, provider).ConfigureAwait(false);
            if (comments.Count == 0)
                return objects;

            var result = new ScaffoldSkippedObjectInfo[objects.Count];
            for (var i = 0; i < objects.Count; i++)
            {
                var obj = objects[i];
                result[i] = comments.TryGetValue(SkippedObjectCommentKey(obj.Schema, obj.Name, obj.Kind), out var comment)
                    ? obj with { Comment = comment }
                    : obj;
            }

            return result;
        }

        private static async Task<IReadOnlyDictionary<string, string>> GetSkippedObjectCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT SCHEMA_NAME(o.schema_id) AS ObjectSchema,
                           o.name AS ObjectName,
                           'Routine' AS Kind,
                           CAST(comment.value AS nvarchar(max)) AS ObjectComment
                    FROM sys.objects o
                    LEFT JOIN sys.extended_properties comment
                      ON comment.major_id = o.object_id
                     AND comment.minor_id = 0
                     AND comment.name = N'MS_Description'
                    WHERE o.is_ms_shipped = 0
                      AND o.type IN ('P', 'FN', 'IF', 'TF')
                    UNION ALL
                    SELECT SCHEMA_NAME(seq.schema_id),
                           seq.name,
                           'Sequence',
                           CAST(comment.value AS nvarchar(max))
                    FROM sys.sequences seq
                    LEFT JOIN sys.extended_properties comment
                      ON comment.major_id = seq.object_id
                     AND comment.minor_id = 0
                     AND comment.name = N'MS_Description'
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT n.nspname AS ObjectSchema,
                           p.proname AS ObjectName,
                           'Routine' AS Kind,
                           obj_description(p.oid, 'pg_proc') AS ObjectComment
                    FROM pg_proc p
                    INNER JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pg_proc sibling
                          WHERE sibling.pronamespace = p.pronamespace
                            AND sibling.proname = p.proname
                            AND sibling.oid <> p.oid
                      )
                    UNION ALL
                    SELECT n.nspname,
                           cls.relname,
                           'Sequence',
                           obj_description(cls.oid, 'pg_class')
                    FROM pg_class cls
                    INNER JOIN pg_namespace n ON n.oid = cls.relnamespace
                    WHERE cls.relkind = 'S'
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectCommentsAsync(connection, """
                    SELECT NULL AS ObjectSchema,
                           routine_name AS ObjectName,
                           'Routine' AS Kind,
                           routine_comment AS ObjectComment
                    FROM information_schema.routines
                    WHERE routine_schema = DATABASE()
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, string>> QuerySkippedObjectCommentsAsync(DbConnection connection, string sql)
        {
            var comments = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var objectName = Convert.ToString(reader["ObjectName"]);
                var kind = Convert.ToString(reader["Kind"]);
                var comment = NullIfWhiteSpace(Convert.ToString(reader["ObjectComment"]));
                if (string.IsNullOrWhiteSpace(objectName)
                    || string.IsNullOrWhiteSpace(kind)
                    || comment is null)
                {
                    continue;
                }

                var key = SkippedObjectCommentKey(
                    NullIfWhiteSpace(Convert.ToString(reader["ObjectSchema"])),
                    objectName!,
                    kind!);
                if (!comments.ContainsKey(key))
                    comments[key] = comment;
            }

            return comments;
        }

        private static string SkippedObjectCommentKey(string? schema, string name, string kind)
            => TableKey(schema, name) + "|" + kind.Trim();

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
