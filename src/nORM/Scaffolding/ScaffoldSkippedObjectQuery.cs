#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectQuery
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
            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerSkippedObjectCommentsAsync(connection).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresSkippedObjectCommentsAsync(connection).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlSkippedObjectCommentsAsync(connection).ConfigureAwait(false);

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
