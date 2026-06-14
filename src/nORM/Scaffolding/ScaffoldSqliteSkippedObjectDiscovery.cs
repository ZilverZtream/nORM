#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;
using static nORM.Scaffolding.ScaffoldSkippedObjectQuery;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteSkippedObjectDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldSkippedObjectInfo>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var objects = new List<ScaffoldSkippedObjectInfo>();
            foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
            {
                objects.AddRange(await QuerySkippedObjectsAsync(
                    connection,
                    GetSqliteSkippedObjectSql(provider, schema)).ConfigureAwait(false));
            }

            return objects;
        }

        public static async Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
        {
            var schemas = new List<string>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA database_list";
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = Convert.ToString(reader["name"]);
                if (string.IsNullOrWhiteSpace(schema)
                    || string.Equals(schema, "temp", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                schemas.Add(schema);
            }

            return schemas.Count == 0 ? new[] { "main" } : schemas;
        }

        public static string SqliteSchemaResult(string schema)
            => string.Equals(schema, "main", StringComparison.OrdinalIgnoreCase)
                ? "NULL"
                : "'" + schema.Replace("'", "''") + "'";
    }
}
