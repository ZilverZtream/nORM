#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSqliteIndexDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var indexes = new List<ScaffoldIndexInfo>();
            foreach (var table in tables)
            {
                await using var listCommand = connection.CreateCommand();
                listCommand.CommandText = SqlitePragma(provider, table.Schema, "index_list", table.Name);
                await using var listReader = await listCommand.ExecuteReaderAsync().ConfigureAwait(false);
                var tableIndexes = new List<(string Name, bool IsUnique, string Origin, bool IsPartial)>();
                while (await listReader.ReadAsync().ConfigureAwait(false))
                {
                    var name = Convert.ToString(listReader["name"]);
                    if (string.IsNullOrWhiteSpace(name))
                        continue;

                    tableIndexes.Add((
                        name,
                        Convert.ToInt32(listReader["unique"], CultureInfo.InvariantCulture) != 0,
                        Convert.ToString(listReader["origin"]) ?? string.Empty,
                        ReaderHasColumn(listReader, "partial")
                            && Convert.ToInt32(listReader["partial"], CultureInfo.InvariantCulture) != 0));
                }

                foreach (var (name, isUnique, origin, isPartial) in tableIndexes)
                {
                    if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase))
                        continue;

                    await AddSqliteIndexAsync(connection, provider, table, name, isUnique, origin, isPartial, indexes).ConfigureAwait(false);
                }
            }

            return ScaffoldIndexNameNormalizer.NormalizeSyntheticIndexNames(indexes, tables);
        }

        private static async Task AddSqliteIndexAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            string name,
            bool isUnique,
            string origin,
            bool isPartial,
            ICollection<ScaffoldIndexInfo> indexes)
        {
            var filterSql = isPartial
                ? await GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name).ConfigureAwait(false)
                : null;

            await using var infoCommand = connection.CreateCommand();
            infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", name);
            await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);
            var columns = new List<(int Ordinal, string Name, bool IsDescending)>();
            var hasUnsupportedKeyPart = false;
            while (await infoReader.ReadAsync().ConfigureAwait(false))
            {
                if (ReaderHasColumn(infoReader, "key")
                    && Convert.ToInt32(infoReader["key"], CultureInfo.InvariantCulture) == 0)
                {
                    continue;
                }

                if (ReaderHasColumn(infoReader, "cid")
                    && Convert.ToInt32(infoReader["cid"], CultureInfo.InvariantCulture) < 0)
                {
                    hasUnsupportedKeyPart = true;
                    continue;
                }

                var columnName = Convert.ToString(infoReader["name"]);
                if (!string.IsNullOrWhiteSpace(columnName))
                {
                    columns.Add((
                        Convert.ToInt32(infoReader["seqno"], CultureInfo.InvariantCulture),
                        columnName,
                        ReaderHasColumn(infoReader, "desc")
                            && Convert.ToInt32(infoReader["desc"], CultureInfo.InvariantCulture) != 0));
                }
            }

            if (hasUnsupportedKeyPart)
                return;

            var isSyntheticName = string.Equals(origin, "u", StringComparison.OrdinalIgnoreCase)
                                  || name.StartsWith("sqlite_autoindex_", StringComparison.OrdinalIgnoreCase);
            foreach (var column in columns.OrderBy(static c => c.Ordinal))
            {
                indexes.Add(new ScaffoldIndexInfo(
                    TableKey(table.Schema, table.Name),
                    column.Name,
                    name,
                    isUnique,
                    columns.Count,
                    column.Ordinal,
                    column.IsDescending,
                    false,
                    IndexNullSortOrder.Default,
                    false,
                    filterSql,
                    isSyntheticName));
            }
        }

        private static async Task<string?> GetSqliteIndexFilterSqlAsync(DbConnection connection, DatabaseProvider provider, string? schemaName, string indexName)
        {
            var sql = await GetSqliteIndexSqlAsync(connection, provider, schemaName, indexName).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            var where = sql.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            return where < 0 ? null : sql[(where + 7)..].Trim();
        }

        private static async Task<string?> GetSqliteIndexSqlAsync(DbConnection connection, DatabaseProvider provider, string? schemaName, string indexName)
        {
            await using var cmd = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            cmd.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'index' AND name = @name";
            var p = cmd.CreateParameter();
            p.ParameterName = "@name";
            p.Value = indexName;
            cmd.Parameters.Add(p);
            return Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;
    }
}
