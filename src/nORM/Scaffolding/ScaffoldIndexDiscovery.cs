#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldIndexDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
                return await GetSqliteIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await QueryIndexesAsync(connection, SqlServerIndexSql).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await QueryIndexesAsync(connection, PostgresIndexSql).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await GetMySqlIndexesAsync(connection, provider, tables).ConfigureAwait(false);

            return Array.Empty<ScaffoldIndexInfo>();
        }

        private static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetSqliteIndexesAsync(
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

            return NormalizeSyntheticIndexNames(indexes);
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

        private static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetMySqlIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var indexes = await QueryIndexesAsync(connection, MySqlIndexSql).ConfigureAwait(false);
            var expressionIndexKeys = (await ScaffoldMySqlUnsupportedFeatureDiscovery.GetExpressionIndexFeaturesAsync(connection, provider, tables).ConfigureAwait(false))
                .Select(static feature => feature.TableKey + "\u001f" + feature.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (expressionIndexKeys.Count == 0)
                return indexes;

            return indexes
                .Where(index => !expressionIndexKeys.Contains(index.TableKey + "\u001f" + index.IndexName))
                .ToArray();
        }

        private static async Task<IReadOnlyList<ScaffoldIndexInfo>> QueryIndexesAsync(DbConnection connection, string sql)
        {
            var indexes = new List<ScaffoldIndexInfo>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                var indexName = Convert.ToString(reader["IndexName"]);
                if (string.IsNullOrWhiteSpace(tableName)
                    || string.IsNullOrWhiteSpace(columnName)
                    || string.IsNullOrWhiteSpace(indexName))
                {
                    continue;
                }

                var columnCount = Convert.ToInt32(reader["ColumnCount"], CultureInfo.InvariantCulture);
                indexes.Add(new ScaffoldIndexInfo(
                    TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName),
                    columnName,
                    indexName,
                    Convert.ToBoolean(reader["IsUnique"], CultureInfo.InvariantCulture),
                    columnCount,
                    Convert.ToInt32(reader["Ordinal"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "IsDescending")
                        && Convert.ToBoolean(reader["IsDescending"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "IsIncluded")
                        && Convert.ToBoolean(reader["IsIncluded"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "NullSortOrder") ? ParseIndexNullSortOrder(Convert.ToString(reader["NullSortOrder"])) : IndexNullSortOrder.Default,
                    ReaderHasColumn(reader, "NullsNotDistinct")
                        && Convert.ToBoolean(reader["NullsNotDistinct"], CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "FilterSql") ? NullIfWhiteSpace(Convert.ToString(reader["FilterSql"])) : null,
                    ReaderHasColumn(reader, "IsSyntheticName")
                        && Convert.ToBoolean(reader["IsSyntheticName"], CultureInfo.InvariantCulture)));
            }

            return NormalizeSyntheticIndexNames(indexes);
        }

        private static IReadOnlyList<ScaffoldIndexInfo> NormalizeSyntheticIndexNames(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            if (!indexes.Any(static index => index.IsSyntheticName))
                return indexes;

            var generatedNames = indexes
                .Where(static index => index.IsSyntheticName)
                .GroupBy(index => index.TableKey + "\u001f" + index.IndexName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    group => group.Key,
                    group =>
                    {
                        var first = group.First();
                        var keyColumns = group
                            .Where(static index => !index.IsIncluded)
                            .OrderBy(static index => index.Ordinal)
                            .Select(static index => index.ColumnName)
                            .ToArray();
                        return BuildGeneratedIndexName(first.TableKey, keyColumns, first.IsUnique);
                    },
                    StringComparer.OrdinalIgnoreCase);

            return indexes
                .Select(index => generatedNames.TryGetValue(index.TableKey + "\u001f" + index.IndexName, out var generatedName)
                    ? index with { IndexName = generatedName, IsSyntheticName = false }
                    : index)
                .ToArray();
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

        private static IndexNullSortOrder ParseIndexNullSortOrder(string? value)
            => value?.Trim() switch
            {
                "First" => IndexNullSortOrder.First,
                "Last" => IndexNullSortOrder.Last,
                _ => IndexNullSortOrder.Default
            };

        private static string BuildGeneratedIndexName(string tableKey, IReadOnlyList<string> columnNames, bool isUnique)
        {
            var prefix = isUnique ? "UX_" : "IX_";
            var tableSegment = LastTableKeySegment(tableKey);
            var segments = new[] { tableSegment }
                .Concat(columnNames)
                .Select(SanitizeConstraintNameSegment)
                .Where(static segment => segment.Length > 0);
            var baseName = prefix + string.Join("_", segments);
            if (baseName.Length <= 128)
                return baseName;

            var hash = StableIdentifierHash(tableKey + "\u001f" + string.Join("\u001f", columnNames));
            var maxBaseLength = 127 - hash.Length;
            return baseName[..Math.Max(prefix.Length, maxBaseLength)] + "_" + hash;
        }

        private static string LastTableKeySegment(string tableKey)
        {
            var dot = tableKey.LastIndexOf('.');
            return dot >= 0 && dot < tableKey.Length - 1 ? tableKey[(dot + 1)..] : tableKey;
        }

        private static string SanitizeConstraintNameSegment(string value)
        {
            var sb = new StringBuilder(value.Length);
            foreach (var ch in value)
                sb.Append(char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_');

            return sb.Length == 0 ? "Table" : sb.ToString();
        }

        private static string StableIdentifierHash(string value)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            var hash = offset;
            foreach (var b in Encoding.UTF8.GetBytes(value))
            {
                hash ^= b;
                hash *= prime;
            }

            return hash.ToString("X16", CultureInfo.InvariantCulture);
        }

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;

        private const string SqlServerIndexSql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS TableSchema,
                t.name AS TableName,
                c.name AS ColumnName,
                i.name AS IndexName,
                i.is_unique AS IsUnique,
                SUM(CASE WHEN ic.is_included_column = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY i.object_id, i.index_id) AS ColumnCount,
                CASE WHEN ic.is_included_column = 1 THEN 2147483647 ELSE ic.key_ordinal - 1 END AS Ordinal,
                ic.is_descending_key AS IsDescending,
                ic.is_included_column AS IsIncluded,
                CASE WHEN kc.is_system_named = 1 THEN 1 ELSE 0 END AS IsSyntheticName,
                i.filter_definition AS FilterSql
            FROM sys.indexes i
            INNER JOIN sys.tables t ON t.object_id = i.object_id
            INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
            LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id AND kc.type = 'UQ'
            WHERE t.is_ms_shipped = 0
              AND i.is_primary_key = 0
              AND i.is_hypothetical = 0
              AND i.type IN (1, 2)
              AND i.name IS NOT NULL
            ORDER BY SCHEMA_NAME(t.schema_id), t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
            """;

        private const string PostgresIndexSql = """
            SELECT
                ns.nspname AS TableSchema,
                tbl.relname AS TableName,
                att.attname AS ColumnName,
                idx.relname AS IndexName,
                ix.indisunique AS IsUnique,
                ix.indnkeyatts AS ColumnCount,
                CASE WHEN key.ord > ix.indnkeyatts THEN 2147483647 ELSE key.ord - 1 END AS Ordinal,
                CASE WHEN key.ord <= ix.indnkeyatts AND (ix.indoption[key.ord - 1] & 1) = 1 THEN 1 ELSE 0 END AS IsDescending,
                CASE WHEN key.ord > ix.indnkeyatts THEN 1 ELSE 0 END AS IsIncluded,
                CASE
                    WHEN key.ord <= ix.indnkeyatts
                     AND (ix.indoption[key.ord - 1] & 1) = 0
                     AND (ix.indoption[key.ord - 1] & 2) = 2 THEN 'First'
                    WHEN key.ord <= ix.indnkeyatts
                     AND (ix.indoption[key.ord - 1] & 1) = 1
                     AND (ix.indoption[key.ord - 1] & 2) = 0 THEN 'Last'
                    ELSE NULL
                END AS NullSortOrder,
                COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) AS NullsNotDistinct,
                CASE WHEN ix.indpred IS NULL THEN NULL ELSE pg_get_expr(ix.indpred, ix.indrelid) END AS FilterSql
            FROM pg_index ix
            INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
            INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
            INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            INNER JOIN pg_am am ON am.oid = idx.relam
            INNER JOIN unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ord) ON true
            INNER JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key.attnum
            WHERE ix.indisprimary = false
              AND ix.indexprs IS NULL
              AND am.amname = 'btree'
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
              AND NOT EXISTS (
                  SELECT 1
                  FROM unnest(ix.indkey) WITH ORDINALITY AS option_key(attnum, ord)
                  INNER JOIN pg_attribute option_att
                      ON option_att.attrelid = tbl.oid
                     AND option_att.attnum = option_key.attnum
                  INNER JOIN pg_opclass option_opclass
                      ON option_opclass.oid = ix.indclass[option_key.ord - 1]
                  WHERE option_key.ord <= ix.indnkeyatts
                    AND (
                        option_opclass.opcdefault = false
                        OR (
                            ix.indcollation[option_key.ord - 1] <> 0
                            AND ix.indcollation[option_key.ord - 1] <> option_att.attcollation
                        )
                    )
              )
            ORDER BY ns.nspname, tbl.relname, idx.relname, key.ord
            """;

        private const string MySqlIndexSql = """
            SELECT
                NULL AS TableSchema,
                s.table_name AS TableName,
                s.column_name AS ColumnName,
                s.index_name AS IndexName,
                CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS IsUnique,
                COUNT(*) OVER (PARTITION BY s.table_schema, s.table_name, s.index_name) AS ColumnCount,
                s.seq_in_index - 1 AS Ordinal,
                CASE WHEN UPPER(COALESCE(s.collation, 'A')) = 'D' THEN 1 ELSE 0 END AS IsDescending
            FROM information_schema.statistics s
            INNER JOIN information_schema.columns c
                ON c.table_schema = s.table_schema
               AND c.table_name = s.table_name
               AND c.column_name = s.column_name
            WHERE s.table_schema = DATABASE()
              AND s.index_name <> 'PRIMARY'
              AND UPPER(COALESCE(NULLIF(s.index_type, ''), 'BTREE')) = 'BTREE'
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.statistics bad
                  INNER JOIN information_schema.columns bad_col
                      ON bad_col.table_schema = bad.table_schema
                     AND bad_col.table_name = bad.table_name
                     AND bad_col.column_name = bad.column_name
                  WHERE bad.table_schema = s.table_schema
                    AND bad.table_name = s.table_name
                    AND bad.index_name = s.index_name
                    AND bad.sub_part IS NOT NULL
                    AND (
                        bad_col.character_maximum_length IS NULL
                        OR bad.sub_part < bad_col.character_maximum_length
                    )
              )
            ORDER BY s.table_schema, s.table_name, s.index_name, s.seq_in_index
            """;
    }
}
