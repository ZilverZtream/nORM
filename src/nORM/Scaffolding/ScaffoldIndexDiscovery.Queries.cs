#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldIndexDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldIndexInfo>> QueryIndexesAsync(
            DbConnection connection,
            string sql,
            IReadOnlyList<ScaffoldTableInfo> tables)
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

            return ScaffoldIndexNameNormalizer.NormalizeSyntheticIndexNames(indexes, tables);
        }

        private static IndexNullSortOrder ParseIndexNullSortOrder(string? value)
            => value?.Trim() switch
            {
                "First" => IndexNullSortOrder.First,
                "Last" => IndexNullSortOrder.Last,
                _ => IndexNullSortOrder.Default
            };
    }
}
