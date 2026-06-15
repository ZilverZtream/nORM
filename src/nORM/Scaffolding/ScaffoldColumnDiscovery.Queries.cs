#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> QueryColumnFacetMapAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, Dictionary<string, ScaffoldColumnFacet>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(columnName))
                    continue;

                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName!);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var maxLength = reader["MaxLength"] == DBNull.Value
                    ? (int?)null
                    : Convert.ToInt32(reader["MaxLength"], CultureInfo.InvariantCulture);
                var isUnicode = reader["IsUnicode"] == DBNull.Value
                    ? (bool?)null
                    : Convert.ToInt32(reader["IsUnicode"], CultureInfo.InvariantCulture) != 0;
                var isFixedLength = reader["IsFixedLength"] != DBNull.Value
                    && Convert.ToInt32(reader["IsFixedLength"], CultureInfo.InvariantCulture) != 0;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns[columnName!] = new ScaffoldColumnFacet(
                    maxLength is > 0 ? maxLength : null,
                    isUnicode,
                    isFixedLength);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, ScaffoldColumnFacet>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> QueryColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns.Add(columnName);
            }

            return ToReadOnlySetDictionary(result);
        }
    }
}
