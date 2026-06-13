#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldColumnDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> QueryColumnStoreTypeMapAsync(
            DbConnection connection,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                var storeType = Convert.ToString(reader["StoreType"]);
                if (string.IsNullOrWhiteSpace(tableName)
                    || string.IsNullOrWhiteSpace(columnName)
                    || string.IsNullOrWhiteSpace(storeType))
                {
                    continue;
                }

                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName!);
                if (!tableKeys.Contains(tableKey))
                    continue;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns[columnName!] = storeType!;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }
    }
}
