#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldKeyDiscovery
    {
        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> QueryOrderedColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = tableKeys.ToDictionary(
                key => key,
                _ => new List<(int Ordinal, string Column)>(),
                StringComparer.OrdinalIgnoreCase);

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

                var ordinal = ReaderHasColumn(reader, "Ordinal")
                    ? Convert.ToInt32(reader["Ordinal"], CultureInfo.InvariantCulture)
                    : result[tableKey].Count + 1;
                result[tableKey].Add((ordinal, columnName));
            }

            return ToOrderedColumnDictionary(result);
        }

        private static async Task<IReadOnlyDictionary<string, string>> QueryPrimaryKeyConstraintNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(
                    NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])),
                    Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var constraintName = NullIfWhiteSpace(Convert.ToString(reader["ConstraintName"]));
                if (constraintName is not null)
                    result[tableKey] = constraintName;
            }

            return result;
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> ToOrderedColumnDictionary(
            Dictionary<string, List<(int Ordinal, string Column)>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyList<string>)pair.Value
                    .OrderBy(item => item.Ordinal)
                    .Select(item => item.Column)
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);
    }
}
