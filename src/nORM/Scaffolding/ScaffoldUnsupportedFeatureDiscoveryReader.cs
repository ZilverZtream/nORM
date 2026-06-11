#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static class ScaffoldUnsupportedFeatureDiscoveryReader
    {
        public static async Task AddFeaturesAsync(
            DbConnection connection,
            ICollection<ScaffoldUnsupportedFeatureInfo> features,
            IReadOnlySet<string> tableKeys,
            string sql)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                features.Add(new ScaffoldUnsupportedFeatureInfo(
                    tableKey,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["ObjectName"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty));
            }
        }

        public static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        public static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        public static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
