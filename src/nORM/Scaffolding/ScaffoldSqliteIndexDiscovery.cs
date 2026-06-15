#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteIndexDiscovery
    {
        public static async Task<IReadOnlyList<ScaffoldIndexInfo>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var indexes = new List<ScaffoldIndexInfo>();
            foreach (var table in tables)
            {
                var createTableSql = await GetSqliteCreateTableSqlAsync(connection, provider, table.Schema, table.Name).ConfigureAwait(false);
                var uniqueConstraintNamesByColumns = ScaffoldSqliteDdlParser.ExtractUniqueConstraintNamesByColumns(createTableSql);
                var tableIndexes = await GetSqliteTableIndexesAsync(connection, provider, table).ConfigureAwait(false);
                foreach (var (name, isUnique, origin, isPartial) in tableIndexes)
                {
                    if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase))
                        continue;

                    await AddSqliteIndexAsync(
                        connection,
                        provider,
                        table,
                        name,
                        isUnique,
                        origin,
                        isPartial,
                        uniqueConstraintNamesByColumns,
                        indexes).ConfigureAwait(false);
                }
            }

            return ScaffoldIndexNameNormalizer.NormalizeSyntheticIndexNames(indexes, tables);
        }
    }
}
