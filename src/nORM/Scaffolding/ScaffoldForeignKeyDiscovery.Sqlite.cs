#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldForeignKeyDiscovery
    {
        private static async Task<IReadOnlyList<ScaffoldForeignKeyInfo>> GetSqliteForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var foreignKeys = new List<ScaffoldForeignKeyInfo>();
            foreach (var table in tables)
            {
                var createTableSql = await GetSqliteCreateTableSqlAsync(connection, provider, table).ConfigureAwait(false);
                var providerSemanticsByColumns = ScaffoldSqliteDdlParser.ExtractForeignKeyProviderSemanticsByColumns(
                    createTableSql);
                var constraintNamesByColumns = ScaffoldSqliteDdlParser.ExtractForeignKeyConstraintNamesByColumns(
                    createTableSql);
                var rows = await ReadSqliteForeignKeyRowsAsync(connection, provider, table).ConfigureAwait(false);

                foreach (var group in rows.GroupBy(static row => row.Id))
                    AddSqliteForeignKeyRows(table, group.OrderBy(static row => row.Seq).ToArray(), providerSemanticsByColumns, constraintNamesByColumns, foreignKeys);
            }

            return foreignKeys;
        }
    }
}
