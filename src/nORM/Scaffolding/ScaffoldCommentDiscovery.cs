#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCommentDiscovery
    {
        public static async Task<IReadOnlyDictionary<string, ScaffoldComments>> GetScaffoldCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var tableKeys = tables.Select(table => TableKey(table.Schema, table.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (tableKeys.Count == 0 || ScaffoldProviderKind.IsSqlite(provider))
                return EmptyComments();

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerScaffoldCommentsAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresScaffoldCommentsAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlScaffoldCommentsAsync(connection, tableKeys).ConfigureAwait(false);

            return EmptyComments();
        }

        private static IReadOnlyDictionary<string, ScaffoldComments> EmptyComments()
            => new Dictionary<string, ScaffoldComments>(StringComparer.OrdinalIgnoreCase);
    }
}
