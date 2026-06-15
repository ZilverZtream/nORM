#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelDiscovery
    {
        private static async Task<(IReadOnlyList<DatabaseScaffolder.ScaffoldTable> Tables, IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys)> BuildScaffoldObjectSelectionAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldOptions options)
        {
            var filterCatalog = GetScaffoldFilterCatalog(connection, provider);
            var discoveredTables = await ScaffoldSchemaDiscoveryAdapter.GetTablesAsync(connection, provider).ConfigureAwait(false);
            var discoveredSkippedObjects = await ScaffoldSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            var selection = ScaffoldObjectSelectionBuilder.BuildSelection(
                discoveredTables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                discoveredSkippedObjects,
                options,
                provider,
                filterCatalog);
            var skippedObjects = await ScaffoldSkippedObjectDiscovery.AttachCommentsAsync(connection, provider, selection.SkippedObjects).ConfigureAwait(false);
            return (
                selection.Tables.Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldTable).ToArray(),
                skippedObjects.Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldSkippedObject).ToArray(),
                selection.QueryArtifactTableKeys);
        }

        private static ScaffoldTableInfo[] BuildTableInfos(IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        private static string? GetScaffoldFilterCatalog(DbConnection connection, DatabaseProvider provider)
            => ScaffoldProviderKind.IsMySql(provider) ? NullIfWhiteSpace(connection.Database) : null;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
