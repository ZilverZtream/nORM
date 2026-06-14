#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelMetadataDiscovery
    {
        public static async Task<ScaffoldModelMetadataDiscoveryResult> DiscoverAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyList<ScaffoldTableInfo> tableInfos,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlySet<string> queryArtifactTableKeys,
            bool useDatabaseNames)
        {
            var columnPropertiesByTable = await ScaffoldColumnPropertyDiscovery.GetColumnPropertyNamesAsync(
                connection,
                provider,
                tableInfos,
                entityByTable,
                useDatabaseNames).ConfigureAwait(false);
            var memberNamesByTable = ScaffoldColumnPropertyNameBuilder.BuildMemberNameMap(columnPropertiesByTable, entityByTable);
            var primaryKeyColumnsByTable = await ScaffoldKeyDiscovery.GetPrimaryKeyColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var scaffoldModelPrimaryKeyColumnsByTable = FilterQueryArtifactPrimaryKeys(primaryKeyColumnsByTable, queryArtifactTableKeys);
            var primaryKeyConstraintNamesByTable = await ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var nonNullableColumnsByTable = await ScaffoldColumnDiscovery.GetNonNullableColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var sqliteDeclaredTypesByTable = ScaffoldProviderKind.IsSqlite(provider)
                ? await ScaffoldModelDiscovery.GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false)
                : new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            var columnStoreTypesByTable = await ScaffoldColumnDiscovery.GetColumnStoreTypesAsync(
                connection,
                provider,
                tableInfos).ConfigureAwait(false);
            var stringBinaryFacetsByTable = await ScaffoldColumnDiscovery.GetStringBinaryFacetsAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var commentsByTable = await ScaffoldCommentDiscovery.GetScaffoldCommentsAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var identityColumnsByTable = await ScaffoldColumnDiscovery.GetIdentityColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var scaffoldedTableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var indexes = FilterIndexesToScaffoldedTables(
                await ScaffoldSchemaDiscoveryAdapter.GetIndexesAsync(connection, provider, tables).ConfigureAwait(false),
                scaffoldedTableKeys);
            var foreignKeys = FilterForeignKeysToScaffoldedTables(
                await ScaffoldSchemaDiscoveryAdapter.GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false),
                scaffoldedTableKeys);
            var unsupportedFeatures = await BuildUnsupportedFeaturesAsync(
                connection,
                provider,
                tables,
                scaffoldModelPrimaryKeyColumnsByTable,
                columnPropertiesByTable,
                indexes,
                foreignKeys).ConfigureAwait(false);
            var featureConfigurations = ScaffoldFeatureConfigurationAdapter.BuildFeatureConfigurations(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);

            return new ScaffoldModelMetadataDiscoveryResult(
                columnPropertiesByTable,
                memberNamesByTable,
                scaffoldModelPrimaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                nonNullableColumnsByTable,
                sqliteDeclaredTypesByTable,
                columnStoreTypesByTable,
                stringBinaryFacetsByTable,
                commentsByTable,
                identityColumnsByTable,
                scaffoldedTableKeys,
                indexes,
                foreignKeys,
                unsupportedFeatures,
                featureConfigurations);
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> FilterQueryArtifactPrimaryKeys(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlySet<string> queryArtifactTableKeys)
        {
            if (queryArtifactTableKeys.Count == 0)
                return primaryKeyColumnsByTable;

            return primaryKeyColumnsByTable
                .Where(pair => !queryArtifactTableKeys.Contains(pair.Key))
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => pair.Value,
                    StringComparer.OrdinalIgnoreCase);
        }
    }
}
