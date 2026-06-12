#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldModelMetadataDiscovery
    {
        public static async Task<ScaffoldModelMetadataDiscoveryResult> DiscoverAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyList<ScaffoldTableInfo> tableInfos,
            IReadOnlyDictionary<string, string> entityByTable,
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
            var primaryKeyConstraintNamesByTable = await ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var nonNullableColumnsByTable = await ScaffoldColumnDiscovery.GetNonNullableColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var sqliteDeclaredTypesByTable = ScaffoldProviderKind.IsSqlite(provider)
                ? await ScaffoldModelDiscovery.GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false)
                : new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
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
                primaryKeyColumnsByTable,
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
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                nonNullableColumnsByTable,
                sqliteDeclaredTypesByTable,
                stringBinaryFacetsByTable,
                commentsByTable,
                identityColumnsByTable,
                scaffoldedTableKeys,
                indexes,
                foreignKeys,
                unsupportedFeatures,
                featureConfigurations);
        }

        private static async Task<List<DatabaseScaffolder.ScaffoldUnsupportedFeature>> BuildUnsupportedFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys)
        {
            var unsupportedFeatures = (await ScaffoldSchemaDiscoveryAdapter.GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
            unsupportedFeatures.AddRange(await ScaffoldSchemaDiscoveryAdapter.GetPostgresEnumColumnFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedDescendingIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedIncludedColumnIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedPartialIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable, columnPropertiesByTable);
            ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
            AddRelationshipPrincipalKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable, indexes);
            AddRelationshipDependentKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable);
            return unsupportedFeatures;
        }

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> FilterIndexesToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> scaffoldedTableKeys)
            => indexes
                .Where(index => scaffoldedTableKeys.Contains(index.TableKey))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

        private static void AddRelationshipPrincipalKeyDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildPrincipalKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable,
                    ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes))));

        private static void AddRelationshipDependentKeyDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildDependentKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable)));

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }

    internal sealed record ScaffoldModelMetadataDiscoveryResult(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        Dictionary<string, HashSet<string>> MemberNamesByTable,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyDictionary<string, string> PrimaryKeyConstraintNamesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> SqliteDeclaredTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> StringBinaryFacetsByTable,
        IReadOnlyDictionary<string, ScaffoldComments> CommentsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ScaffoldedTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> Indexes,
        IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> ForeignKeys,
        List<DatabaseScaffolder.ScaffoldUnsupportedFeature> UnsupportedFeatures,
        DatabaseScaffolder.ScaffoldFeatureConfigurations FeatureConfigurations);
}
