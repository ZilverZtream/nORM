#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelMetadataDiscovery
    {
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
    }
}
