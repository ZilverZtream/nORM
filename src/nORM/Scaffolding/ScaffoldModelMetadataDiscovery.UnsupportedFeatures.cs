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
        private static async Task<List<ScaffoldUnsupportedFeature>> BuildUnsupportedFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            bool noRelationships)
        {
            var unsupportedFeatures = (await ScaffoldSchemaDiscoveryAdapter.GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
            unsupportedFeatures.AddRange(await ScaffoldSchemaDiscoveryAdapter.GetPostgresEnumColumnFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedDescendingIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedIncludedColumnIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedPartialIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable, columnPropertiesByTable);
            if (noRelationships)
                return unsupportedFeatures;

            ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
            AddRelationshipPrincipalKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable, indexes);
            AddRelationshipDependentKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable);
            return unsupportedFeatures;
        }

        private static void AddRelationshipPrincipalKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildPrincipalKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable,
                    ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes))));

        private static void AddRelationshipDependentKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildDependentKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable)));
    }
}
