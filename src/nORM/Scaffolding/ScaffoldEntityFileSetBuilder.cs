#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldEntityFileSetBuilder
    {
        public static async Task<DatabaseScaffolder.ScaffoldEntityFileSet> BuildAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string outputDirectory,
            string namespaceName,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> sqliteDeclaredTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnStoreTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable,
            IReadOnlyDictionary<string, ScaffoldComments> commentsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> relationships,
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> manyToManyJoins,
            IReadOnlySet<string> manyToManyJoinTableKeys,
            IReadOnlySet<string> queryArtifactTableKeys,
            DatabaseScaffolder.ScaffoldFeatureConfigurations featureConfigurations,
            ScaffoldOptions options)
        {
            var context = new EntityFileSetBuildContext(
                connection,
                provider,
                outputDirectory,
                namespaceName,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                nonNullableColumnsByTable,
                sqliteDeclaredTypesByTable,
                columnStoreTypesByTable,
                stringBinaryFacetsByTable,
                commentsByTable,
                identityColumnsByTable,
                indexes,
                relationships,
                manyToManyJoins,
                manyToManyJoinTableKeys,
                queryArtifactTableKeys,
                featureConfigurations,
                options);
            var entityNames = new List<string>();
            var generatedFiles = new List<(string Path, string Content)>();

            foreach (var table in tables)
            {
                var file = await BuildEntityFileAsync(context, table).ConfigureAwait(false);
                if (file is null)
                    continue;

                entityNames.Add(file.Value.EntityName);
                generatedFiles.Add((file.Value.Path, file.Value.Content));
            }

            return new DatabaseScaffolder.ScaffoldEntityFileSet(generatedFiles, entityNames);
        }

        private static async Task<EntityFileResult?> BuildEntityFileAsync(
            EntityFileSetBuildContext context,
            DatabaseScaffolder.ScaffoldTable table)
        {
            var tableName = table.Name;
            var schemaName = table.Schema;
            var tableKey = TableKey(schemaName, tableName);
            if (context.ManyToManyJoinTableKeys.Contains(tableKey))
                return null;

            var entityName = context.EntityByTable[tableKey];
            var references = context.Relationships
                .Where(r => string.Equals(r.DependentTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var collections = context.Relationships
                .Where(r => string.Equals(r.PrincipalTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var manyToManyCollections = ScaffoldRelationshipAdapter.BuildManyToManyNavigations(context.ManyToManyJoins, tableKey);
            var tableIndexes = context.Indexes
                .Where(i => string.Equals(i.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            context.ColumnPropertiesByTable.TryGetValue(tableKey, out var columnPropertyNames);
            context.FeatureConfigurations.ComputedColumnsByTable.TryGetValue(tableKey, out var computedColumns);
            context.FeatureConfigurations.RowVersionColumnsByTable.TryGetValue(tableKey, out var rowVersionColumns);
            context.IdentityColumnsByTable.TryGetValue(tableKey, out var identityColumns);
            context.NonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns);
            context.FeatureConfigurations.DecimalPrecisionByTable.TryGetValue(tableKey, out var decimalPrecisions);
            context.StringBinaryFacetsByTable.TryGetValue(tableKey, out var columnFacets);
            context.ColumnStoreTypesByTable.TryGetValue(tableKey, out var columnStoreTypes);
            context.CommentsByTable.TryGetValue(tableKey, out var comments);
            context.SqliteDeclaredTypesByTable.TryGetValue(tableKey, out var sqliteDeclaredTypes);
            context.FeatureConfigurations.ProviderSpecificColumnTypesByTable.TryGetValue(tableKey, out var providerSpecificColumnTypes);

            var isReadOnlyEntity = ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                tableKey,
                context.QueryArtifactTableKeys,
                context.FeatureConfigurations.ProviderNativeTemporalTableKeys,
                context.FeatureConfigurations.ProviderOwnedTriggerTableKeys,
                context.FeatureConfigurations.ProviderSpecificIdentityStrategyTableKeys,
                context.FeatureConfigurations.ProviderSpecificDefaultTableKeys,
                providerSpecificColumnTypes,
                context.PrimaryKeyColumnsByTable);
            var entityCode = await ScaffoldEntityFileAdapter.ScaffoldEntityAsync(
                context.Connection,
                context.Provider,
                schemaName,
                tableName,
                entityName,
                context.NamespaceName,
                columnPropertyNames,
                tableIndexes,
                references,
                collections,
                manyToManyCollections,
                computedColumns,
                rowVersionColumns,
                identityColumns,
                decimalPrecisions,
                columnFacets,
                comments,
                isReadOnlyEntity,
                context.Options.UseNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                columnStoreTypes,
                providerSpecificColumnTypes).ConfigureAwait(false);

            return new EntityFileResult(
                entityName,
                Path.Combine(context.OutputDirectory, entityName + ".cs"),
                entityCode);
        }

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);

        private sealed record EntityFileSetBuildContext(
            DbConnection Connection,
            DatabaseProvider Provider,
            string OutputDirectory,
            string NamespaceName,
            IReadOnlyDictionary<string, string> EntityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> SqliteDeclaredTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnStoreTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> StringBinaryFacetsByTable,
            IReadOnlyDictionary<string, ScaffoldComments> CommentsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> Indexes,
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> Relationships,
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> ManyToManyJoins,
            IReadOnlySet<string> ManyToManyJoinTableKeys,
            IReadOnlySet<string> QueryArtifactTableKeys,
            DatabaseScaffolder.ScaffoldFeatureConfigurations FeatureConfigurations,
            ScaffoldOptions Options);

        private readonly record struct EntityFileResult(string EntityName, string Path, string Content);
    }
}
