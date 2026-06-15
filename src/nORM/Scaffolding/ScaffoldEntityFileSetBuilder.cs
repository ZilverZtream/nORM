#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static class ScaffoldEntityFileSetBuilder
    {
        public static async Task<ScaffoldEntityFileSet> BuildAsync(
            ScaffoldEntityFileSetRequest request)
        {
            var entityNames = new List<string>();
            var generatedFiles = new List<(string Path, string Content)>();

            foreach (var table in request.Discovery.Tables)
            {
                var file = await BuildEntityFileAsync(request, table).ConfigureAwait(false);
                if (file is null)
                    continue;

                entityNames.Add(file.Value.EntityName);
                generatedFiles.Add((file.Value.Path, file.Value.Content));
            }

            return new ScaffoldEntityFileSet(generatedFiles, entityNames);
        }

        private static async Task<EntityFileResult?> BuildEntityFileAsync(
            ScaffoldEntityFileSetRequest request,
            ScaffoldTable table)
        {
            var tableName = table.Name;
            var schemaName = table.Schema;
            var tableKey = TableKey(schemaName, tableName);
            if (request.Composition.ManyToManyJoinTableKeys.Contains(tableKey))
                return null;

            var discovery = request.Discovery;
            var composition = request.Composition;
            var entityName = discovery.EntityByTable[tableKey];
            var references = composition.Relationships
                .Where(r => string.Equals(r.DependentTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var collections = composition.Relationships
                .Where(r => string.Equals(r.PrincipalTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var manyToManyCollections = ScaffoldRelationshipAdapter.BuildManyToManyNavigations(composition.ManyToManyJoins, tableKey);
            var tableIndexes = discovery.Indexes
                .Where(i => string.Equals(i.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            discovery.ColumnPropertiesByTable.TryGetValue(tableKey, out var columnPropertyNames);
            discovery.FeatureConfigurations.ComputedColumnsByTable.TryGetValue(tableKey, out var computedColumns);
            discovery.FeatureConfigurations.RowVersionColumnsByTable.TryGetValue(tableKey, out var rowVersionColumns);
            discovery.IdentityColumnsByTable.TryGetValue(tableKey, out var identityColumns);
            discovery.NonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns);
            discovery.FeatureConfigurations.DecimalPrecisionByTable.TryGetValue(tableKey, out var decimalPrecisions);
            discovery.StringBinaryFacetsByTable.TryGetValue(tableKey, out var columnFacets);
            discovery.ColumnStoreTypesByTable.TryGetValue(tableKey, out var columnStoreTypes);
            discovery.CommentsByTable.TryGetValue(tableKey, out var comments);
            discovery.SqliteDeclaredTypesByTable.TryGetValue(tableKey, out var sqliteDeclaredTypes);
            discovery.FeatureConfigurations.ProviderSpecificColumnTypesByTable.TryGetValue(tableKey, out var providerSpecificColumnTypes);

            var isQueryArtifact = discovery.QueryArtifactTableKeys.Contains(tableKey);
            var isReadOnlyEntity = ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                tableKey,
                discovery.QueryArtifactTableKeys,
                discovery.FeatureConfigurations.ProviderOwnedWriteBlockedTableKeys,
                discovery.PrimaryKeyColumnsByTable);
            var entityCode = await ScaffoldEntityFileAdapter.ScaffoldEntityAsync(
                request.Connection,
                request.Provider,
                schemaName,
                tableName,
                entityName,
                request.NamespaceName,
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
                isQueryArtifact,
                request.Options.UseNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                columnStoreTypes,
                providerSpecificColumnTypes).ConfigureAwait(false);

            return new EntityFileResult(
                entityName,
                Path.Combine(request.OutputDirectory, entityName + ".cs"),
                entityCode);
        }

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);

        private readonly record struct EntityFileResult(string EntityName, string Path, string Content);
    }
}
