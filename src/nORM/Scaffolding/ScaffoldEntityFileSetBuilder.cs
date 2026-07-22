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
            var tableKey = TableKey(table.Schema, table.Name);
            if (request.Composition.ManyToManyJoinTableKeys.Contains(tableKey))
                return null;

            var tableContext = new EntityTableContext(
                table.Name,
                table.Schema,
                tableKey,
                request.Discovery.EntityByTable[tableKey]);
            var entityCode = await ScaffoldEntityFileAdapter.ScaffoldEntityAsync(
                BuildEntitySourceInfo(request, tableContext)).ConfigureAwait(false);

            return new EntityFileResult(
                tableContext.EntityName,
                Path.Combine(request.OutputDirectory, tableContext.EntityName + ".cs"),
                entityCode);
        }

        private static ScaffoldEntitySourceInfo BuildEntitySourceInfo(
            ScaffoldEntityFileSetRequest request,
            EntityTableContext table)
        {
            var discovery = request.Discovery;
            var composition = request.Composition;
            var references = composition.Relationships
                .Where(r => string.Equals(r.DependentTableKey, table.TableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var collections = composition.Relationships
                .Where(r => string.Equals(r.PrincipalTableKey, table.TableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            var manyToManyCollections = ScaffoldRelationshipAdapter.BuildManyToManyNavigations(composition.ManyToManyJoins, table.TableKey);
            var tableIndexes = discovery.Indexes
                .Where(i => string.Equals(i.TableKey, table.TableKey, StringComparison.OrdinalIgnoreCase))
                .ToArray();
            discovery.ColumnPropertiesByTable.TryGetValue(table.TableKey, out var columnPropertyNames);
            discovery.FeatureConfigurations.ComputedColumnsByTable.TryGetValue(table.TableKey, out var computedColumns);
            discovery.FeatureConfigurations.RowVersionColumnsByTable.TryGetValue(table.TableKey, out var rowVersionColumns);
            discovery.IdentityColumnsByTable.TryGetValue(table.TableKey, out var identityColumns);
            discovery.NonNullableColumnsByTable.TryGetValue(table.TableKey, out var nonNullableColumns);
            discovery.FeatureConfigurations.DecimalPrecisionByTable.TryGetValue(table.TableKey, out var decimalPrecisions);
            discovery.StringBinaryFacetsByTable.TryGetValue(table.TableKey, out var columnFacets);
            discovery.ColumnStoreTypesByTable.TryGetValue(table.TableKey, out var columnStoreTypes);
            discovery.CommentsByTable.TryGetValue(table.TableKey, out var comments);
            discovery.SqliteDeclaredTypesByTable.TryGetValue(table.TableKey, out var sqliteDeclaredTypes);
            discovery.FeatureConfigurations.ProviderSpecificColumnTypesByTable.TryGetValue(table.TableKey, out var providerSpecificColumnTypes);
            discovery.PrimaryKeyColumnsByTable.TryGetValue(table.TableKey, out var primaryKeyColumns);

            var isQueryArtifact = discovery.QueryArtifactTableKeys.Contains(table.TableKey);
            var isReadOnlyEntity = ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                table.TableKey,
                discovery.QueryArtifactTableKeys,
                discovery.FeatureConfigurations.ProviderOwnedWriteBlockedTableKeys,
                discovery.PrimaryKeyColumnsByTable);

            return new ScaffoldEntitySourceInfo(
                request.Connection,
                request.Provider,
                table.SchemaName,
                table.TableName,
                table.EntityName,
                request.NamespaceName,
                columnPropertyNames,
                ScaffoldEntityFileAdapter.ConvertEntityIndexInfos(tableIndexes),
                ScaffoldEntityFileAdapter.ConvertEntityReferenceInfos(references),
                ScaffoldEntityFileAdapter.ConvertEntityCollectionInfos(collections),
                ScaffoldEntityFileAdapter.ConvertEntityManyToManyNavigationInfos(manyToManyCollections),
                computedColumns,
                rowVersionColumns,
                identityColumns,
                ScaffoldEntityFileAdapter.ConvertEntityDecimalPrecisionInfos(decimalPrecisions),
                columnFacets,
                comments,
                isReadOnlyEntity,
                isQueryArtifact,
                request.Options.UseNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                columnStoreTypes,
                providerSpecificColumnTypes,
                primaryKeyColumns);
        }

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);

        private readonly record struct EntityTableContext(
            string TableName,
            string? SchemaName,
            string TableKey,
            string EntityName);

        private readonly record struct EntityFileResult(string EntityName, string Path, string Content);
    }
}
