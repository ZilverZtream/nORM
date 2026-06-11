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
    internal static class ScaffoldEntityFileAdapter
    {
        public static async Task<DatabaseScaffolder.ScaffoldEntityFileSet> BuildScaffoldEntityFilesAsync(
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
            var entityNames = new List<string>();
            var generatedFiles = new List<(string Path, string Content)>();
            foreach (var table in tables)
            {
                var tableName = table.Name;
                var schemaName = table.Schema;

                var tableKey = TableKey(schemaName, tableName);
                if (manyToManyJoinTableKeys.Contains(tableKey))
                    continue;

                var entityName = entityByTable[tableKey];
                entityNames.Add(entityName);

                var references = relationships.Where(r => string.Equals(r.DependentTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                var collections = relationships.Where(r => string.Equals(r.PrincipalTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                var manyToManyCollections = ScaffoldRelationshipAdapter.BuildManyToManyNavigations(manyToManyJoins, tableKey);
                var tableIndexes = indexes.Where(i => string.Equals(i.TableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                columnPropertiesByTable.TryGetValue(tableKey, out var columnPropertyNames);
                featureConfigurations.ComputedColumnsByTable.TryGetValue(tableKey, out var computedColumns);
                featureConfigurations.RowVersionColumnsByTable.TryGetValue(tableKey, out var rowVersionColumns);
                identityColumnsByTable.TryGetValue(tableKey, out var identityColumns);
                nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns);
                featureConfigurations.DecimalPrecisionByTable.TryGetValue(tableKey, out var decimalPrecisions);
                stringBinaryFacetsByTable.TryGetValue(tableKey, out var columnFacets);
                commentsByTable.TryGetValue(tableKey, out var comments);
                sqliteDeclaredTypesByTable.TryGetValue(tableKey, out var sqliteDeclaredTypes);
                featureConfigurations.ProviderSpecificColumnTypesByTable.TryGetValue(tableKey, out var providerSpecificColumnTypes);
                var isReadOnlyEntity = ShouldMarkScaffoldedEntityReadOnly(
                    tableKey,
                    queryArtifactTableKeys,
                    featureConfigurations.ProviderNativeTemporalTableKeys,
                    featureConfigurations.ProviderOwnedTriggerTableKeys,
                    featureConfigurations.ProviderSpecificIdentityStrategyTableKeys,
                    featureConfigurations.ProviderSpecificDefaultTableKeys,
                    providerSpecificColumnTypes,
                    primaryKeyColumnsByTable);
                var entityCode = await ScaffoldEntityAsync(connection, provider, schemaName, tableName, entityName, namespaceName, columnPropertyNames, tableIndexes, references, collections, manyToManyCollections, computedColumns, rowVersionColumns, identityColumns, decimalPrecisions, columnFacets, comments, isReadOnlyEntity, options.UseNullableReferenceTypes, nonNullableColumns, sqliteDeclaredTypes, providerSpecificColumnTypes).ConfigureAwait(false);
                generatedFiles.Add((Path.Combine(outputDirectory, entityName + ".cs"), entityCode));
            }

            return new DatabaseScaffolder.ScaffoldEntityFileSet(generatedFiles, entityNames);
        }

        public static Task<string> ScaffoldEntityAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            string entityName,
            string namespaceName,
            IReadOnlyDictionary<string, string>? columnPropertyNames = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex>? indexes = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship>? references = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship>? collections = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyNavigation>? manyToManyCollections = null,
            IReadOnlySet<string>? computedColumns = null,
            IReadOnlySet<string>? rowVersionColumns = null,
            IReadOnlySet<string>? identityColumns = null,
            IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>? decimalPrecisions = null,
            IReadOnlyDictionary<string, ScaffoldColumnFacet>? columnFacets = null,
            ScaffoldComments? comments = null,
            bool isReadOnlyEntity = false,
            bool useNullableReferenceTypes = true,
            IReadOnlySet<string>? nonNullableColumns = null,
            IReadOnlyDictionary<string, string>? sqliteDeclaredTypes = null,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes = null)
            => ScaffoldEntitySourceBuilder.BuildAsync(new ScaffoldEntitySourceInfo(
                connection,
                provider,
                schemaName,
                tableName,
                entityName,
                namespaceName,
                columnPropertyNames,
                ConvertEntityIndexInfos(indexes),
                ConvertEntityReferenceInfos(references),
                ConvertEntityCollectionInfos(collections),
                ConvertEntityManyToManyNavigationInfos(manyToManyCollections),
                computedColumns,
                rowVersionColumns,
                identityColumns,
                ConvertEntityDecimalPrecisionInfos(decimalPrecisions),
                columnFacets,
                comments,
                isReadOnlyEntity,
                useNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                providerSpecificColumnTypes));

        public static ScaffoldEntityIndexSourceInfo[] ConvertEntityIndexInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex>? indexes)
            => (indexes ?? Array.Empty<DatabaseScaffolder.ScaffoldIndex>())
                .Select(index => new ScaffoldEntityIndexSourceInfo(
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql))
                .ToArray();

        public static ScaffoldEntityReferenceInfo[] ConvertEntityReferenceInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship>? references)
            => (references ?? Array.Empty<DatabaseScaffolder.ScaffoldRelationship>())
                .Select(reference => new ScaffoldEntityReferenceInfo(
                    reference.PrincipalEntityName,
                    reference.ReferenceNavigationName,
                    reference.ForeignKeyPropertyName,
                    reference.IsComposite,
                    reference.IsRequired))
                .ToArray();

        public static ScaffoldEntityCollectionInfo[] ConvertEntityCollectionInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship>? collections)
            => (collections ?? Array.Empty<DatabaseScaffolder.ScaffoldRelationship>())
                .Select(collection => new ScaffoldEntityCollectionInfo(
                    collection.DependentEntityName,
                    collection.CollectionNavigationName,
                    collection.ForeignKeyPropertyName,
                    collection.IsUniqueDependentKey))
                .ToArray();

        public static ScaffoldEntityManyToManyNavigationInfo[] ConvertEntityManyToManyNavigationInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyNavigation>? manyToManyCollections)
            => (manyToManyCollections ?? Array.Empty<DatabaseScaffolder.ScaffoldManyToManyNavigation>())
                .Select(collection => new ScaffoldEntityManyToManyNavigationInfo(
                    collection.TargetEntityName,
                    collection.CollectionNavigationName))
                .ToArray();

        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? ConvertEntityDecimalPrecisionInfos(
            IReadOnlyDictionary<string, DatabaseScaffolder.ScaffoldDecimalPrecision>? decimalPrecisions)
            => decimalPrecisions?.ToDictionary(
                pair => pair.Key,
                pair => new ScaffoldDecimalPrecisionInfo(pair.Value.Precision, pair.Value.Scale),
                StringComparer.OrdinalIgnoreCase);

        public static bool ShouldMarkScaffoldedEntityReadOnly(
            string tableKey,
            IReadOnlySet<string> queryArtifactTableKeys,
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => queryArtifactTableKeys.Contains(tableKey)
               || providerNativeTemporalTableKeys.Contains(tableKey)
               || providerOwnedTriggerTableKeys.Contains(tableKey)
               || providerSpecificIdentityStrategyTableKeys.Contains(tableKey)
               || providerSpecificDefaultTableKeys.Contains(tableKey)
               || ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes)
               || !primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
               || primaryKeyColumns.Count == 0;

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
