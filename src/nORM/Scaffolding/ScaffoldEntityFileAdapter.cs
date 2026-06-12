#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
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
            => await ScaffoldEntityFileSetBuilder.BuildAsync(
                connection,
                provider,
                outputDirectory,
                namespaceName,
                tables,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                nonNullableColumnsByTable,
                sqliteDeclaredTypesByTable,
                stringBinaryFacetsByTable,
                commentsByTable,
                identityColumnsByTable,
                indexes,
                relationships,
                manyToManyJoins,
                manyToManyJoinTableKeys,
                queryArtifactTableKeys,
                featureConfigurations,
                options).ConfigureAwait(false);

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
    }
}
