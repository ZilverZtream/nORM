#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityFileAdapter
    {
        public static async Task<DatabaseScaffolder.ScaffoldEntityFileSet> BuildScaffoldEntityFilesAsync(
            ScaffoldEntityFileSetRequest request)
            => await ScaffoldEntityFileSetBuilder.BuildAsync(request).ConfigureAwait(false);

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
            bool suppressWriteMetadata = false,
            bool useNullableReferenceTypes = true,
            IReadOnlySet<string>? nonNullableColumns = null,
            IReadOnlyDictionary<string, string>? sqliteDeclaredTypes = null,
            IReadOnlyDictionary<string, string>? columnStoreTypes = null,
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
                suppressWriteMetadata,
                useNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                columnStoreTypes,
                providerSpecificColumnTypes));
    }
}
