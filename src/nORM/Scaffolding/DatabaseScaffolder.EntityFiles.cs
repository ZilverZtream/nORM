#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static async Task EmitScaffoldOutputAsync(
            string outputDirectory,
            List<(string Path, string Content)> generatedFiles,
            string diagnostics,
            string? diagnosticsJson,
            ScaffoldOptions options)
            => await ScaffoldOutputManager.EmitAsync(
                outputDirectory,
                generatedFiles,
                diagnostics,
                diagnosticsJson,
                options).ConfigureAwait(false);

        private static Task<string> ScaffoldEntityAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            string entityName,
            string namespaceName,
            IReadOnlyDictionary<string, string>? columnPropertyNames = null,
            IReadOnlyList<ScaffoldIndex>? indexes = null,
            IReadOnlyList<ScaffoldRelationship>? references = null,
            IReadOnlyList<ScaffoldRelationship>? collections = null,
            IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections = null,
            IReadOnlySet<string>? computedColumns = null,
            IReadOnlySet<string>? rowVersionColumns = null,
            IReadOnlySet<string>? identityColumns = null,
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions = null,
            IReadOnlyDictionary<string, ScaffoldColumnFacet>? columnFacets = null,
            ScaffoldComments? comments = null,
            bool isReadOnlyEntity = false,
            bool useNullableReferenceTypes = true,
            IReadOnlySet<string>? nonNullableColumns = null,
            IReadOnlyDictionary<string, string>? sqliteDeclaredTypes = null,
            IReadOnlyDictionary<string, string>? columnStoreTypes = null,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes = null)
            => ScaffoldEntityFileAdapter.ScaffoldEntityAsync(
                connection,
                provider,
                schemaName,
                tableName,
                entityName,
                namespaceName,
                columnPropertyNames,
                indexes,
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
                useNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                columnStoreTypes,
                providerSpecificColumnTypes);

        private static ScaffoldEntityIndexSourceInfo[] ConvertEntityIndexInfos(IReadOnlyList<ScaffoldIndex>? indexes)
            => ScaffoldEntityFileAdapter.ConvertEntityIndexInfos(indexes);

        private static ScaffoldEntityReferenceInfo[] ConvertEntityReferenceInfos(IReadOnlyList<ScaffoldRelationship>? references)
            => ScaffoldEntityFileAdapter.ConvertEntityReferenceInfos(references);

        private static ScaffoldEntityCollectionInfo[] ConvertEntityCollectionInfos(IReadOnlyList<ScaffoldRelationship>? collections)
            => ScaffoldEntityFileAdapter.ConvertEntityCollectionInfos(collections);

        private static ScaffoldEntityManyToManyNavigationInfo[] ConvertEntityManyToManyNavigationInfos(IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections)
            => ScaffoldEntityFileAdapter.ConvertEntityManyToManyNavigationInfos(manyToManyCollections);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? ConvertEntityDecimalPrecisionInfos(
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions)
            => ScaffoldEntityFileAdapter.ConvertEntityDecimalPrecisionInfos(decimalPrecisions);
    }
}
