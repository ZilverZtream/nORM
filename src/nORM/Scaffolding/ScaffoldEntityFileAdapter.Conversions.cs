#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityFileAdapter
    {
        public static ScaffoldEntityIndexSourceInfo[] ConvertEntityIndexInfos(
            IReadOnlyList<ScaffoldIndex>? indexes)
            => (indexes ?? Array.Empty<ScaffoldIndex>())
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
            IReadOnlyList<ScaffoldRelationship>? references)
            => (references ?? Array.Empty<ScaffoldRelationship>())
                .Select(reference => new ScaffoldEntityReferenceInfo(
                    reference.PrincipalEntityName,
                    reference.ReferenceNavigationName,
                    reference.ForeignKeyPropertyName,
                    reference.IsComposite,
                    reference.IsRequired))
                .ToArray();

        public static ScaffoldEntityCollectionInfo[] ConvertEntityCollectionInfos(
            IReadOnlyList<ScaffoldRelationship>? collections)
            => (collections ?? Array.Empty<ScaffoldRelationship>())
                .Select(collection => new ScaffoldEntityCollectionInfo(
                    collection.DependentEntityName,
                    collection.CollectionNavigationName,
                    collection.ForeignKeyPropertyName,
                    collection.IsUniqueDependentKey))
                .ToArray();

        public static ScaffoldEntityManyToManyNavigationInfo[] ConvertEntityManyToManyNavigationInfos(
            IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections)
            => (manyToManyCollections ?? Array.Empty<ScaffoldManyToManyNavigation>())
                .Select(collection => new ScaffoldEntityManyToManyNavigationInfo(
                    collection.TargetEntityName,
                    collection.CollectionNavigationName))
                .ToArray();

        public static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? ConvertEntityDecimalPrecisionInfos(
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions)
            => decimalPrecisions?.ToDictionary(
                pair => pair.Key,
                pair => new ScaffoldDecimalPrecisionInfo(pair.Value.Precision, pair.Value.Scale),
                StringComparer.OrdinalIgnoreCase);
    }
}
