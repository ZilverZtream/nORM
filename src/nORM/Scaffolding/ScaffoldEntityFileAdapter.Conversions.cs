#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldEntityFileAdapter
    {
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
