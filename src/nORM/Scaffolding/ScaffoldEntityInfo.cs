#nullable enable
using System;
using System.Collections.Generic;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldEntityInfo(
        string NamespaceName,
        string EntityName,
        string TableName,
        string? SchemaName,
        string? TableComment,
        bool HasIndexes,
        bool IsReadOnlyEntity,
        bool UseNullableReferenceTypes,
        IReadOnlyList<ScaffoldEntityColumnInfo> Columns,
        IReadOnlyList<ScaffoldEntityReferenceInfo> References,
        IReadOnlyList<ScaffoldEntityCollectionInfo> Collections,
        IReadOnlyList<ScaffoldEntityManyToManyNavigationInfo> ManyToManyCollections);

    internal sealed record ScaffoldEntityColumnInfo(
        string ColumnName,
        string PropertyName,
        Type ClrType,
        bool EffectiveAllowNull,
        bool IsKey,
        bool IsAutoIncrement,
        bool IsComputed,
        bool IsRowVersion,
        int? MaxLength,
        ScaffoldEntityDecimalPrecisionInfo? DecimalPrecision,
        string? Comment,
        IReadOnlyList<ScaffoldEntityIndexInfo> Indexes);

    internal readonly record struct ScaffoldEntityIndexInfo(
        string IndexName,
        bool IsUnique,
        int ColumnCount,
        int Ordinal,
        bool IsDescending,
        bool IsIncluded,
        IndexNullSortOrder NullSortOrder,
        bool NullsNotDistinct,
        string? FilterSql);

    internal readonly record struct ScaffoldEntityReferenceInfo(
        string PrincipalEntityName,
        string ReferenceNavigationName,
        string ForeignKeyPropertyName,
        bool IsComposite,
        bool IsRequired);

    internal readonly record struct ScaffoldEntityCollectionInfo(
        string DependentEntityName,
        string CollectionNavigationName,
        string ForeignKeyPropertyName,
        bool IsUniqueDependentKey);

    internal readonly record struct ScaffoldEntityManyToManyNavigationInfo(
        string TargetEntityName,
        string CollectionNavigationName);

    internal readonly record struct ScaffoldEntityDecimalPrecisionInfo(
        int Precision,
        int? Scale);
}
