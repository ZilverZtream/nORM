#nullable enable
using System.Collections.Generic;
using System.Data.Common;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldEntitySourceInfo(
        DbConnection Connection,
        DatabaseProvider Provider,
        string? SchemaName,
        string TableName,
        string EntityName,
        string NamespaceName,
        IReadOnlyDictionary<string, string>? ColumnPropertyNames,
        IReadOnlyList<ScaffoldEntityIndexSourceInfo>? Indexes,
        IReadOnlyList<ScaffoldEntityReferenceInfo>? References,
        IReadOnlyList<ScaffoldEntityCollectionInfo>? Collections,
        IReadOnlyList<ScaffoldEntityManyToManyNavigationInfo>? ManyToManyCollections,
        IReadOnlySet<string>? ComputedColumns,
        IReadOnlySet<string>? RowVersionColumns,
        IReadOnlySet<string>? IdentityColumns,
        IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? DecimalPrecisions,
        IReadOnlyDictionary<string, ScaffoldColumnFacet>? ColumnFacets,
        ScaffoldComments? Comments,
        bool IsReadOnlyEntity,
        bool SuppressWriteMetadata,
        bool UseNullableReferenceTypes,
        IReadOnlySet<string>? NonNullableColumns,
        IReadOnlyDictionary<string, string>? SqliteDeclaredTypes,
        IReadOnlyDictionary<string, string>? ColumnStoreTypes,
        IReadOnlyDictionary<string, string>? ProviderSpecificColumnTypes,
        IReadOnlyList<string>? PrimaryKeyColumns = null);

    internal readonly record struct ScaffoldEntityIndexSourceInfo(
        string ColumnName,
        string IndexName,
        bool IsUnique,
        int ColumnCount,
        int Ordinal,
        bool IsDescending,
        bool IsIncluded,
        IndexNullSortOrder NullSortOrder,
        bool NullsNotDistinct,
        string? FilterSql);
}
