using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldIndexInfo(
        string TableKey,
        string ColumnName,
        string IndexName,
        bool IsUnique,
        int ColumnCount,
        int Ordinal,
        bool IsDescending,
        bool IsIncluded,
        IndexNullSortOrder NullSortOrder,
        bool NullsNotDistinct,
        string? FilterSql,
        bool IsSyntheticName = false);
}
