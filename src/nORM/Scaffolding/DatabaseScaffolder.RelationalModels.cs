#nullable enable
using System.Collections.Generic;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        internal readonly record struct ScaffoldForeignKey(
            string? DependentSchema,
            string DependentTable,
            string DependentColumn,
            string? PrincipalSchema,
            string PrincipalTable,
            string PrincipalColumn,
            string ConstraintName,
            int ColumnCount,
            string OnDelete = "NO ACTION",
            string OnUpdate = "NO ACTION",
            bool IsSyntheticConstraintName = false);

        internal readonly record struct ScaffoldIndex(
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

        internal readonly record struct ScaffoldRelationship(
            string DependentTableKey,
            string PrincipalTableKey,
            string DependentEntityName,
            string PrincipalEntityName,
            string ForeignKeyPropertyName,
            string PrincipalKeyPropertyName,
            string ReferenceNavigationName,
            string CollectionNavigationName,
            bool IsUniqueDependentKey,
            bool CascadeDelete,
            string OnDelete,
            string OnUpdate,
            string? ConstraintName)
        {
            public IReadOnlyList<string> ForeignKeyPropertyNames { get; init; } = new[] { ForeignKeyPropertyName };

            public IReadOnlyList<string> PrincipalKeyPropertyNames { get; init; } = new[] { PrincipalKeyPropertyName };

            public bool IsRequired { get; init; }

            public bool IsComposite => ForeignKeyPropertyNames.Count > 1 || PrincipalKeyPropertyNames.Count > 1;
        }

        internal readonly record struct ScaffoldManyToManyJoin(
            string JoinTableKey,
            string LeftTableKey,
            string RightTableKey,
            string JoinTableName,
            string? JoinTableSchema,
            string LeftEntityName,
            string RightEntityName,
            string[] LeftForeignKeyColumns,
            string[] RightForeignKeyColumns,
            string[] LeftPrincipalKeyProperties,
            string[] RightPrincipalKeyProperties,
            string LeftOnDelete,
            string LeftOnUpdate,
            string RightOnDelete,
            string RightOnUpdate,
            bool UsesPrimaryKeys,
            string LeftCollectionNavigationName,
            string RightCollectionNavigationName)
        {
            public string LeftForeignKeyColumn => LeftForeignKeyColumns[0];

            public string RightForeignKeyColumn => RightForeignKeyColumns[0];

            public bool IsComposite => LeftForeignKeyColumns.Length > 1 || RightForeignKeyColumns.Length > 1;
        }
    }
}
