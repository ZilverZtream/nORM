#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipAdapter
    {
        public static IReadOnlyList<ScaffoldForeignKeyInfo> ConvertForeignKeyInfos(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys)
        {
            var converted = new ScaffoldForeignKeyInfo[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new ScaffoldForeignKeyInfo(
                    foreignKey.DependentSchema,
                    foreignKey.DependentTable,
                    foreignKey.DependentColumn,
                    foreignKey.PrincipalSchema,
                    foreignKey.PrincipalTable,
                    foreignKey.PrincipalColumn,
                    foreignKey.ConstraintName,
                    foreignKey.ColumnCount,
                    foreignKey.OnDelete,
                    foreignKey.OnUpdate,
                    foreignKey.IsSyntheticConstraintName);
            }

            return converted;
        }

        public static IReadOnlyList<ScaffoldIndexInfo> ConvertIndexInfos(
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            var converted = new ScaffoldIndexInfo[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new ScaffoldIndexInfo(
                    index.TableKey,
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql,
                    index.IsSyntheticName);
            }

            return converted;
        }

        public static IReadOnlyList<ScaffoldManyToManyJoin> ConvertManyToManyJoins(
            IReadOnlyList<ScaffoldManyToManyJoinInfo> joins)
        {
            var converted = new ScaffoldManyToManyJoin[joins.Count];
            for (var i = 0; i < joins.Count; i++)
            {
                var join = joins[i];
                converted[i] = new ScaffoldManyToManyJoin(
                    join.JoinTableKey,
                    join.LeftTableKey,
                    join.RightTableKey,
                    join.JoinTableName,
                    join.JoinTableSchema,
                    join.LeftEntityName,
                    join.RightEntityName,
                    join.LeftForeignKeyColumns,
                    join.RightForeignKeyColumns,
                    join.LeftPrincipalKeyProperties,
                    join.RightPrincipalKeyProperties,
                    join.LeftOnDelete,
                    join.LeftOnUpdate,
                    join.RightOnDelete,
                    join.RightOnUpdate,
                    join.UsesPrimaryKeys,
                    join.LeftCollectionNavigationName,
                    join.RightCollectionNavigationName);
            }

            return converted;
        }

        public static IReadOnlyList<ScaffoldRelationship> ConvertRelationships(
            IReadOnlyList<ScaffoldRelationshipInfo> relationships)
        {
            var converted = new ScaffoldRelationship[relationships.Count];
            for (var i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                converted[i] = new ScaffoldRelationship(
                    relationship.DependentTableKey,
                    relationship.PrincipalTableKey,
                    relationship.DependentEntityName,
                    relationship.PrincipalEntityName,
                    relationship.ForeignKeyPropertyName,
                    relationship.PrincipalKeyPropertyName,
                    relationship.ReferenceNavigationName,
                    relationship.CollectionNavigationName,
                    relationship.IsUniqueDependentKey,
                    relationship.CascadeDelete,
                    relationship.OnDelete,
                    relationship.OnUpdate,
                    relationship.ConstraintName)
                {
                    IsRequired = relationship.IsRequired,
                    ForeignKeyPropertyNames = relationship.ForeignKeyPropertyNames,
                    PrincipalKeyPropertyNames = relationship.PrincipalKeyPropertyNames
                };
            }

            return converted;
        }
    }
}
