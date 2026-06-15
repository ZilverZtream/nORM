#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextInfoFactory
    {
        private static ScaffoldContextRelationshipInfo[] ConvertContextRelationshipInfos(
            IReadOnlyList<ScaffoldRelationship> relationships)
        {
            var converted = new ScaffoldContextRelationshipInfo[relationships.Count];
            for (var i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                converted[i] = new ScaffoldContextRelationshipInfo(
                    relationship.DependentEntityName,
                    relationship.PrincipalEntityName,
                    relationship.ReferenceNavigationName,
                    relationship.CollectionNavigationName,
                    relationship.IsUniqueDependentKey,
                    relationship.CascadeDelete,
                    relationship.OnDelete,
                    relationship.OnUpdate,
                    relationship.ConstraintName,
                    relationship.ForeignKeyPropertyNames,
                    relationship.PrincipalKeyPropertyNames);
            }

            return converted;
        }

        private static ScaffoldManyToManyJoinInfo[] ConvertManyToManyJoinInfos(
            IReadOnlyList<ScaffoldManyToManyJoin> joins)
        {
            var converted = new ScaffoldManyToManyJoinInfo[joins.Count];
            for (var i = 0; i < joins.Count; i++)
            {
                var join = joins[i];
                converted[i] = new ScaffoldManyToManyJoinInfo(
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
    }
}
