#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldContextInfoFactory
    {
        public static ScaffoldContextInfo Create(
            string namespaceName,
            string contextName,
            IEnumerable<string> entities,
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> relationships,
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> manyToManyJoins,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>? routineStubs = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey>? compositePrimaryKeys = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration>? collationConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>? sequenceStubs = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
            bool pluralizeQueryProperties = true,
            bool useNullableReferenceTypes = true,
            string? entityNamespaceName = null,
            bool useDatabaseNames = false)
        {
            compositePrimaryKeys ??= Array.Empty<DatabaseScaffolder.ScaffoldPrimaryKey>();
            defaultValueConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldDefaultValueConfiguration>();
            checkConstraintConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration>();
            computedColumnConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldComputedColumnConfiguration>();
            expressionIndexConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration>();
            collationConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldCollationConfiguration>();
            sequenceStubs ??= Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();
            identityOptionConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration>();
            precisionConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldPrecisionConfiguration>();
            columnFacetConfigurations ??= Array.Empty<DatabaseScaffolder.ScaffoldColumnFacetConfiguration>();
            routineStubs ??= Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();

            return new ScaffoldContextInfo(
                namespaceName,
                contextName,
                entities.ToArray(),
                ConvertContextRelationshipInfos(relationships),
                ConvertManyToManyJoinInfos(manyToManyJoins),
                ConvertRoutineStubInfos(routineStubs),
                ConvertContextPrimaryKeyInfos(compositePrimaryKeys),
                ConvertContextDefaultValueInfos(defaultValueConfigurations),
                ConvertContextCheckConstraintInfos(checkConstraintConfigurations),
                ConvertContextComputedColumnInfos(computedColumnConfigurations),
                ConvertContextExpressionIndexInfos(expressionIndexConfigurations),
                ConvertContextCollationInfos(collationConfigurations),
                ConvertContextSequenceInfos(sequenceStubs),
                ConvertContextIdentityOptionInfos(identityOptionConfigurations),
                ConvertContextPrecisionInfos(precisionConfigurations),
                ConvertContextColumnFacetInfos(columnFacetConfigurations),
                pluralizeQueryProperties,
                useNullableReferenceTypes,
                entityNamespaceName,
                useDatabaseNames);
        }

        private static ScaffoldContextRelationshipInfo[] ConvertContextRelationshipInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> relationships)
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
            IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> joins)
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

        private static ScaffoldRoutineStubInfo[] ConvertRoutineStubInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> routineStubs)
        {
            var converted = new ScaffoldRoutineStubInfo[routineStubs.Count];
            for (var i = 0; i < routineStubs.Count; i++)
            {
                var routine = routineStubs[i];
                converted[i] = new ScaffoldRoutineStubInfo(
                    routine.Schema,
                    routine.Name,
                    routine.Kind,
                    routine.Detail,
                    routine.Comment,
                    BuildSkippedObjectMetadata(routine));
            }

            return converted;
        }

        private static ScaffoldContextPrimaryKeyInfo[] ConvertContextPrimaryKeyInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> primaryKeys)
            => primaryKeys
                .Select(key => new ScaffoldContextPrimaryKeyInfo(key.EntityName, key.PropertyNames, key.ConstraintName))
                .ToArray();

        private static ScaffoldContextDefaultValueInfo[] ConvertContextDefaultValueInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> defaultValues)
            => defaultValues
                .Select(defaultValue => new ScaffoldContextDefaultValueInfo(defaultValue.EntityName, defaultValue.PropertyName, defaultValue.DefaultValueSql))
                .ToArray();

        private static ScaffoldContextCheckConstraintInfo[] ConvertContextCheckConstraintInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> checks)
            => checks
                .Select(check => new ScaffoldContextCheckConstraintInfo(check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldContextComputedColumnInfo[] ConvertContextComputedColumnInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> computedColumns)
            => computedColumns
                .Select(computed => new ScaffoldContextComputedColumnInfo(computed.EntityName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static ScaffoldContextExpressionIndexInfo[] ConvertContextExpressionIndexInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> expressionIndexes)
            => expressionIndexes
                .Select(index => new ScaffoldContextExpressionIndexInfo(index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql))
                .ToArray();

        private static ScaffoldContextCollationInfo[] ConvertContextCollationInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> collations)
            => collations
                .Select(collation => new ScaffoldContextCollationInfo(collation.EntityName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static ScaffoldContextSequenceInfo[] ConvertContextSequenceInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> sequenceStubs)
            => sequenceStubs
                .Select(sequence => new ScaffoldContextSequenceInfo(sequence.Schema, sequence.Name, sequence.Detail, sequence.Comment))
                .ToArray();

        private static ScaffoldContextIdentityOptionInfo[] ConvertContextIdentityOptionInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> identityOptions)
            => identityOptions
                .Select(identity => new ScaffoldContextIdentityOptionInfo(identity.EntityName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();

        private static ScaffoldContextPrecisionInfo[] ConvertContextPrecisionInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> precisionConfigurations)
            => precisionConfigurations
                .Select(precision => new ScaffoldContextPrecisionInfo(precision.EntityName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static ScaffoldContextColumnFacetInfo[] ConvertContextColumnFacetInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(facet => new ScaffoldContextColumnFacetInfo(facet.EntityName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(
            DatabaseScaffolder.ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));
    }
}
