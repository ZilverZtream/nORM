#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextInfoFactory
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

    }
}
