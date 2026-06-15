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
            IReadOnlyList<ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
            IReadOnlyList<ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
            IReadOnlyList<ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
            IReadOnlyList<ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
            IReadOnlyList<ScaffoldCollationConfiguration>? collationConfigurations = null,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>? sequenceStubs = null,
            IReadOnlyList<ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
            IReadOnlyList<ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
            IReadOnlyList<ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
            bool usePluralizer = true,
            bool useNullableReferenceTypes = true,
            string? entityNamespaceName = null,
            bool useDatabaseNames = false)
        {
            compositePrimaryKeys ??= Array.Empty<DatabaseScaffolder.ScaffoldPrimaryKey>();
            defaultValueConfigurations ??= Array.Empty<ScaffoldDefaultValueConfiguration>();
            checkConstraintConfigurations ??= Array.Empty<ScaffoldCheckConstraintConfiguration>();
            computedColumnConfigurations ??= Array.Empty<ScaffoldComputedColumnConfiguration>();
            expressionIndexConfigurations ??= Array.Empty<ScaffoldExpressionIndexConfiguration>();
            collationConfigurations ??= Array.Empty<ScaffoldCollationConfiguration>();
            sequenceStubs ??= Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();
            identityOptionConfigurations ??= Array.Empty<ScaffoldIdentityOptionConfiguration>();
            precisionConfigurations ??= Array.Empty<ScaffoldPrecisionConfiguration>();
            columnFacetConfigurations ??= Array.Empty<ScaffoldColumnFacetConfiguration>();
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
                usePluralizer,
                useNullableReferenceTypes,
                entityNamespaceName,
                useDatabaseNames);
        }

    }
}
