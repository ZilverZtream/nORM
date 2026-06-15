#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static class ScaffoldContextAdapter
    {
        public static string Write(
            string namespaceName,
            string contextName,
            IEnumerable<string> entities,
            IReadOnlyList<ScaffoldRelationship> relationships,
            IReadOnlyList<ScaffoldManyToManyJoin> manyToManyJoins,
            IReadOnlyList<ScaffoldSkippedObject>? routineStubs = null,
            IReadOnlyList<ScaffoldPrimaryKey>? compositePrimaryKeys = null,
            IReadOnlyList<ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
            IReadOnlyList<ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
            IReadOnlyList<ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
            IReadOnlyList<ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
            IReadOnlyList<ScaffoldCollationConfiguration>? collationConfigurations = null,
            IReadOnlyList<ScaffoldSkippedObject>? sequenceStubs = null,
            IReadOnlyList<ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
            IReadOnlyList<ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
            IReadOnlyList<ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
            bool usePluralizer = true,
            bool useNullableReferenceTypes = true,
            string? entityNamespaceName = null,
            bool useDatabaseNames = false)
            => ScaffoldContextWriter.Write(ScaffoldContextInfoFactory.Create(
                namespaceName,
                contextName,
                entities,
                relationships,
                manyToManyJoins,
                routineStubs,
                compositePrimaryKeys,
                defaultValueConfigurations,
                checkConstraintConfigurations,
                computedColumnConfigurations,
                expressionIndexConfigurations,
                collationConfigurations,
                sequenceStubs,
                identityOptionConfigurations,
                precisionConfigurations,
                columnFacetConfigurations,
                usePluralizer,
                useNullableReferenceTypes,
                entityNamespaceName,
                useDatabaseNames));
    }
}
