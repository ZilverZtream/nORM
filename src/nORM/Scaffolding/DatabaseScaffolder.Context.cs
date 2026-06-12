#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
            => ScaffoldContext(namespaceName, contextName, entities, usePluralizer: true);

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities, bool usePluralizer)
            => ScaffoldContextWithRelationships(namespaceName, contextName, entities, Array.Empty<ScaffoldRelationship>(), Array.Empty<ScaffoldManyToManyJoin>(), usePluralizer: usePluralizer);

        private static string ScaffoldContextWithRelationships(
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
            => ScaffoldContextAdapter.Write(
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
                useDatabaseNames);
    }
}
