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
