#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldModelComposition(
        IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> ManyToManyJoins,
        IReadOnlySet<string> ManyToManyJoinTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> Relationships,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> CompositePrimaryKeys,
        IReadOnlyList<ScaffoldDefaultValueConfiguration> DefaultValueConfigurations,
        IReadOnlyList<ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyList<ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlyList<ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations);
}
