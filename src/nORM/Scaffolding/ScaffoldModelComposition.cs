#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldModelComposition(
        IReadOnlyList<ScaffoldManyToManyJoin> ManyToManyJoins,
        IReadOnlySet<string> ManyToManyJoinTableKeys,
        IReadOnlyList<ScaffoldRelationship> Relationships,
        IReadOnlyList<ScaffoldPrimaryKey> CompositePrimaryKeys,
        IReadOnlyList<ScaffoldDefaultValueConfiguration> DefaultValueConfigurations,
        IReadOnlyList<ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyList<ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlyList<ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations);
}
