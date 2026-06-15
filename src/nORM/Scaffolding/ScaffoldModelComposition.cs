#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldModelComposition(
        IReadOnlyList<DatabaseScaffolder.ScaffoldManyToManyJoin> ManyToManyJoins,
        IReadOnlySet<string> ManyToManyJoinTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldRelationship> Relationships,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> CompositePrimaryKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> DefaultValueConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> CheckConstraints,
        IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> CollationConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> PrecisionConfigurations,
        IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations);
}
