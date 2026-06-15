#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        private static IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> ConvertCheckConstraintConfigurations(
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checks)
            => checks
                .Select(static check => new DatabaseScaffolder.ScaffoldCheckConstraintConfiguration(check.TableKey, check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldCheckConstraintConfigurationInfo ConvertCheckConstraintConfiguration(
            DatabaseScaffolder.ScaffoldCheckConstraintConfiguration check)
            => new(check.TableKey, check.EntityName, check.Name, check.Sql);

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> ConvertExpressionIndexConfigurations(
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexes)
            => expressionIndexes
                .Select(static index => new DatabaseScaffolder.ScaffoldExpressionIndexConfiguration(index.TableKey, index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql)
                {
                    IncludedColumnNames = index.IncludedColumnNames,
                    NullSortOrder = index.NullSortOrder,
                    NullsNotDistinct = index.NullsNotDistinct
                })
                .ToArray();
    }
}
