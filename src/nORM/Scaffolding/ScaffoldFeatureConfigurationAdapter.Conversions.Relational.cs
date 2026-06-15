#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationAdapter
    {
        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> ConvertCheckConstraintConfigurations(
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checks)
            => checks
                .Select(static check => new ScaffoldCheckConstraintConfiguration(check.TableKey, check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldCheckConstraintConfigurationInfo ConvertCheckConstraintConfiguration(
            ScaffoldCheckConstraintConfiguration check)
            => new(check.TableKey, check.EntityName, check.Name, check.Sql);

        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> ConvertExpressionIndexConfigurations(
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexes)
            => expressionIndexes
                .Select(static index => new ScaffoldExpressionIndexConfiguration(index.TableKey, index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql)
                {
                    IncludedColumnNames = index.IncludedColumnNames,
                    NullSortOrder = index.NullSortOrder,
                    NullsNotDistinct = index.NullsNotDistinct
                })
                .ToArray();
    }
}
