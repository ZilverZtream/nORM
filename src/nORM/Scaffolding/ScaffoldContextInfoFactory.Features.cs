#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextInfoFactory
    {
        private static ScaffoldContextPrimaryKeyInfo[] ConvertContextPrimaryKeyInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrimaryKey> primaryKeys)
            => primaryKeys
                .Select(key => new ScaffoldContextPrimaryKeyInfo(key.EntityName, key.PropertyNames, key.ConstraintName))
                .ToArray();

        private static ScaffoldContextDefaultValueInfo[] ConvertContextDefaultValueInfos(
            IReadOnlyList<ScaffoldDefaultValueConfiguration> defaultValues)
            => defaultValues
                .Select(defaultValue => new ScaffoldContextDefaultValueInfo(defaultValue.EntityName, defaultValue.PropertyName, defaultValue.DefaultValueSql, defaultValue.ConstraintName))
                .ToArray();

        private static ScaffoldContextCheckConstraintInfo[] ConvertContextCheckConstraintInfos(
            IReadOnlyList<ScaffoldCheckConstraintConfiguration> checks)
            => checks
                .Select(check => new ScaffoldContextCheckConstraintInfo(check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldContextComputedColumnInfo[] ConvertContextComputedColumnInfos(
            IReadOnlyList<ScaffoldComputedColumnConfiguration> computedColumns)
            => computedColumns
                .Select(computed => new ScaffoldContextComputedColumnInfo(computed.EntityName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static ScaffoldContextExpressionIndexInfo[] ConvertContextExpressionIndexInfos(
            IReadOnlyList<ScaffoldExpressionIndexConfiguration> expressionIndexes)
            => expressionIndexes
                .Select(index => new ScaffoldContextExpressionIndexInfo(index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql)
                {
                    IncludedColumnNames = index.IncludedColumnNames,
                    NullSortOrder = index.NullSortOrder,
                    NullsNotDistinct = index.NullsNotDistinct
                })
                .ToArray();

        private static ScaffoldContextCollationInfo[] ConvertContextCollationInfos(
            IReadOnlyList<ScaffoldCollationConfiguration> collations)
            => collations
                .Select(collation => new ScaffoldContextCollationInfo(collation.EntityName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static ScaffoldContextIdentityOptionInfo[] ConvertContextIdentityOptionInfos(
            IReadOnlyList<ScaffoldIdentityOptionConfiguration> identityOptions)
            => identityOptions
                .Select(identity => new ScaffoldContextIdentityOptionInfo(identity.EntityName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();

        private static ScaffoldContextPrecisionInfo[] ConvertContextPrecisionInfos(
            IReadOnlyList<ScaffoldPrecisionConfiguration> precisionConfigurations)
            => precisionConfigurations
                .Select(precision => new ScaffoldContextPrecisionInfo(precision.EntityName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static ScaffoldContextColumnFacetInfo[] ConvertContextColumnFacetInfos(
            IReadOnlyList<ScaffoldColumnFacetConfiguration> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(facet => new ScaffoldContextColumnFacetInfo(facet.EntityName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();
    }
}
