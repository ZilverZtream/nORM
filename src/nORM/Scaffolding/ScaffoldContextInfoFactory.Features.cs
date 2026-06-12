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
            IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> defaultValues)
            => defaultValues
                .Select(defaultValue => new ScaffoldContextDefaultValueInfo(defaultValue.EntityName, defaultValue.PropertyName, defaultValue.DefaultValueSql))
                .ToArray();

        private static ScaffoldContextCheckConstraintInfo[] ConvertContextCheckConstraintInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldCheckConstraintConfiguration> checks)
            => checks
                .Select(check => new ScaffoldContextCheckConstraintInfo(check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldContextComputedColumnInfo[] ConvertContextComputedColumnInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldComputedColumnConfiguration> computedColumns)
            => computedColumns
                .Select(computed => new ScaffoldContextComputedColumnInfo(computed.EntityName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static ScaffoldContextExpressionIndexInfo[] ConvertContextExpressionIndexInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldExpressionIndexConfiguration> expressionIndexes)
            => expressionIndexes
                .Select(index => new ScaffoldContextExpressionIndexInfo(index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql))
                .ToArray();

        private static ScaffoldContextCollationInfo[] ConvertContextCollationInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldCollationConfiguration> collations)
            => collations
                .Select(collation => new ScaffoldContextCollationInfo(collation.EntityName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static ScaffoldContextIdentityOptionInfo[] ConvertContextIdentityOptionInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIdentityOptionConfiguration> identityOptions)
            => identityOptions
                .Select(identity => new ScaffoldContextIdentityOptionInfo(identity.EntityName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();

        private static ScaffoldContextPrecisionInfo[] ConvertContextPrecisionInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldPrecisionConfiguration> precisionConfigurations)
            => precisionConfigurations
                .Select(precision => new ScaffoldContextPrecisionInfo(precision.EntityName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static ScaffoldContextColumnFacetInfo[] ConvertContextColumnFacetInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldColumnFacetConfiguration> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(facet => new ScaffoldContextColumnFacetInfo(facet.EntityName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();
    }
}
