#nullable enable
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static void AppendModelConfiguration(StringBuilder sb, ScaffoldContextInfo context)
        {
            AppendPrimaryKeyConfigurations(sb, context.CompositePrimaryKeys);
            AppendDefaultValueConfigurations(sb, context.DefaultValueConfigurations);
            AppendIdentityOptionConfigurations(sb, context.IdentityOptionConfigurations);
            AppendPrecisionConfigurations(sb, context.PrecisionConfigurations);
            AppendColumnFacetConfigurations(sb, context.ColumnFacetConfigurations);
            AppendCheckConstraintConfigurations(sb, context.CheckConstraintConfigurations);
            AppendComputedColumnConfigurations(sb, context.ComputedColumnConfigurations);
            AppendCollationConfigurations(sb, context.CollationConfigurations);
            AppendExpressionIndexConfigurations(sb, context.ExpressionIndexConfigurations);
            AppendRelationshipConfigurations(sb, context.Relationships);
            AppendManyToManyConfigurations(sb, context.ManyToManyJoins);
        }
    }
}
