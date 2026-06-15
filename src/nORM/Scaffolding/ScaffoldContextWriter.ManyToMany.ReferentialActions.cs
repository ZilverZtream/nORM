#nullable enable

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static string FormatManyToManyUsingTableWithReferentialActions(
            ScaffoldManyToManyJoinInfo join,
            string joinTable,
            string schemaArgument,
            string leftKey,
            string rightKey)
        {
            var leftOnDelete = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.LeftOnDelete);
            var leftOnUpdate = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.LeftOnUpdate);
            var rightOnDelete = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.RightOnDelete);
            var rightOnUpdate = ScaffoldReferentialAction.FormatModelBuilderLiteral(join.RightOnUpdate);
            return join.UsesPrimaryKeys
                ? $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});"
                : $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, {leftOnDelete}, {leftOnUpdate}, {rightOnDelete}, {rightOnUpdate}{schemaArgument});";
        }

        private static bool RequiresExplicitManyToManyReferentialActions(ScaffoldManyToManyJoinInfo join)
            => !IsDefaultReferentialAction(join.LeftOnDelete)
               || !IsDefaultReferentialAction(join.LeftOnUpdate)
               || !IsDefaultReferentialAction(join.RightOnDelete)
               || !IsDefaultReferentialAction(join.RightOnUpdate);

        private static bool IsDefaultReferentialAction(string action)
            => ScaffoldReferentialAction.IsDefault(action);
    }
}
