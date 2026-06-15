#nullable enable
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextWriter
    {
        private static string FormatManyToManyUsingTableCall(
            ScaffoldManyToManyJoinInfo join,
            string joinTable,
            string? joinSchema,
            string leftKey,
            string rightKey)
        {
            var schemaArgument = joinSchema is null ? string.Empty : $", schema: \"{joinSchema}\"";
            if (RequiresExplicitManyToManyReferentialActions(join))
                return FormatManyToManyUsingTableWithReferentialActions(join, joinTable, schemaArgument, leftKey, rightKey);

            if (joinSchema is null && !join.IsComposite && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\");";

            if (joinSchema is null && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)});";

            if (joinSchema is null && !join.IsComposite)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", {leftKey}, {rightKey});";

            if (joinSchema is null)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey});";

            if (!join.IsComposite && join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", schema: \"{joinSchema}\");";

            if (join.UsesPrimaryKeys)
                return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, schema: \"{joinSchema}\");";

            if (!join.IsComposite)
                return $".UsingTable(\"{joinTable}\", \"{EscapeStringLiteral(join.LeftForeignKeyColumn)}\", \"{EscapeStringLiteral(join.RightForeignKeyColumn)}\", {leftKey}, {rightKey}, schema: \"{joinSchema}\");";

            return $".UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, schema: \"{joinSchema}\");";
        }
    }
}
