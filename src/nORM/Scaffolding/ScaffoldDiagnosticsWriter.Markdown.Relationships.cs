#nullable enable
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static void AppendCompositeForeignKeys(
            StringBuilder sb,
            IReadOnlyList<ScaffoldCompositeForeignKeyDiagnosticInfo> compositeForeignKeys)
        {
            if (compositeForeignKeys.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Composite Foreign Keys");
            sb.AppendLine();
            sb.AppendLine("These composite foreign keys do not target the generated principal primary key or an exact ordered unfiltered unique index, so v1 scaffolding keeps them diagnostic.");
            sb.AppendLine("The generated entity classes keep the scalar columns, and no relationship navigation is emitted for these constraints.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Constraint | Dependent | Columns | Principal | Principal Columns | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var foreignKey in compositeForeignKeys)
            {
                sb.AppendLine(
                    $"| {CodeForCompositeForeignKey()} | {Severity()} | {CategoryForCompositeForeignKey()} | {EscapeMarkdown(foreignKey.ConstraintName)} | {EscapeMarkdown(foreignKey.DependentTable)} | {EscapeMarkdown(string.Join(", ", foreignKey.DependentColumns))} | {EscapeMarkdown(foreignKey.PrincipalTable)} | {EscapeMarkdown(string.Join(", ", foreignKey.PrincipalColumns))} | {EscapeMarkdown(SuggestedActionForCompositeForeignKey())} |");
            }
        }

        private static void AppendPossibleJoinTables(
            StringBuilder sb,
            IReadOnlyList<ScaffoldPossibleJoinTableDiagnosticInfo> possibleJoinTables)
        {
            if (possibleJoinTables.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Possible Many-To-Many Join Tables");
            sb.AppendLine();
            sb.AppendLine("These tables look like join-table candidates but were scaffolded as normal entities because at least one safe `UsingTable` requirement was not met.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Table | Principal Tables | Constraints | Reason Codes | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var table in possibleJoinTables)
            {
                sb.AppendLine(
                    $"| {CodeForPossibleJoinTable()} | {Severity()} | {CategoryForPossibleJoinTable()} | {EscapeMarkdown(table.TableKey)} | {EscapeMarkdown(string.Join(", ", table.PrincipalTables))} | {EscapeMarkdown(string.Join(", ", table.ConstraintNames))} | {EscapeMarkdown(string.Join(", ", table.Reasons))} | {EscapeMarkdown(SuggestedActionForPossibleJoinTable())} |");
            }
        }
    }
}
