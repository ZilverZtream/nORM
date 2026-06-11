#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
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

        private static void AppendUnsupportedFeatures(
            StringBuilder sb,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures)
        {
            if (unsupportedFeatures.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Provider-Owned Schema Features");
            sb.AppendLine();
            sb.AppendLine("Defaults, ordinary table CHECK constraints, and computed/generated column expressions are emitted as migration metadata when possible. Collations, scaffoldable citext/JSON/XML/UUID scalar storage, parsed SQL Server identity seed/increment settings, and supported FK referential actions are emitted when a generated property or relationship can safely own them. Remaining provider-specific column types, rowversion/timestamp columns, unparsed identity strategies, unrecognized/provider-specific FK referential action tokens, relationships that do not target the generated principal primary key or an exact ordered unfiltered unique index, triggers, provider-native temporal tables, and tables without primary keys are discovered for review, but are not emitted as complete provider-neutral nORM model code. Unmodeled defaults, unparsed identity strategies, unsafe provider-specific column types, triggers, temporal tables, and keyless tables make generated types read-only so write paths fail closed.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Kind | Table | Object | Detail | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
            foreach (var feature in unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal))
            {
                sb.AppendLine(
                    $"| {CodeForUnsupportedFeature(feature.Kind)} | {Severity()} | {CategoryForUnsupportedFeature(feature.Kind)} | {EscapeMarkdown(feature.Kind)} | {EscapeMarkdown(feature.TableKey)} | {EscapeMarkdown(feature.Name)} | {EscapeMarkdown(feature.Detail)} | {EscapeMarkdown(SuggestedActionForUnsupportedFeature(feature.Kind))} |");
            }
        }

        private static void AppendSkippedObjects(
            StringBuilder sb,
            IReadOnlyList<ScaffoldSkippedObjectInfo> skippedObjects)
        {
            if (skippedObjects.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Skipped Database Objects");
            sb.AppendLine();
            sb.AppendLine("Ordinary views/materialized views are emitted as read-only query artifacts by default; SQLite virtual tables and local table/view synonyms can be emitted when explicitly selected by table/schema filters or when query-artifact emission is enabled. Routines can be emitted as opt-in provider-bound stubs, and sequences/non-query synonyms/events remain provider-owned review items.");
            sb.AppendLine();
            sb.AppendLine("| Code | Severity | Category | Kind | Name | Detail | Suggested Action |");
            sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- |");
            foreach (var obj in skippedObjects
                .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                .ThenBy(o => o.Kind, StringComparer.Ordinal))
            {
                sb.AppendLine(
                    $"| {CodeForSkippedObject(obj.Kind)} | {Severity()} | {CategoryForSkippedObject(obj.Kind)} | {EscapeMarkdown(obj.Kind)} | {EscapeMarkdown(TableKey(obj.Schema, obj.Name))} | {EscapeMarkdown(obj.Detail)} | {EscapeMarkdown(SuggestedActionForSkippedObject(obj.Kind))} |");
            }
        }

        private static string EscapeMarkdown(string value)
            => value
                .Replace("\\", "\\\\")
                .Replace("|", "\\|")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;
    }
}
