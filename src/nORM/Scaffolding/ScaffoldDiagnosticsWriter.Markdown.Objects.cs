#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
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
    }
}
