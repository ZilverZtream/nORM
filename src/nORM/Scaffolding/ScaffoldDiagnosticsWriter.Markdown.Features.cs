#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldDiagnosticsWriter
    {
        private static void AppendUnsupportedFeatures(
            StringBuilder sb,
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> unsupportedFeatures)
        {
            if (unsupportedFeatures.Count == 0)
                return;

            sb.AppendLine();
            sb.AppendLine("## Provider-Owned Schema Features");
            sb.AppendLine();
            sb.AppendLine("Defaults, ordinary table CHECK constraints, computed/generated column expressions, and rowversion/timestamp concurrency columns are emitted as model or migration metadata when possible. Collations, scaffoldable citext/JSON/XML/UUID scalar storage, parsed SQL Server identity seed/increment settings, and supported FK referential actions are emitted when a generated property or relationship can safely own them. Remaining provider-specific column types, provider-managed rowversion/timestamp DDL, unparsed identity strategies, unrecognized/provider-specific FK referential action tokens, relationships that do not target the generated principal primary key or an exact ordered unfiltered unique index, triggers, provider-native temporal tables, and tables without primary keys are discovered for review, but are not emitted as complete provider-neutral nORM model code. Unmodeled defaults, unparsed identity strategies, unsafe provider-specific column types, triggers, temporal tables, and keyless tables make generated types read-only so write paths fail closed.");
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
    }
}
