#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using nORM.Core;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSequenceStubWriter
    {
        public static void AppendSequenceStubs(
            StringBuilder sb,
            IReadOnlyList<ScaffoldContextSequenceInfo> sequenceStubs,
            HashSet<string> memberNames,
            bool useDatabaseNames)
        {
            var duplicateSequenceNames = FindDuplicateNames(sequenceStubs);
            foreach (var sequence in sequenceStubs
                .OrderBy(s => s.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(s => s.Name, StringComparer.Ordinal))
            {
                var provider = ParseSequenceProvider(sequence.Detail);
                if (provider is not ("sqlserver" or "postgres"))
                    continue;

                var valueTypeName = MapSequenceValueTypeName(sequence.Detail);
                var sequenceMemberName = GetSchemaAwareSequenceMemberName(sequence, duplicateSequenceNames, useDatabaseNames);
                var methodBase = ScaffoldNameHelper.MakeUnique("Next" + sequenceMemberName + "ValueAsync", memberNames);
                var resultType = ScaffoldNameHelper.MakeUnique(sequenceMemberName + "SequenceValue", memberNames);

                sb.AppendLine();
                sb.AppendLine($"    private sealed class {resultType}");
                sb.AppendLine("    {");
                sb.AppendLine($"        public {valueTypeName} Value {{ get; set; }}");
                sb.AppendLine("    }");
                sb.AppendLine();
                var sequenceSummary = $"Gets the next provider-bound value from sequence `{QualifiedSequenceName(sequence)}`.";
                if (!string.IsNullOrWhiteSpace(sequence.Comment))
                {
                    AppendXmlSummary(sb, "    ", sequence.Comment!);
                    sb.AppendLine($"    /// <remarks>{EscapeXmlDocumentation(sequenceSummary + " Sequence DDL and allocation semantics remain provider-owned and are not translated by nORM.")}</remarks>");
                }
                else
                {
                    sb.AppendLine($"    /// <summary>{EscapeXmlDocumentation(sequenceSummary)}</summary>");
                    sb.AppendLine("    /// <remarks>Sequence DDL and allocation semantics remain provider-owned and are not translated by nORM.</remarks>");
                }
                sb.AppendLine($"    public async Task<{valueTypeName}> {methodBase}(CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine($"        var rows = await QueryUnchangedAsync<{resultType}>({FormatSequenceSqlExpression(sequence, provider)}, ct).ConfigureAwait(false);");
                sb.AppendLine("        if (rows.Count == 0)");
                sb.AppendLine($"            throw new NormConfigurationException(\"Sequence `{EscapeStringLiteral(QualifiedSequenceName(sequence))}` did not return a value.\");");
                sb.AppendLine("        return rows[0].Value;");
                sb.AppendLine("    }");
            }
        }
    }
}
