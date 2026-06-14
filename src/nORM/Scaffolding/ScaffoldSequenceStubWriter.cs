#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using nORM.Core;
using static nORM.Scaffolding.ScaffoldCodeText;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSequenceStubWriter
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
                var sequenceSummary = $"Gets the next provider-bound value from sequence `{QualifiedRoutineName(sequence)}`.";
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
                sb.AppendLine($"            throw new NormConfigurationException(\"Sequence `{EscapeStringLiteral(QualifiedRoutineName(sequence))}` did not return a value.\");");
                sb.AppendLine("        return rows[0].Value;");
                sb.AppendLine("    }");
            }
        }

        private static string ParseSequenceProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase))
                return "sqlserver";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            return string.Empty;
        }

        private static HashSet<string> FindDuplicateNames(IReadOnlyList<ScaffoldContextSequenceInfo> sequenceStubs)
            => sequenceStubs
                .GroupBy(sequence => sequence.Name, StringComparer.OrdinalIgnoreCase)
                .Where(group => group.Count() > 1)
                .Select(group => group.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static string GetSchemaAwareSequenceMemberName(
            ScaffoldContextSequenceInfo sequence,
            IReadOnlySet<string> duplicateSequenceNames,
            bool useDatabaseNames)
        {
            var sourceName = duplicateSequenceNames.Contains(sequence.Name)
                ? string.IsNullOrWhiteSpace(sequence.Schema)
                    ? "Default_" + sequence.Name
                    : sequence.Schema + "_" + sequence.Name
                : sequence.Name;

            return ScaffoldNameHelper.ToScaffoldClrNamePart(sourceName, useDatabaseNames);
        }

        private static string MapSequenceValueTypeName(string detail)
        {
            var dataType = ParseSemicolonValue(detail, "dataType");
            var normalized = dataType.Split('(', 2)[0].Trim().ToLowerInvariant();
            return normalized switch
            {
                "tinyint" => "byte",
                "smallint" => "short",
                "int" or "integer" => "int",
                "bigint" => "long",
                "decimal" or "numeric" => "decimal",
                _ => "long"
            };
        }

        private static string FormatSequenceSqlExpression(ScaffoldContextSequenceInfo sequence, string provider)
        {
            var escapedValueAlias = " + Provider.Escape(\"Value\")";
            var sequenceName = FormatProviderEscapedRoutineName(sequence);
            if (provider == "sqlserver")
                return "\"SELECT NEXT VALUE FOR \" + " + sequenceName + " + \" AS \"" + escapedValueAlias;

            return "\"SELECT nextval('\" + (" + sequenceName + ").Replace(\"'\", \"''\") + \"'::regclass) AS \"" + escapedValueAlias;
        }

        private static string FormatProviderEscapedRoutineName(ScaffoldContextSequenceInfo routine)
        {
            var name = EscapeStringLiteral(routine.Name);
            if (string.IsNullOrWhiteSpace(routine.Schema))
                return $"Provider.Escape(\"{name}\")";

            var schema = EscapeStringLiteral(routine.Schema!);
            return $"Provider.Escape(\"{schema}\") + \".\" + Provider.Escape(\"{name}\")";
        }

        private static string QualifiedRoutineName(ScaffoldContextSequenceInfo routine)
            => string.IsNullOrWhiteSpace(routine.Schema) ? routine.Name : routine.Schema + "." + routine.Name;

        private static string ParseSemicolonValue(string detail, string key)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            return values.TryGetValue(key, out var value) ? value : string.Empty;
        }
    }
}
