#nullable enable
using System;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static void AppendRoutineDocumentation(StringBuilder sb, ScaffoldRoutineStubInfo routine, string routineType, string parameterSummary)
        {
            sb.AppendLine();
            var routineExecutionSummary = $"Executes provider-bound {routineType} `{QualifiedRoutineName(routine)}`.";
            if (!string.IsNullOrWhiteSpace(routine.Comment))
            {
                AppendXmlSummary(sb, "    ", routine.Comment!);
                var parameterRemark = string.IsNullOrWhiteSpace(parameterSummary)
                    ? string.Empty
                    : " Parameters discovered at scaffold time: " + parameterSummary + ".";
                sb.AppendLine($"    /// <remarks>{EscapeXmlDocumentation(routineExecutionSummary + parameterRemark + " Routine bodies are provider-owned and are not translated by nORM.")}</remarks>");
                return;
            }

            sb.AppendLine($"    /// <summary>{EscapeXmlDocumentation(routineExecutionSummary)}</summary>");
            if (!string.IsNullOrWhiteSpace(parameterSummary))
                sb.AppendLine($"    /// <remarks>Parameters discovered at scaffold time: {EscapeXmlDocumentation(parameterSummary)}. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
            else
                sb.AppendLine("    /// <remarks>Routine bodies are provider-owned and are not translated by nORM.</remarks>");
        }
    }
}
