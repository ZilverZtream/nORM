#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoredProcedureOutputRoutineStubWriter
    {
        public static void AppendRoutineOutputMembers(
            StringBuilder sb,
            HashSet<string> memberNames,
            string routineMemberName,
            ScaffoldRoutineStubInfo routine,
            string routineType,
            string parameterSignature,
            string? resultType,
            bool hasKnownNoResultSet,
            string routineNameExpression,
            int discoveredInputParameterCount,
            string outputFactory,
            IReadOnlyList<RoutineOutputParameter> outputParameters)
        {
            var outputMethod = ScaffoldNameHelper.MakeUnique(routineMemberName + "WithOutputAsync", memberNames);
            var storedProcedureParameters = FormatStoredProcedureParameterArgument(routine, discoveredInputParameterCount);
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with output parameters.</summary>");
            if (hasKnownNoResultSet)
            {
                sb.AppendLine("    /// <remarks>Pass explicit <see cref=\"OutputParameter\"/> definitions for provider output values. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                sb.AppendLine($"    public Task<StoredProcedureNonQueryResult> {outputMethod}({parameterSignature}, CancellationToken ct = default, params OutputParameter[] outputParameters)");
                sb.AppendLine($"        => ExecuteStoredProcedureNonQueryWithOutputAsync({routineNameExpression}, ct, {storedProcedureParameters}, outputParameters);");
            }
            else
            {
                sb.AppendLine("    /// <remarks>Pass explicit <see cref=\"OutputParameter\"/> definitions for provider output values. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                sb.AppendLine($"    public Task<StoredProcedureResult<TResult>> {outputMethod}<TResult>({parameterSignature}, CancellationToken ct = default, params OutputParameter[] outputParameters) where TResult : class, new()");
                sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters}, outputParameters);");
                if (resultType != null)
                {
                    sb.AppendLine();
                    sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with output parameters and the scaffold-discovered result shape.</summary>");
                    sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                    sb.AppendLine($"    public Task<StoredProcedureResult<{resultType}>> {outputMethod}({parameterSignature}, CancellationToken ct = default, params OutputParameter[] outputParameters)");
                    sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<{resultType}>({routineNameExpression}, ct, {storedProcedureParameters}, outputParameters);");
                }
            }

            AppendScaffoldedOutputParameterOverloads(sb, routine, routineType, parameterSignature, resultType, hasKnownNoResultSet, routineNameExpression, storedProcedureParameters, outputMethod, outputFactory, outputParameters);
        }
    }
}
