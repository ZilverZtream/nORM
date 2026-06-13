#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static class ScaffoldStoredProcedureRoutineStubWriter
    {
        public static void AppendStoredProcedureRoutineMembers(
            StringBuilder sb,
            HashSet<string> memberNames,
            string routineMemberName,
            string methodBase,
            ScaffoldRoutineStubInfo routine,
            string routineType,
            string parameterSignature,
            string? resultType,
            bool hasKnownNoResultSet,
            string routineNameExpression,
            int discoveredInputParameterCount)
        {
            var storedProcedureParameters = FormatStoredProcedureParameterArgument(routine, discoveredInputParameterCount);
            if (hasKnownNoResultSet)
            {
                sb.AppendLine($"    public Task<int> {methodBase}({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine($"        => ExecuteStoredProcedureNonQueryAsync({routineNameExpression}, ct, {storedProcedureParameters});");
                return;
            }

            sb.AppendLine($"    public Task<List<TResult>> {methodBase}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine($"        => ExecuteStoredProcedureAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters});");
            if (resultType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and materializes the scaffold-discovered result shape.</summary>");
                sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                sb.AppendLine($"    public Task<List<{resultType}>> {methodBase}({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine($"        => ExecuteStoredProcedureAsync<{resultType}>({routineNameExpression}, ct, {storedProcedureParameters});");
            }

            var streamMethod = ScaffoldNameHelper.MakeUnique("Stream" + routineMemberName + "Async", memberNames);
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Streams provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows without buffering the full result set.</summary>");
            sb.AppendLine("    /// <remarks>Use the buffered wrapper when output parameters are required. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
            sb.AppendLine($"    public IAsyncEnumerable<TResult> {streamMethod}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine($"        => ExecuteStoredProcedureAsAsyncEnumerable<TResult>({routineNameExpression}, ct, {storedProcedureParameters});");
            if (resultType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Streams provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows using the scaffold-discovered result shape.</summary>");
                sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                sb.AppendLine($"    public IAsyncEnumerable<{resultType}> {streamMethod}({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine($"        => ExecuteStoredProcedureAsAsyncEnumerable<{resultType}>({routineNameExpression}, ct, {storedProcedureParameters});");
            }
        }

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
            string? outputFactory,
            IReadOnlyList<RoutineOutputParameter> outputParameters)
            => ScaffoldStoredProcedureOutputRoutineStubWriter.AppendRoutineOutputMembers(
                sb,
                memberNames,
                routineMemberName,
                routine,
                routineType,
                parameterSignature,
                resultType,
                hasKnownNoResultSet,
                routineNameExpression,
                discoveredInputParameterCount,
                outputFactory,
                outputParameters);
    }
}
