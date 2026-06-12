#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static class ScaffoldStoredProcedureOutputRoutineStubWriter
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

        private static void AppendScaffoldedOutputParameterOverloads(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            string routineType,
            string parameterSignature,
            string? resultType,
            bool hasKnownNoResultSet,
            string routineNameExpression,
            string storedProcedureParameters,
            string outputMethod,
            string outputFactory,
            IReadOnlyList<RoutineOutputParameter> outputParameters)
        {
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with output parameters discovered at scaffold time.</summary>");
            sb.AppendLine("    /// <remarks>Use this overload when the scaffolded output parameter metadata still matches the database routine. Pass explicit output parameters to the overload with <c>params OutputParameter[]</c> after routine signature changes.</remarks>");
            if (hasKnownNoResultSet)
            {
                sb.AppendLine($"    public Task<StoredProcedureNonQueryResult> {outputMethod}({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine($"        => ExecuteStoredProcedureNonQueryWithOutputAsync({routineNameExpression}, ct, {storedProcedureParameters}, {outputFactory}());");
            }
            else
            {
                sb.AppendLine($"    public Task<StoredProcedureResult<TResult>> {outputMethod}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
                sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters}, {outputFactory}());");
                if (resultType != null)
                {
                    sb.AppendLine();
                    sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with scaffold-discovered output parameters and result shape.</summary>");
                    sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                    sb.AppendLine($"    public Task<StoredProcedureResult<{resultType}>> {outputMethod}({parameterSignature}, CancellationToken ct = default)");
                    sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<{resultType}>({routineNameExpression}, ct, {storedProcedureParameters}, {outputFactory}());");
                }
            }

            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Creates output parameter definitions discovered for `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` at scaffold time.</summary>");
            sb.AppendLine($"    public static OutputParameter[] {outputFactory}()");
            sb.AppendLine("        => new[]");
            sb.AppendLine("        {");
            foreach (var parameter in outputParameters)
                sb.AppendLine($"            {FormatRoutineOutputParameterCreation(parameter)},");
            sb.AppendLine("        };");
        }

        private static string FormatRoutineOutputParameterCreation(RoutineOutputParameter parameter)
        {
            var baseCall = $"new OutputParameter(\"{EscapeStringLiteral(parameter.Name)}\", System.Data.DbType.{parameter.DbType}";
            if (parameter.Precision.HasValue && parameter.Scale.HasValue)
            {
                baseCall += $", (byte){parameter.Precision.Value.ToString(CultureInfo.InvariantCulture)}, (byte){parameter.Scale.Value.ToString(CultureInfo.InvariantCulture)}";
                var hasNonDefaultDecimalDirection = !string.Equals(parameter.Direction, nameof(ParameterDirection.Output), StringComparison.Ordinal);
                return hasNonDefaultDecimalDirection
                    ? baseCall + ", System.Data.ParameterDirection." + parameter.Direction + ")"
                    : baseCall + ")";
            }

            if (parameter.Precision.HasValue)
            {
                return baseCall
                    + $", null, (byte){parameter.Precision.Value.ToString(CultureInfo.InvariantCulture)}, null, System.Data.ParameterDirection.{parameter.Direction}, null)";
            }

            var hasNonDefaultDirection = !string.Equals(parameter.Direction, nameof(ParameterDirection.Output), StringComparison.Ordinal);
            if (!parameter.Size.HasValue && !hasNonDefaultDirection)
                return baseCall + ")";

            var sizeArgument = parameter.Size.HasValue
                ? parameter.Size.Value.ToString(CultureInfo.InvariantCulture)
                : "null";
            if (!hasNonDefaultDirection)
                return baseCall + ", " + sizeArgument + ")";

            return baseCall + ", " + sizeArgument + ", System.Data.ParameterDirection." + parameter.Direction + ")";
        }
    }
}
