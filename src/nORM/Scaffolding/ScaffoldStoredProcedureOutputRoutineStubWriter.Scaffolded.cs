#nullable enable
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldStoredProcedureOutputRoutineStubWriter
    {
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

            AppendOutputParameterFactory(sb, routine, outputFactory, outputParameters);
        }

        private static void AppendOutputParameterFactory(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            string outputFactory,
            IReadOnlyList<RoutineOutputParameter> outputParameters)
        {
            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Creates output parameter definitions discovered for `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` at scaffold time.</summary>");
            sb.AppendLine($"    public static OutputParameter[] {outputFactory}()");
            sb.AppendLine("        => new[]");
            sb.AppendLine("        {");
            foreach (var parameter in outputParameters)
                sb.AppendLine($"            {FormatRoutineOutputParameterCreation(parameter)},");
            sb.AppendLine("        };");
        }
    }
}
