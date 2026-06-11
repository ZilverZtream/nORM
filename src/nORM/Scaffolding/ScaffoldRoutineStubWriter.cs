#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal readonly record struct ScaffoldRoutineStubInfo(
        string? Schema,
        string Name,
        string Kind,
        string Detail,
        string? Comment,
        IReadOnlyDictionary<string, object?> Metadata);

    internal static class ScaffoldRoutineStubWriter
    {
        public static void AppendRoutineStubs(StringBuilder sb, IReadOnlyList<ScaffoldRoutineStubInfo> routineStubs, HashSet<string> memberNames, bool useNullableReferenceTypes, bool useDatabaseNames)
        {
            var nullableReferenceSuffix = useNullableReferenceTypes ? "?" : string.Empty;
            var nullableObjectType = "object" + nullableReferenceSuffix;
            foreach (var routine in routineStubs
                .OrderBy(r => r.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(r => r.Name, StringComparer.Ordinal)
                .ThenBy(r => r.Detail, StringComparer.Ordinal))
            {
                AppendRoutineStub(sb, routine, memberNames, useNullableReferenceTypes, useDatabaseNames, nullableReferenceSuffix, nullableObjectType);
            }

            AppendRoutineParameterGuard(sb, nullableObjectType);
        }

        private static void AppendRoutineStub(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            HashSet<string> memberNames,
            bool useNullableReferenceTypes,
            bool useDatabaseNames,
            string nullableReferenceSuffix,
            string nullableObjectType)
        {
            var metadata = routine.Metadata;
            var routineType = Convert.ToString(metadata.TryGetValue("routineType", out var type) ? type : null) ?? "routine";
            var callShape = Convert.ToString(metadata.TryGetValue("callShape", out var shape) ? shape : null);
            var outputParameterCount = metadata.TryGetValue("outputParameterCount", out var outputCountValue) && outputCountValue is int outputCount
                ? outputCount
                : 0;
            var inputParameters = GetRoutineInputParameters(metadata, useNullableReferenceTypes);
            var inputParameterDataTypes = GetRoutineInputParameterDataTypes(metadata);
            var outputParameters = GetRoutineOutputParameters(metadata);
            var discoveredInputParameterCount = GetRoutineInputParameterCount(metadata);
            var routineMemberName = ScaffoldNameHelper.ToScaffoldClrNamePart(routine.Name, useDatabaseNames);
            var methodBase = ScaffoldNameHelper.MakeUnique(routineMemberName + "Async", memberNames);
            var parameterType = inputParameters.Count > 0
                ? ScaffoldNameHelper.MakeUnique(routineMemberName + "Parameters", memberNames)
                : null;
            var scalarSetResultColumn = TryGetScalarSetReturningRoutineResultColumn(metadata, useNullableReferenceTypes, out var scalarSetColumn)
                ? scalarSetColumn
                : (RoutineResultColumn?)null;
            var resultColumns = scalarSetResultColumn.HasValue
                ? new[] { scalarSetColumn }
                : GetRoutineResultColumns(metadata, useNullableReferenceTypes, useDatabaseNames);
            var resultType = resultColumns.Count > 0
                ? ScaffoldNameHelper.MakeUnique(routineMemberName + "Result", memberNames)
                : null;
            var isFunctionCallShape = IsFunctionCallShape(callShape);
            var hasKnownNoResultSet = !isFunctionCallShape
                && metadata.ContainsKey("resultColumns")
                && resultColumns.Count == 0;
            var outputFactory = outputParameters.Count > 0
                ? ScaffoldNameHelper.MakeUnique("Create" + routineMemberName + "OutputParameters", memberNames)
                : null;
            var routineNameExpression = FormatProviderEscapedRoutineName(routine);
            var parameterSummary = FormatRoutineParameterSummary(metadata);
            var isScalarFunction = string.Equals(callShape, "scalar-function", StringComparison.OrdinalIgnoreCase);

            AppendRoutineParameterType(sb, parameterType, inputParameters);
            AppendRoutineResultType(sb, resultType, resultColumns, useNullableReferenceTypes);
            AppendRoutineDocumentation(sb, routine, routineType, parameterSummary);

            var requiresPositionalFunctionArguments = isFunctionCallShape
                && discoveredInputParameterCount > 0
                && inputParameters.Count == 0;
            var requiresDictionaryRoutineArguments = !isFunctionCallShape
                && discoveredInputParameterCount > 0
                && inputParameters.Count == 0;
            var parameterSignature = requiresPositionalFunctionArguments
                ? $"{nullableObjectType}[]{nullableReferenceSuffix} arguments = null"
                : requiresDictionaryRoutineArguments ? $"IReadOnlyDictionary<string, {nullableObjectType}>{nullableReferenceSuffix} parameters = null"
                : parameterType == null ? $"{nullableObjectType} parameters = null" : $"{parameterType}{nullableReferenceSuffix} parameters = null";

            if (isFunctionCallShape)
            {
                AppendFunctionRoutineMembers(
                    sb,
                    memberNames,
                    routineMemberName,
                    methodBase,
                    routine,
                    parameterSignature,
                    parameterType,
                    inputParameters,
                    resultType,
                    isScalarFunction,
                    requiresPositionalFunctionArguments,
                    discoveredInputParameterCount,
                    inputParameterDataTypes,
                    scalarSetResultColumn.HasValue,
                    useNullableReferenceTypes);
            }
            else
            {
                ScaffoldStoredProcedureRoutineStubWriter.AppendStoredProcedureRoutineMembers(
                    sb,
                    memberNames,
                    routineMemberName,
                    methodBase,
                    routine,
                    routineType,
                    parameterSignature,
                    resultType,
                    hasKnownNoResultSet,
                    routineNameExpression,
                    discoveredInputParameterCount);
            }

            if (outputParameterCount > 0 && !isFunctionCallShape && outputFactory != null)
            {
                ScaffoldStoredProcedureRoutineStubWriter.AppendRoutineOutputMembers(
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

        private static void AppendRoutineParameterType(StringBuilder sb, string? parameterType, IReadOnlyList<RoutineStubParameter> inputParameters)
        {
            if (parameterType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    public sealed class {parameterType}");
            sb.AppendLine("    {");
            foreach (var parameter in inputParameters)
                sb.AppendLine($"        public {parameter.TypeName} {parameter.Name} {{ get; init; }}");
            sb.AppendLine("    }");
        }

        private static void AppendRoutineResultType(StringBuilder sb, string? resultType, IReadOnlyList<RoutineResultColumn> resultColumns, bool useNullableReferenceTypes)
        {
            if (resultType == null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    public sealed class {resultType}");
            sb.AppendLine("    {");
            foreach (var column in resultColumns)
            {
                var initializer = RequiresDefaultInitializer(column.TypeName, useNullableReferenceTypes) ? " = default!;" : string.Empty;
                sb.AppendLine($"        public {column.TypeName} {column.Name} {{ get; set; }}{initializer}");
            }
            sb.AppendLine("    }");
        }

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

        private static void AppendFunctionRoutineMembers(
            StringBuilder sb,
            HashSet<string> memberNames,
            string routineMemberName,
            string methodBase,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            string? resultType,
            bool isScalarFunction,
            bool requiresPositionalFunctionArguments,
            int discoveredInputParameterCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            var streamMethod = isScalarFunction
                ? null
                : ScaffoldNameHelper.MakeUnique("Stream" + routineMemberName + "Async", memberNames);
            var scalarValueMethod = isScalarFunction
                ? ScaffoldNameHelper.MakeUnique(routineMemberName + "ValueAsync", memberNames)
                : null;
            var scalarValueType = isScalarFunction
                ? ScaffoldNameHelper.MakeUnique(routineMemberName + "ValueResult", memberNames)
                : null;

            ScaffoldFunctionRoutineStubWriter.AppendFunctionRoutineStub(
                sb,
                methodBase,
                streamMethod,
                scalarValueMethod,
                scalarValueType,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                resultType,
                scalar: isScalarFunction,
                usePositionalArguments: requiresPositionalFunctionArguments,
                expectedArgumentCount: discoveredInputParameterCount,
                inputParameterDataTypes: inputParameterDataTypes,
                scalarSetReturnsValue: scalarSetReturnsValue,
                useNullableReferenceTypes: useNullableReferenceTypes);
        }

        private static void AppendRoutineParameterGuard(StringBuilder sb, string nullableObjectType)
        {
            sb.AppendLine();
            sb.AppendLine($"    private static {nullableObjectType} RequireScaffoldedRoutineParameters({nullableObjectType} parameters, int expectedInputCount, string routineName)");
            sb.AppendLine("    {");
            sb.AppendLine("        if (expectedInputCount <= 0)");
            sb.AppendLine("            return parameters;");
            sb.AppendLine();
            sb.AppendLine("        if (parameters is null)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass a parameter object containing the scaffolded inputs.\");");
            sb.AppendLine();
            sb.AppendLine($"        if (parameters is IReadOnlyDictionary<string, {nullableObjectType}> dictionary && dictionary.Count != expectedInputCount)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass exactly {expectedInputCount} dictionary entries using the provider parameter names.\");");
            sb.AppendLine();
            sb.AppendLine("        return parameters;");
            sb.AppendLine("    }");
        }

        private static string FormatRoutineParameterSummary(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.FormatRoutineParameterSummary(metadata);

        private static IReadOnlyList<RoutineStubParameter> GetRoutineInputParameters(IReadOnlyDictionary<string, object?> metadata, bool useNullableReferenceTypes)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameters(metadata, useNullableReferenceTypes);

        private static IReadOnlyList<string> GetRoutineInputParameterDataTypes(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameterDataTypes(metadata);

        private static int GetRoutineInputParameterCount(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameterCount(metadata);

        private static IReadOnlyList<RoutineOutputParameter> GetRoutineOutputParameters(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineOutputParameters(metadata);

        private static IReadOnlyList<RoutineResultColumn> GetRoutineResultColumns(IReadOnlyDictionary<string, object?> metadata, bool useNullableReferenceTypes, bool useDatabaseNames)
            => ScaffoldRoutineMetadataReader.GetRoutineResultColumns(metadata, useNullableReferenceTypes, useDatabaseNames);

        private static bool TryGetScalarSetReturningRoutineResultColumn(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes,
            out RoutineResultColumn column)
            => ScaffoldRoutineMetadataReader.TryGetScalarSetReturningRoutineResultColumn(metadata, useNullableReferenceTypes, out column);

        private static bool RequiresDefaultInitializer(string typeName, bool useNullableReferenceTypes = true)
            => useNullableReferenceTypes
               && !typeName.EndsWith("?", StringComparison.Ordinal)
               && (typeName == "string" || typeName.EndsWith("[]", StringComparison.Ordinal));
    }
}
