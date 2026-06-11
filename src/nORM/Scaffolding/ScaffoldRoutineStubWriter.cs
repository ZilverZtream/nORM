#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
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
                AppendStoredProcedureRoutineMembers(
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
                AppendRoutineOutputMembers(
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

            AppendFunctionRoutineStub(
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

        private static void AppendStoredProcedureRoutineMembers(
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

        private static void AppendRoutineOutputMembers(
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
        private static void AppendFunctionRoutineStub(
            StringBuilder sb,
            string methodBase,
            string? streamMethod,
            string? scalarValueMethod,
            string? scalarValueType,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            string? resultType,
            bool scalar,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue = false,
            bool useNullableReferenceTypes = true)
        {
            sb.AppendLine($"    public Task<List<TResult>> {methodBase}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalar || scalarSetReturnsValue)
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("    }");

            if (!scalar && resultType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Executes provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and materializes the scaffold-discovered result shape.</summary>");
                sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                sb.AppendLine($"    public Task<List<{resultType}>> {methodBase}({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
                AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
                AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
                sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
                if (scalarSetReturnsValue)
                    sb.AppendLine($"        return QueryUnchangedAsync<{resultType}>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
                else
                    sb.AppendLine($"        return QueryUnchangedAsync<{resultType}>(\"SELECT * FROM \" + invocation, ct, args);");
                sb.AppendLine("    }");
            }

            if (scalar && scalarValueType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    private sealed class {scalarValueType}<TValue>");
                sb.AppendLine("    {");
                sb.AppendLine(useNullableReferenceTypes
                    ? "        public TValue? Value { get; set; }"
                    : "        public TValue Value { get; set; }");
                sb.AppendLine("    }");
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Executes provider-bound scalar function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and returns its single value.</summary>");
                sb.AppendLine("    /// <remarks>The routine body is provider-owned and is not translated by nORM.</remarks>");
                sb.AppendLine($"    public async Task<TValue{(useNullableReferenceTypes ? "?" : string.Empty)}> {scalarValueMethod}<TValue>({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
                AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
                AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
                sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
                sb.AppendLine($"        var rows = await QueryUnchangedAsync<{scalarValueType}<TValue>>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args).ConfigureAwait(false);");
                sb.AppendLine("        return rows.Count == 0 ? default : rows[0].Value;");
                sb.AppendLine("    }");
            }

            if (streamMethod is null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Streams provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows without buffering the full result set.</summary>");
            sb.AppendLine("    /// <remarks>Routine bodies are provider-owned and are not translated by nORM.</remarks>");
            sb.AppendLine($"    public async IAsyncEnumerable<TResult> {streamMethod}<TResult>({parameterSignature}, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalarSetReturnsValue)
                sb.AppendLine("        var rows = QueryUnchangedStreamAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine("        var rows = QueryUnchangedStreamAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("        await foreach (var row in rows.ConfigureAwait(false))");
            sb.AppendLine("            yield return row;");
            sb.AppendLine("    }");

            if (resultType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Streams provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows using the scaffold-discovered result shape.</summary>");
                sb.AppendLine("    /// <remarks>Use the generic overload after routine result shape changes.</remarks>");
                sb.AppendLine($"    public async IAsyncEnumerable<{resultType}> {streamMethod}({parameterSignature}, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments, useNullableReferenceTypes));
                AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
                AppendFunctionPlaceholderLine(sb, routine, inputParameterDataTypes, expectedArgumentCount);
                sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
                if (scalarSetReturnsValue)
                    sb.AppendLine($"        var rows = QueryUnchangedStreamAsync<{resultType}>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
                else
                    sb.AppendLine($"        var rows = QueryUnchangedStreamAsync<{resultType}>(\"SELECT * FROM \" + invocation, ct, args);");
                sb.AppendLine("        await foreach (var row in rows.ConfigureAwait(false))");
                sb.AppendLine("            yield return row;");
                sb.AppendLine("    }");
            }
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

        private static bool RequiresDefaultInitializer(string typeName, bool useNullableReferenceTypes = true)
            => useNullableReferenceTypes
               && !typeName.EndsWith("?", StringComparison.Ordinal)
               && (typeName == "string" || typeName.EndsWith("[]", StringComparison.Ordinal));
    }
}
