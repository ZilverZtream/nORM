#nullable enable
using System;
using System.Collections.Generic;
using System.Text;
using static nORM.Scaffolding.ScaffoldCodeText;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static class ScaffoldFunctionRoutineStubWriter
    {
        public static void AppendFunctionRoutineStub(
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
    }
}
