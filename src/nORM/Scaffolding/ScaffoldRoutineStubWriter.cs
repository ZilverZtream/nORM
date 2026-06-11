#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

    internal static partial class ScaffoldRoutineStubWriter
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

    }
}
