#nullable enable
using System;
using System.Collections.Generic;
using static nORM.Scaffolding.ScaffoldRoutineInvocationFormatter;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static RoutineStubPlan BuildRoutineStubPlan(
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
            var scalarSetReturnsValue = TryGetScalarSetReturningRoutineResultColumn(metadata, useNullableReferenceTypes, out var scalarSetColumn);
            var resultColumns = scalarSetReturnsValue
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
            var isScalarFunction = string.Equals(callShape, "scalar-function", StringComparison.OrdinalIgnoreCase);
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

            return new RoutineStubPlan(
                routineType,
                outputParameterCount,
                inputParameters,
                inputParameterDataTypes,
                outputParameters,
                discoveredInputParameterCount,
                routineMemberName,
                methodBase,
                parameterType,
                scalarSetReturnsValue,
                resultColumns,
                resultType,
                isFunctionCallShape,
                hasKnownNoResultSet,
                outputFactory,
                FormatProviderEscapedRoutineName(routine),
                FormatRoutineParameterSummary(metadata),
                isScalarFunction,
                requiresPositionalFunctionArguments,
                parameterSignature);
        }

        private readonly record struct RoutineStubPlan(
            string RoutineType,
            int OutputParameterCount,
            IReadOnlyList<RoutineStubParameter> InputParameters,
            IReadOnlyList<string> InputParameterDataTypes,
            IReadOnlyList<RoutineOutputParameter> OutputParameters,
            int DiscoveredInputParameterCount,
            string RoutineMemberName,
            string MethodBase,
            string? ParameterType,
            bool ScalarSetReturnsValue,
            IReadOnlyList<RoutineResultColumn> ResultColumns,
            string? ResultType,
            bool IsFunctionCallShape,
            bool HasKnownNoResultSet,
            string? OutputFactory,
            string RoutineNameExpression,
            string ParameterSummary,
            bool IsScalarFunction,
            bool RequiresPositionalFunctionArguments,
            string ParameterSignature);
    }
}
