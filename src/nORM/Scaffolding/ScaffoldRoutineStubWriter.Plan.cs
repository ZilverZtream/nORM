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
            string routineMemberName,
            HashSet<string> memberNames,
            bool useNullableReferenceTypes,
            bool useDatabaseNames,
            string nullableReferenceSuffix,
            string nullableObjectType)
        {
            var metadata = routine.Metadata;
            var routineType = GetMetadataString(metadata, "routineType") ?? "routine";
            var callShape = GetMetadataString(metadata, "callShape");
            var outputParameterCount = GetMetadataInt(metadata, "outputParameterCount");
            var inputParameters = GetRoutineInputParameters(metadata, useNullableReferenceTypes);
            var inputParameterDataTypes = GetRoutineInputParameterDataTypes(metadata);
            var outputParameters = GetRoutineOutputParameters(metadata);
            var discoveredInputParameterCount = GetRoutineInputParameterCount(metadata);
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
            var parameterSignature = BuildRoutineParameterSignature(
                requiresPositionalFunctionArguments,
                requiresDictionaryRoutineArguments,
                parameterType,
                nullableObjectType,
                nullableReferenceSuffix);

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
    }
}
