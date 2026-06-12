#nullable enable
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
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
    }
}
