#nullable enable
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
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
            AppendGenericFunctionQueryMethod(
                sb,
                methodBase,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                scalar,
                usePositionalArguments,
                expectedArgumentCount,
                inputParameterDataTypes,
                scalarSetReturnsValue,
                useNullableReferenceTypes);

            AppendTypedFunctionQueryMethod(
                sb,
                methodBase,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                resultType,
                scalar,
                usePositionalArguments,
                expectedArgumentCount,
                inputParameterDataTypes,
                scalarSetReturnsValue,
                useNullableReferenceTypes);

            AppendScalarFunctionValueMethod(
                sb,
                scalarValueMethod,
                scalarValueType,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                scalar,
                usePositionalArguments,
                expectedArgumentCount,
                inputParameterDataTypes,
                useNullableReferenceTypes);

            AppendFunctionStreamMethods(
                sb,
                streamMethod,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                resultType,
                usePositionalArguments,
                expectedArgumentCount,
                inputParameterDataTypes,
                scalarSetReturnsValue,
                useNullableReferenceTypes);
        }
    }
}
