#nullable enable
using System.Collections.Generic;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFunctionRoutineStubWriter
    {
        private static void AppendFunctionStreamMethods(
            StringBuilder sb,
            string? streamMethod,
            ScaffoldRoutineStubInfo routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            string? resultType,
            bool usePositionalArguments,
            int expectedArgumentCount,
            IReadOnlyList<string> inputParameterDataTypes,
            bool scalarSetReturnsValue,
            bool useNullableReferenceTypes)
        {
            if (streamMethod is null)
                return;

            AppendGenericFunctionStreamMethod(
                sb,
                streamMethod,
                routine,
                parameterSignature,
                parameterType,
                inputParameters,
                usePositionalArguments,
                expectedArgumentCount,
                inputParameterDataTypes,
                scalarSetReturnsValue,
                useNullableReferenceTypes);

            AppendTypedFunctionStreamMethod(
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
