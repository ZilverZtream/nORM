#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
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
