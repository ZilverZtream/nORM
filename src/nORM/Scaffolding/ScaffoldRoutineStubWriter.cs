#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            var duplicateRoutineNames = FindDuplicateNames(routineStubs);
            foreach (var routine in routineStubs
                .OrderBy(r => r.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(r => r.Name, StringComparer.Ordinal)
                .ThenBy(r => r.Detail, StringComparer.Ordinal))
            {
                var routineMemberName = GetSchemaAwareRoutineMemberName(routine, duplicateRoutineNames, useDatabaseNames);
                AppendRoutineStub(sb, routine, routineMemberName, memberNames, useNullableReferenceTypes, useDatabaseNames, nullableReferenceSuffix, nullableObjectType);
            }

            AppendRoutineParameterGuard(sb, nullableObjectType);
        }

        private static void AppendRoutineStub(
            StringBuilder sb,
            ScaffoldRoutineStubInfo routine,
            string routineMemberName,
            HashSet<string> memberNames,
            bool useNullableReferenceTypes,
            bool useDatabaseNames,
            string nullableReferenceSuffix,
            string nullableObjectType)
        {
            var plan = BuildRoutineStubPlan(
                routine,
                routineMemberName,
                memberNames,
                useNullableReferenceTypes,
                useDatabaseNames,
                nullableReferenceSuffix,
                nullableObjectType);

            AppendRoutineParameterType(sb, plan.ParameterType, plan.InputParameters);
            AppendRoutineResultType(sb, plan.ResultType, plan.ResultColumns, useNullableReferenceTypes);
            AppendRoutineDocumentation(sb, routine, plan.RoutineType, plan.ParameterSummary);
            AppendRoutineInvocationMembers(sb, memberNames, routine, plan, useNullableReferenceTypes);
            AppendRoutineOutputMembers(sb, memberNames, routine, plan);
        }

        private static void AppendRoutineInvocationMembers(
            StringBuilder sb,
            HashSet<string> memberNames,
            ScaffoldRoutineStubInfo routine,
            RoutineStubPlan plan,
            bool useNullableReferenceTypes)
        {
            if (plan.IsFunctionCallShape)
            {
                AppendFunctionRoutineMembers(
                    sb,
                    memberNames,
                    plan.RoutineMemberName,
                    plan.MethodBase,
                    routine,
                    plan.ParameterSignature,
                    plan.ParameterType,
                    plan.InputParameters,
                    plan.ResultType,
                    plan.IsScalarFunction,
                    plan.RequiresPositionalFunctionArguments,
                    plan.DiscoveredInputParameterCount,
                    plan.InputParameterDataTypes,
                    plan.ScalarSetReturnsValue,
                    useNullableReferenceTypes);
            }
            else
            {
                ScaffoldStoredProcedureRoutineStubWriter.AppendStoredProcedureRoutineMembers(
                    sb,
                    memberNames,
                    plan.RoutineMemberName,
                    plan.MethodBase,
                    routine,
                    plan.RoutineType,
                    plan.ParameterSignature,
                    plan.ResultType,
                    plan.HasKnownNoResultSet,
                    plan.RoutineNameExpression,
                    plan.DiscoveredInputParameterCount);
            }
        }

        private static void AppendRoutineOutputMembers(
            StringBuilder sb,
            HashSet<string> memberNames,
            ScaffoldRoutineStubInfo routine,
            RoutineStubPlan plan)
        {
            if (plan.OutputParameterCount > 0 && !plan.IsFunctionCallShape)
            {
                ScaffoldStoredProcedureRoutineStubWriter.AppendRoutineOutputMembers(
                    sb,
                    memberNames,
                    plan.RoutineMemberName,
                    routine,
                    plan.RoutineType,
                    plan.ParameterSignature,
                    plan.ResultType,
                    plan.HasKnownNoResultSet,
                    plan.RoutineNameExpression,
                    plan.DiscoveredInputParameterCount,
                    plan.OutputFactory,
                    plan.OutputParameters);
            }
        }

        private static HashSet<string> FindDuplicateNames(IReadOnlyList<ScaffoldRoutineStubInfo> routineStubs)
            => routineStubs
                .GroupBy(routine => routine.Name, StringComparer.OrdinalIgnoreCase)
                .Where(group => group
                    .Select(routine => string.IsNullOrWhiteSpace(routine.Schema) ? string.Empty : routine.Schema)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count() > 1)
                .Select(group => group.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static string GetSchemaAwareRoutineMemberName(
            ScaffoldRoutineStubInfo routine,
            IReadOnlySet<string> duplicateRoutineNames,
            bool useDatabaseNames)
        {
            var sourceName = duplicateRoutineNames.Contains(routine.Name)
                ? string.IsNullOrWhiteSpace(routine.Schema)
                    ? "Default_" + routine.Name
                    : routine.Schema + "_" + routine.Name
                : routine.Name;

            return ScaffoldNameHelper.ToScaffoldClrNamePart(sourceName, useDatabaseNames);
        }
    }
}
