#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static string[] BuildRoutineMemberNames(
            IReadOnlyList<ScaffoldRoutineStubInfo> routineStubs,
            bool useDatabaseNames)
        {
            var duplicateRoutineNames = FindDuplicateNames(routineStubs);
            var overloadSuffixes = BuildSameSchemaOverloadSuffixes(routineStubs);
            var names = new string[routineStubs.Count];
            for (var i = 0; i < routineStubs.Count; i++)
            {
                overloadSuffixes.TryGetValue(i, out var overloadSuffix);
                names[i] = GetSchemaAwareRoutineMemberName(routineStubs[i], duplicateRoutineNames, useDatabaseNames, overloadSuffix);
            }

            return names;
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

        private static IReadOnlyDictionary<int, string> BuildSameSchemaOverloadSuffixes(IReadOnlyList<ScaffoldRoutineStubInfo> routineStubs)
        {
            var suffixes = new Dictionary<int, string>();
            var indexedRoutines = routineStubs
                .Select((routine, index) => new IndexedRoutine(index, routine))
                .GroupBy(
                    item => (item.Routine.Schema ?? string.Empty) + "\u001f" + item.Routine.Name,
                    StringComparer.OrdinalIgnoreCase);

            foreach (var group in indexedRoutines)
            {
                var routines = group.ToArray();
                if (routines.Length < 2)
                    continue;

                var candidates = routines
                    .Select(item => new RoutineOverloadSuffix(item.Index, TryBuildRoutineSignatureNamePart(item.Routine, out var suffix) ? suffix : null))
                    .ToArray();
                if (candidates.Any(candidate => string.IsNullOrWhiteSpace(candidate.Suffix))
                    || candidates.Select(candidate => candidate.Suffix!).Distinct(StringComparer.OrdinalIgnoreCase).Count() != candidates.Length)
                {
                    continue;
                }

                foreach (var candidate in candidates)
                    suffixes[candidate.Index] = candidate.Suffix!;
            }

            return suffixes;
        }

        private static bool TryBuildRoutineSignatureNamePart(ScaffoldRoutineStubInfo routine, out string suffix)
        {
            suffix = string.Empty;
            var dataTypes = GetRoutineInputParameterDataTypes(routine.Metadata);
            if (dataTypes.Count == 0)
                return false;

            var parts = dataTypes
                .Select(GetRoutineSignatureTypeNamePart)
                .ToArray();
            if (parts.Any(string.IsNullOrWhiteSpace))
                return false;

            suffix = string.Join("_", parts);
            return suffix.Length > 0;
        }

        private static string GetRoutineSignatureTypeNamePart(string dataType)
        {
            var normalized = NormalizeRoutineSignatureTypeName(dataType);
            return string.IsNullOrWhiteSpace(normalized)
                ? string.Empty
                : ScaffoldNameHelper.ToPascalCase(normalized);
        }

        private static string NormalizeRoutineSignatureTypeName(string dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return string.Empty;

            var sb = new StringBuilder(dataType.Length + 8);
            for (var i = 0; i < dataType.Length; i++)
            {
                var ch = dataType[i];
                if (char.IsLetterOrDigit(ch))
                {
                    sb.Append(ch);
                }
                else if (ch == '[' && i + 1 < dataType.Length && dataType[i + 1] == ']')
                {
                    sb.Append(" Array ");
                    i++;
                }
                else
                {
                    sb.Append(' ');
                }
            }

            return sb.ToString();
        }

        private static string GetSchemaAwareRoutineMemberName(
            ScaffoldRoutineStubInfo routine,
            IReadOnlySet<string> duplicateRoutineNames,
            bool useDatabaseNames,
            string? overloadSuffix)
        {
            var sourceName = duplicateRoutineNames.Contains(routine.Name)
                ? string.IsNullOrWhiteSpace(routine.Schema)
                    ? "Default_" + routine.Name
                    : routine.Schema + "_" + routine.Name
                : routine.Name;

            if (!string.IsNullOrWhiteSpace(overloadSuffix))
                sourceName += "_" + overloadSuffix;

            return ScaffoldNameHelper.ToScaffoldClrNamePart(sourceName, useDatabaseNames);
        }

        private readonly record struct IndexedRoutine(int Index, ScaffoldRoutineStubInfo Routine);

        private readonly record struct RoutineOverloadSuffix(int Index, string? Suffix);
    }
}
