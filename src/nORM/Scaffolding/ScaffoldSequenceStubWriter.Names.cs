#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSequenceStubWriter
    {
        private static HashSet<string> FindDuplicateNames(IReadOnlyList<ScaffoldContextSequenceInfo> sequenceStubs)
            => sequenceStubs
                .GroupBy(sequence => sequence.Name, StringComparer.OrdinalIgnoreCase)
                .Where(group => group
                    .Select(sequence => string.IsNullOrWhiteSpace(sequence.Schema) ? string.Empty : sequence.Schema)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count() > 1)
                .Select(group => group.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static string GetSchemaAwareSequenceMemberName(
            ScaffoldContextSequenceInfo sequence,
            IReadOnlySet<string> duplicateSequenceNames,
            bool useDatabaseNames)
        {
            var sourceName = duplicateSequenceNames.Contains(sequence.Name)
                ? string.IsNullOrWhiteSpace(sequence.Schema)
                    ? "Default_" + sequence.Name
                    : sequence.Schema + "_" + sequence.Name
                : sequence.Name;

            return ScaffoldNameHelper.ToScaffoldClrNamePart(sourceName, useDatabaseNames);
        }
    }
}
