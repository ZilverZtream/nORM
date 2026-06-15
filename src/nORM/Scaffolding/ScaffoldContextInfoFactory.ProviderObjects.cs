#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldContextInfoFactory
    {
        private static ScaffoldRoutineStubInfo[] ConvertRoutineStubInfos(
            IReadOnlyList<ScaffoldSkippedObject> routineStubs)
        {
            var converted = new ScaffoldRoutineStubInfo[routineStubs.Count];
            for (var i = 0; i < routineStubs.Count; i++)
            {
                var routine = routineStubs[i];
                converted[i] = new ScaffoldRoutineStubInfo(
                    routine.Schema,
                    routine.Name,
                    routine.Kind,
                    routine.Detail,
                    routine.Comment,
                    BuildSkippedObjectMetadata(routine));
            }

            return converted;
        }

        private static ScaffoldContextSequenceInfo[] ConvertContextSequenceInfos(
            IReadOnlyList<ScaffoldSkippedObject> sequenceStubs)
            => sequenceStubs
                .Select(sequence => new ScaffoldContextSequenceInfo(sequence.Schema, sequence.Name, sequence.Detail, sequence.Comment))
                .ToArray();

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(
            ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));
    }
}
