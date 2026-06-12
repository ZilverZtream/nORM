#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static string FormatRoutineParameterSummary(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.FormatRoutineParameterSummary(metadata);

        private static IReadOnlyList<RoutineStubParameter> GetRoutineInputParameters(IReadOnlyDictionary<string, object?> metadata, bool useNullableReferenceTypes)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameters(metadata, useNullableReferenceTypes);

        private static IReadOnlyList<string> GetRoutineInputParameterDataTypes(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameterDataTypes(metadata);

        private static int GetRoutineInputParameterCount(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineInputParameterCount(metadata);

        private static IReadOnlyList<RoutineOutputParameter> GetRoutineOutputParameters(IReadOnlyDictionary<string, object?> metadata)
            => ScaffoldRoutineMetadataReader.GetRoutineOutputParameters(metadata);

        private static IReadOnlyList<RoutineResultColumn> GetRoutineResultColumns(IReadOnlyDictionary<string, object?> metadata, bool useNullableReferenceTypes, bool useDatabaseNames)
            => ScaffoldRoutineMetadataReader.GetRoutineResultColumns(metadata, useNullableReferenceTypes, useDatabaseNames);

        private static bool TryGetScalarSetReturningRoutineResultColumn(
            IReadOnlyDictionary<string, object?> metadata,
            bool useNullableReferenceTypes,
            out RoutineResultColumn column)
            => ScaffoldRoutineMetadataReader.TryGetScalarSetReturningRoutineResultColumn(metadata, useNullableReferenceTypes, out column);
    }
}
