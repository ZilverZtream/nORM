#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRoutineStubWriter
    {
        private static string? GetMetadataString(IReadOnlyDictionary<string, object?> metadata, string name)
            => Convert.ToString(metadata.TryGetValue(name, out var value) ? value : null);

        private static int GetMetadataInt(IReadOnlyDictionary<string, object?> metadata, string name)
            => metadata.TryGetValue(name, out var value) && value is int intValue
                ? intValue
                : 0;
    }
}
