#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSkippedObjectMetadataBuilder
    {
        private static IReadOnlyDictionary<string, object?> BuildEventMetadata(ScaffoldSkippedObjectInfo obj)
        {
            if (!obj.Detail.StartsWith("MySQL event", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var values = ScaffoldSemicolonParser.Parse(obj.Detail, out _);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = "MySQL"
            };

            AddMetadataValue(metadata, values, "eventType");
            AddMetadataValue(metadata, values, "status");
            AddMetadataValue(metadata, values, "intervalValue");
            AddMetadataValue(metadata, values, "intervalField");
            AddMetadataValue(metadata, values, "executeAt");
            AddMetadataValue(metadata, values, "starts");
            AddMetadataValue(metadata, values, "ends");
            return metadata;
        }
    }
}
