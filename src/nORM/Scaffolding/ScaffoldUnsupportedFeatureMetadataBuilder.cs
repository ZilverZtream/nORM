#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildMetadata(ScaffoldUnsupportedFeatureInfo feature)
        {
            var metadata = feature.Metadata is null
                ? new Dictionary<string, object?>(StringComparer.Ordinal)
                : new Dictionary<string, object?>(feature.Metadata, StringComparer.Ordinal);
            AddFeatureMetadata(metadata, feature);

            return metadata;
        }

        private static void AddFeatureMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            if (TryAddKeyOrRelationshipFeatureMetadata(metadata, feature))
                return;
            if (TryAddScalarFeatureMetadata(metadata, feature))
                return;
            if (TryAddIndexFeatureMetadata(metadata, feature))
                return;

            _ = TryAddProviderObjectFeatureMetadata(metadata, feature);
        }

        public static bool TryParseMetadataBoolean(string value, out bool parsed)
        {
            var trimmed = value.Trim();
            if (trimmed.Equals("true", StringComparison.OrdinalIgnoreCase)
                || trimmed.Equals("1", StringComparison.Ordinal))
            {
                parsed = true;
                return true;
            }

            if (trimmed.Equals("false", StringComparison.OrdinalIgnoreCase)
                || trimmed.Equals("0", StringComparison.Ordinal))
            {
                parsed = false;
                return true;
            }

            parsed = false;
            return false;
        }

        private static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsWriteBlockingProviderSpecificColumnType(detail);
    }
}
