#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        private static bool TryAddIndexFeatureMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            switch (feature.Kind)
            {
                case "PartialIndex":
                    metadata["filtered"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                case "ExpressionIndex":
                    metadata["expressionBased"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    ScaffoldIndexFeatureMetadataBuilder.AddExpressionIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                case "IncludedColumnIndex":
                    metadata["includedColumns"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                case "DescendingIndex":
                    metadata["descending"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                case "PrefixIndex":
                    metadata["prefixIndex"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                case "ProviderSpecificIndex":
                    metadata["providerSpecific"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    return true;
                default:
                    return false;
            }
        }
    }
}
