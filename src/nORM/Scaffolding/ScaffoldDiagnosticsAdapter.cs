#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Database scaffolding emits dynamic entity types and traverses live mapping metadata; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Database scaffolding reflects over provider and entity metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal static partial class ScaffoldDiagnosticsAdapter
    {
        public static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(
            ScaffoldUnsupportedFeature feature)
            => ScaffoldUnsupportedFeatureMetadataBuilder.BuildMetadata(
                new ScaffoldUnsupportedFeatureInfo(feature.TableKey, feature.Kind, feature.Name, feature.Detail)
                {
                    Metadata = feature.Metadata
                });

        public static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(value, out parsed);

        public static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> ConvertUnsupportedFeatureInfos(
            IReadOnlyList<ScaffoldUnsupportedFeature> features)
        {
            var converted = new ScaffoldUnsupportedFeatureInfo[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                converted[i] = new ScaffoldUnsupportedFeatureInfo(
                    feature.TableKey,
                    feature.Kind,
                    feature.Name,
                    feature.Detail)
                {
                    Metadata = BuildUnsupportedFeatureMetadata(feature)
                };
            }

            return converted;
        }
    }
}
