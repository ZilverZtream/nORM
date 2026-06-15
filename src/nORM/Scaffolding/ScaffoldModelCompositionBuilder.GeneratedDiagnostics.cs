#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelCompositionBuilder
    {
        public static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
            IReadOnlySet<string> manyToManyJoinTableKeys)
        {
            foreach (var feature in generatedModelFeatureDiagnostics)
            {
                if (!ShouldRestoreGeneratedManyToManyUnsupportedFeature(feature.Kind)
                    || !manyToManyJoinTableKeys.Contains(feature.TableKey)
                    || unsupportedFeatures.Contains(feature))
                {
                    continue;
                }

                unsupportedFeatures.Add(feature);
            }
        }

        public static bool ShouldRestoreGeneratedManyToManyUnsupportedFeature(string kind)
            => !string.Equals(kind, "Computed", StringComparison.OrdinalIgnoreCase);
    }
}
