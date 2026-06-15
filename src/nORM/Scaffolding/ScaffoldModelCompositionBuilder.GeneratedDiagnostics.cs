#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelCompositionBuilder
    {
        public static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<DatabaseScaffolder.ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
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
