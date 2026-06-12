#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureConfigurationBuilder
    {
        private static void RemoveGeneratedUnsupportedFeatures(
            List<ScaffoldFeatureInput> unsupportedFeatures,
            ICollection<int> generatedFeatureIndexes,
            Predicate<ScaffoldFeatureInput> predicate)
        {
            for (var i = unsupportedFeatures.Count - 1; i >= 0; i--)
            {
                var input = unsupportedFeatures[i];
                if (!predicate(input))
                    continue;

                generatedFeatureIndexes.Add(input.SourceIndex);
                unsupportedFeatures.RemoveAt(i);
            }
        }
    }
}
