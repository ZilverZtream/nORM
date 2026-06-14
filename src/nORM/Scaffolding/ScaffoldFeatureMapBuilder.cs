#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureMapBuilder
    {
        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldFeatureInput> features)
            => BuildFeatureTableKeys(features, "TemporalTable");

        public static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
        {
            var kindSet = kinds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var input in features)
            {
                if (kindSet.Contains(input.Feature.Kind))
                    result.Add(input.Feature.TableKey);
            }

            return result;
        }
    }
}
