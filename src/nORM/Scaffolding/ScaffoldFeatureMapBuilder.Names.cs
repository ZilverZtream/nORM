#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureMapBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
        {
            var kindSet = kinds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!kindSet.Contains(feature.Kind)
                    || string.IsNullOrWhiteSpace(feature.Name))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var names))
                {
                    names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = names;
                }

                names.Add(feature.Name);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlySet<string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }
    }
}
