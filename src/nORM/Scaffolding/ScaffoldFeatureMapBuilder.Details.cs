#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureMapBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldFeatureInput> features,
            params string[] kinds)
        {
            var kindSet = kinds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!kindSet.Contains(feature.Kind)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = feature.Detail;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }
    }
}
