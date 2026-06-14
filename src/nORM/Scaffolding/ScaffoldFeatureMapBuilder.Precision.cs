#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldFeatureMapBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new Dictionary<string, Dictionary<string, ScaffoldDecimalPrecisionInfo>>(StringComparer.OrdinalIgnoreCase);
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "PrecisionScale", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !ScaffoldSqlMetadataParser.TryParseDecimalPrecision(feature.Detail, out var precision, out var scale))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, ScaffoldDecimalPrecisionInfo>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = new ScaffoldDecimalPrecisionInfo(precision, scale);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }
    }
}
