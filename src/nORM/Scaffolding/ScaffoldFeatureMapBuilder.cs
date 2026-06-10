#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldFeatureMapBuilder
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

        public static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldFeatureInput> features)
            => BuildFeatureTableKeys(features, "TemporalTable");

        public static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
        {
            var tableKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            tableKeys.UnionWith(providerNativeTemporalTableKeys);
            tableKeys.UnionWith(providerOwnedTriggerTableKeys);
            tableKeys.UnionWith(providerSpecificIdentityStrategyTableKeys);
            tableKeys.UnionWith(providerSpecificDefaultTableKeys);
            foreach (var (tableKey, providerSpecificColumnTypes) in providerSpecificColumnTypesByTable)
            {
                if (ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes))
                    tableKeys.Add(tableKey);
            }

            return tableKeys;
        }

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
