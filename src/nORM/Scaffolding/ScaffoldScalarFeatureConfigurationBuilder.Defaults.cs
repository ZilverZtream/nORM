#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldScalarFeatureConfigurationBuilder
    {
        public static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldFeatureInput> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.ContainsKey(feature.Name)
                    || !ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(feature.Detail, out var defaultValueSql))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = defaultValueSql;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        public static IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
        {
            var result = new List<ScaffoldDefaultValueConfigurationInfo>();
            foreach (var (tableKey, defaults) in defaultValuesByTable)
            {
                if (!entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var properties))
                    continue;

                foreach (var (columnName, defaultValueSql) in defaults)
                {
                    if (properties.TryGetValue(columnName, out var propertyName))
                        result.Add(new ScaffoldDefaultValueConfigurationInfo(tableKey, entityName, columnName, propertyName, defaultValueSql));
                }
            }

            return result;
        }
    }
}
