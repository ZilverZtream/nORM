#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldScalarFeatureConfigurationBuilder
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

        public static IReadOnlyList<ScaffoldPrecisionConfigurationInfo> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
        {
            var result = new List<ScaffoldPrecisionConfigurationInfo>();
            foreach (var (tableKey, precisions) in decimalPrecisionByTable)
            {
                if (!entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var properties))
                {
                    continue;
                }

                foreach (var (columnName, precision) in precisions)
                {
                    if (properties.TryGetValue(columnName, out var propertyName))
                    {
                        result.Add(new ScaffoldPrecisionConfigurationInfo(
                            tableKey,
                            entityName,
                            columnName,
                            propertyName,
                            precision.Precision,
                            precision.Scale));
                    }
                }
            }

            return result;
        }

        public static IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
        {
            var result = new List<ScaffoldColumnFacetConfigurationInfo>();
            foreach (var (tableKey, facets) in columnFacetsByTable)
            {
                if (!entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var properties))
                {
                    continue;
                }

                foreach (var (columnName, facet) in facets)
                {
                    var isUnicode = facet.IsUnicode == false ? false : (bool?)null;
                    if ((facet.MaxLength.HasValue || isUnicode.HasValue || facet.IsFixedLength)
                        && properties.TryGetValue(columnName, out var propertyName))
                    {
                        result.Add(new ScaffoldColumnFacetConfigurationInfo(
                            tableKey,
                            entityName,
                            columnName,
                            propertyName,
                            facet.MaxLength,
                            isUnicode,
                            facet.IsFixedLength));
                    }
                }
            }

            return result;
        }

        public static IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new List<ScaffoldIdentityOptionConfigurationInfo>();
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "IdentityStrategy", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !ScaffoldSqlMetadataParser.TryParseIdentityOptions(feature.Detail, out var seed, out var increment)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                result.Add(new ScaffoldIdentityOptionConfigurationInfo(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    seed,
                    increment));
            }

            return result;
        }

        public static IReadOnlyList<ScaffoldCollationConfigurationInfo> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new List<ScaffoldCollationConfigurationInfo>();
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "Collation", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                result.Add(new ScaffoldCollationConfigurationInfo(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    feature.Detail.Trim()));
            }

            return result;
        }

        public static IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new List<ScaffoldComputedColumnConfigurationInfo>();
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "Computed", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                var (sql, stored) = ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(feature.Detail);
                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                result.Add(new ScaffoldComputedColumnConfigurationInfo(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    sql,
                    stored));
            }

            return result;
        }
    }
}
