#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldScalarFeatureConfigurationBuilder
    {
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
