#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldScalarFeatureConfigurationBuilder
    {
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
    }
}
