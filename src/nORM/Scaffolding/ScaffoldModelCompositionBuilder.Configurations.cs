#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelCompositionBuilder
    {
        private static IReadOnlyList<DatabaseScaffolder.ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            ScaffoldModelDiscoveryResult discovery,
            ScaffoldFeatureConfigurations featureConfigurations)
            => ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                discovery.EntityByTable,
                discovery.ColumnPropertiesByTable,
                featureConfigurations.DefaultValuesByTable,
                featureConfigurations.DefaultConstraintNamesByTable);

        private static T[] ExcludeJoinTableConfigurations<T>(
            IEnumerable<T> configurations,
            IReadOnlySet<string> manyToManyJoinTableKeys,
            Func<T, string> getTableKey)
            => configurations
                .Where(config => !manyToManyJoinTableKeys.Contains(getTableKey(config)))
                .ToArray();
    }
}
