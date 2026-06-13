#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldExpressionIndexConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var featureList = features as IReadOnlyList<ScaffoldFeatureInput> ?? features.ToArray();
            var providerSpecificIndexes = BuildFeatureMap(featureList, "ProviderSpecificIndex");
            var includedColumnIndexes = BuildFeatureMap(featureList, "IncludedColumnIndex");
            var result = new List<ScaffoldExpressionIndexConfigurationInfo>();
            foreach (var input in featureList)
            {
                if (TryBuildExpressionIndexConfiguration(
                    entityByTable,
                    providerSpecificIndexes,
                    includedColumnIndexes,
                    input.Feature,
                    out var configuration))
                {
                    result.Add(configuration);
                }
            }

            return result;
        }

        public static bool IsProviderOwnedExpressionIndexDetail(string detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return true;

            var values = ScaffoldSemicolonParser.Parse(detail, out var header);
            if (values.ContainsKey("expression")
                && header.StartsWith("MySQL expression index", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return values.ContainsKey("expression")
                   && header.EndsWith("expression index", StringComparison.OrdinalIgnoreCase)
                   && !header.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase);
        }
    }
}
