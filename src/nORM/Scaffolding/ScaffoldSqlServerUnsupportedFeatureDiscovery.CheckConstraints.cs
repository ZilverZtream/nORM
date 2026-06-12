using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqlServerUnsupportedFeatureDiscovery
    {
        private static Task MarkSystemNamedCheckConstraintFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeatureInfo> features,
            HashSet<string> tableKeys)
            => ScaffoldSyntheticFeatureNameMarker.MarkFeaturesAsync(
                connection,
                features,
                tableKeys,
                SqlServerSystemNamedCheckConstraintSql,
                "CheckConstraint");
    }
}
