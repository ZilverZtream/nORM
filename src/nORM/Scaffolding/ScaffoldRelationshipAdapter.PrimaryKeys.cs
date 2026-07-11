#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldRelationshipAdapter
    {
        public static IReadOnlyList<ScaffoldPrimaryKey> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys,
            bool suppressFixedPrimaryConstraintName = false)
            => ConvertPrimaryKeyConfigurations(ScaffoldPrimaryKeyConfigurationBuilder.BuildPrimaryKeyConfigurations(
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                skippedTableKeys,
                suppressFixedPrimaryConstraintName));

        public static IReadOnlyList<ScaffoldPrimaryKey> ConvertPrimaryKeyConfigurations(
            IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> primaryKeys)
            => primaryKeys
                .Select(static key => new ScaffoldPrimaryKey(key.EntityName, key.PropertyNames.ToArray(), key.ConstraintName))
                .ToArray();
    }
}
