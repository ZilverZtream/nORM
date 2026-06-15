#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldModelDiscoveryResult(
        IReadOnlyList<ScaffoldTable> Tables,
        IReadOnlyList<ScaffoldSkippedObject> SkippedObjects,
        IReadOnlySet<string> QueryArtifactTableKeys,
        IReadOnlyDictionary<string, string> EntityByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        Dictionary<string, HashSet<string>> MemberNamesByTable,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyDictionary<string, string> PrimaryKeyConstraintNamesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> SqliteDeclaredTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnStoreTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> StringBinaryFacetsByTable,
        IReadOnlyDictionary<string, ScaffoldComments> CommentsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ScaffoldedTableKeys,
        IReadOnlyList<ScaffoldIndex> Indexes,
        IReadOnlyList<ScaffoldForeignKey> ForeignKeys,
        List<ScaffoldUnsupportedFeature> UnsupportedFeatures,
        ScaffoldFeatureConfigurations FeatureConfigurations);
}
