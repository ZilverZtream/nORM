#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldDiagnosticsRequest(
        IReadOnlyList<ScaffoldForeignKey> ForeignKeys,
        IReadOnlyList<ScaffoldUnsupportedFeature> UnsupportedFeatures,
        IReadOnlyList<ScaffoldSkippedObject> SkippedObjects,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyList<ScaffoldIndex> Indexes,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> DatabaseGeneratedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys,
        IReadOnlySet<string>? EmittedManyToManyJoinTableKeys,
        bool SuppressRelationshipDiagnostics = false);
}
