#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldDiagnosticsRequest(
        IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> ForeignKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> UnsupportedFeatures,
        IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> SkippedObjects,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> Indexes,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> DatabaseGeneratedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys,
        IReadOnlySet<string>? EmittedManyToManyJoinTableKeys,
        bool SuppressRelationshipDiagnostics = false);
}
