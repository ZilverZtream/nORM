#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal sealed record ScaffoldDiagnosticReportRequest(
        IReadOnlyList<ScaffoldForeignKeyInfo> ForeignKeys,
        IReadOnlyList<ScaffoldUnsupportedFeatureInfo> UnsupportedFeatures,
        IReadOnlyList<ScaffoldSkippedObjectInfo> SkippedObjects,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyList<ScaffoldIndexInfo> Indexes,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> DatabaseGeneratedColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys,
        IReadOnlySet<string>? EmittedManyToManyJoinTableKeys);
}
