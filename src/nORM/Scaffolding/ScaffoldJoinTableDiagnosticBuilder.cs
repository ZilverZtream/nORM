#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldJoinTableDiagnosticBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildPossibleJoinTableMetadata(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableMetadataBuilder.BuildPossibleJoinTableMetadata(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        public static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKeyInfo> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldCompositeForeignKeyMetadataBuilder.BuildCompositeForeignKeyMetadata(
                rows,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable);

        public static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndexInfo> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableReasonBuilder.BuildPossibleJoinTableReasons(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);
    }
}
