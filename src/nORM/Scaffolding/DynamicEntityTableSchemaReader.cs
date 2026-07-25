#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using ColumnInfo = nORM.Scaffolding.DynamicEntityTypeGenerator.ColumnInfo;

namespace nORM.Scaffolding
{
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Database scaffolding emits dynamic entity types and traverses live mapping metadata; not NativeAOT-compatible. See docs/aot-trimming.md.")]
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Database scaffolding reflects over provider and entity metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
    internal static partial class DynamicEntityTableSchemaReader
    {
        public static IReadOnlyList<ColumnInfo> GetTableSchema(DbConnection connection, string? schemaName, string tableName)
        {
            var qualified = DynamicEntityConnectionKind.EscapeQualified(connection, schemaName, tableName);
            var postgresDomainColumnCastTypes = GetPostgresDomainColumnCastTypes(connection, schemaName, tableName);
            var schema = ReadSchemaTable(connection, schemaName, tableName, qualified, postgresDomainColumnCastTypes);

            if (schema is null)
                return Array.Empty<ColumnInfo>();

            var existingPropertyNames = CreateReservedMemberNameSet();
            existingPropertyNames.Add(EscapeCSharpIdentifier(ToPascalCase(tableName)));
            var metadata = ReadColumnMetadata(connection, schemaName, tableName, postgresDomainColumnCastTypes);
            return BuildColumnInfos(connection, schema, existingPropertyNames, metadata);
        }

        private static DataTable? ReadSchemaTable(
            DbConnection connection,
            string? schemaName,
            string tableName,
            string qualified,
            IReadOnlyDictionary<string, string> postgresDomainColumnCastTypes)
        {
            // Microsoft.Data.Sqlite's GetSchemaTable(KeyInfo) emits double-quoted identifiers that older SQLite
            // (DQS enabled) tolerated as string-literal fallbacks; the SQLitePCLRaw 3.0.x native bundle
            // (SQLite 3.53+, pinned to clear CVE-2025-6965) defaults DQS OFF, so the reader now errors. Restore
            // DQS for DML+DDL on THIS connection (the caller may pass a raw connection that never went through
            // the provider init). nORM's own SQL never relies on DQS — it quotes identifiers correctly and
            // parameterizes/single-quotes every value — so this only re-aligns the driver's schema reader.
            EnableDoubleQuotedStringsForSchemaReader(connection);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = BuildSchemaProbeSql(connection, schemaName, tableName, qualified, postgresDomainColumnCastTypes);
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            return reader.GetSchemaTable();
        }

        private static void EnableDoubleQuotedStringsForSchemaReader(DbConnection connection)
        {
            if (connection is not Microsoft.Data.Sqlite.SqliteConnection sqlite || sqlite.State != ConnectionState.Open)
                return;
            const int SQLITE_DBCONFIG_DQS_DML = 1013;
            const int SQLITE_DBCONFIG_DQS_DDL = 1014;
            SQLitePCL.raw.sqlite3_db_config(sqlite.Handle, SQLITE_DBCONFIG_DQS_DML, 1, out _);
            SQLitePCL.raw.sqlite3_db_config(sqlite.Handle, SQLITE_DBCONFIG_DQS_DDL, 1, out _);
        }
    }
}
