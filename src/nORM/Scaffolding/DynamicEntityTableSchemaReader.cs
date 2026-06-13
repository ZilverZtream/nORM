#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using ColumnInfo = nORM.Scaffolding.DynamicEntityTypeGenerator.ColumnInfo;

namespace nORM.Scaffolding
{
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
            using var cmd = connection.CreateCommand();
            cmd.CommandText = BuildSchemaProbeSql(connection, schemaName, tableName, qualified, postgresDomainColumnCastTypes);
            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo);
            return reader.GetSchemaTable();
        }
    }
}
