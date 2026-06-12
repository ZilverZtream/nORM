#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        public static IReadOnlyDictionary<string, string> GetSqliteDeclaredColumnTypes(DbConnection connection, string? schemaName, string tableName)
        {
            if (!DynamicEntityConnectionKind.IsSqlite(connection))
                return new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "name") || !ReaderHasColumn(reader, "type"))
                    continue;

                var name = Convert.ToString(reader["name"]);
                var type = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(type))
                    result[name] = type;
            }

            return result;
        }

        private static string SqlitePragma(DbConnection connection, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : DynamicEntityConnectionKind.EscapeIdentifier(connection, schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({DynamicEntityConnectionKind.EscapeIdentifier(connection, argument)})";
        }
    }
}
