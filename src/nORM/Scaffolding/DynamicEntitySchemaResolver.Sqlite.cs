#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaResolver
    {
        public static IReadOnlyList<string> GetSqliteMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            var schemas = new List<string>();
            using (var schemaCommand = connection.CreateCommand())
            {
                schemaCommand.CommandText = "PRAGMA database_list";
                using var reader = schemaCommand.ExecuteReader();
                while (reader.Read())
                {
                    var schemaName = ReaderHasColumn(reader, "name")
                        ? Convert.ToString(reader["name"])
                        : reader.FieldCount > 1 ? Convert.ToString(reader[1]) : null;
                    if (!string.IsNullOrWhiteSpace(schemaName))
                        schemas.Add(schemaName);
                }
            }

            var matches = new List<string>();
            foreach (var schema in schemas.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT name FROM {DynamicEntityConnectionKind.EscapeIdentifier(connection, schema)}.sqlite_master WHERE type IN ('table', 'view') AND name = @tableName LIMIT 1";
                AddStringParameter(command, "@tableName", tableName);
                if (command.ExecuteScalar() is not null)
                    matches.Add(schema);
            }

            return matches;
        }
    }
}
