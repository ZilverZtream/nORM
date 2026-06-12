#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityReadOnlyClassifier
    {
        private static bool HasSqliteUnmodeledDefaults(DbConnection connection, string? schemaName, string tableName)
        {
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"PRAGMA {DynamicEntityConnectionKind.EscapeIdentifier(connection, schema)}.table_xinfo({DynamicEntityConnectionKind.EscapeIdentifier(connection, tableName)})";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "dflt_value"))
                    continue;

                if (HasUnmodeledDefaultSql(Convert.ToString(reader["dflt_value"])))
                    return true;
            }

            return false;
        }
    }
}
