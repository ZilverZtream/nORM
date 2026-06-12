#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using static nORM.Scaffolding.DynamicEntitySchemaMetadataQuery;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityComputedColumnReader
    {
        private static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> GetSqliteComputedColumns(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn>(StringComparer.OrdinalIgnoreCase);
            foreach (var (columnName, computed) in ExtractSqliteGeneratedColumns(GetSqliteCreateTableSql(connection, schemaName, tableName)))
                result[columnName] = computed;

            using var cmd = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : DynamicEntityConnectionKind.EscapeIdentifier(connection, schemaName!) + ".";
            cmd.CommandText = $"PRAGMA {schemaPrefix}table_xinfo({DynamicEntityConnectionKind.EscapeIdentifier(connection, tableName)})";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "hidden")
                    || !ReaderHasColumn(reader, "name")
                    || Convert.ToInt32(reader["hidden"], CultureInfo.InvariantCulture) is not (2 or 3))
                {
                    continue;
                }

                var name = Convert.ToString(reader["name"]);
                if (!string.IsNullOrWhiteSpace(name) && !result.ContainsKey(name))
                    result[name] = new DynamicEntityTypeGenerator.ScaffoldComputedColumn(string.Empty, Stored: false);
            }

            return result;
        }

        public static string? GetSqliteCreateTableSql(DbConnection connection, string? schemaName, string tableName)
        {
            using var command = connection.CreateCommand();
            var schemaPrefix = string.IsNullOrWhiteSpace(schemaName)
                ? string.Empty
                : DynamicEntityConnectionKind.EscapeIdentifier(connection, schemaName!) + ".";
            command.CommandText = $"SELECT sql FROM {schemaPrefix}sqlite_master WHERE type = 'table' AND name = @tableName";
            AddStringParameter(command, "@tableName", tableName);
            return Convert.ToString(command.ExecuteScalar());
        }

        public static IReadOnlyDictionary<string, DynamicEntityTypeGenerator.ScaffoldComputedColumn> ExtractSqliteGeneratedColumns(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractGeneratedColumns(createTableSql)
                .ToDictionary(
                    static pair => pair.Key,
                    static pair => new DynamicEntityTypeGenerator.ScaffoldComputedColumn(pair.Value.Sql, pair.Value.Stored),
                    StringComparer.OrdinalIgnoreCase);
    }
}
