#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlySet<string> GetSqliteIdentityColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                rows.Add((
                    Convert.ToString(reader["name"]) ?? string.Empty,
                    Convert.ToString(reader["type"]) ?? string.Empty,
                    ReaderHasColumn(reader, "pk")
                        ? Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture)
                        : 0));
            }

            var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
            if (primaryKeyColumns.Length == 1
                && primaryKeyColumns[0].Type.Contains("INT", StringComparison.OrdinalIgnoreCase))
            {
                return new HashSet<string>(StringComparer.OrdinalIgnoreCase) { primaryKeyColumns[0].Name };
            }

            return EmptyColumnNameSet();
        }

        private static IReadOnlyDictionary<string, int> GetSqlitePrimaryKeyOrdinals(DbConnection connection, string? schemaName, string tableName)
        {
            var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "name") || !ReaderHasColumn(reader, "pk"))
                    continue;

                var ordinal = Convert.ToInt32(reader["pk"], CultureInfo.InvariantCulture);
                var name = Convert.ToString(reader["name"]);
                if (ordinal > 0 && !string.IsNullOrWhiteSpace(name))
                    result[name] = ordinal;
            }

            return result;
        }
    }
}
