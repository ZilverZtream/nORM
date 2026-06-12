#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaMetadataReader
    {
        private static IReadOnlyDictionary<string, ScaffoldColumnFacet> GetSqliteDeclaredStringBinaryFacets(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldColumnFacet>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["name"]);
                var declaredType = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(columnName)
                    && ScaffoldSqliteDdlParser.TryParseDeclaredStringBinaryFacet(declaredType, out var facet))
                {
                    result[columnName!] = facet;
                }
            }

            return result;
        }

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo> GetSqliteDeclaredDecimalPrecisions(
            DbConnection connection,
            string? schemaName,
            string tableName)
        {
            var result = new Dictionary<string, ScaffoldDecimalPrecisionInfo>(StringComparer.OrdinalIgnoreCase);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = SqlitePragma(connection, schemaName, "table_xinfo", tableName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var columnName = Convert.ToString(reader["name"]);
                var declaredType = Convert.ToString(reader["type"]);
                if (!string.IsNullOrWhiteSpace(columnName)
                    && ScaffoldSqliteDdlParser.TryParseDeclaredDecimalPrecision(declaredType, out var precision))
                {
                    result[columnName!] = precision;
                }
            }

            return result;
        }

    }
}
