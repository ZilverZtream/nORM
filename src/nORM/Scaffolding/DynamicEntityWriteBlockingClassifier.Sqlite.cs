#nullable enable
using System;
using System.Data.Common;
using static nORM.Scaffolding.DynamicEntitySchemaResolver;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntityWriteBlockingClassifier
    {
        private static bool HasWriteBlockingSqliteColumns(DbConnection connection, string? schemaName, string tableName)
        {
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"PRAGMA {EscapeIdentifier(connection, schema)}.table_xinfo({EscapeIdentifier(connection, tableName)})";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                if (!ReaderHasColumn(reader, "type"))
                    continue;

                if (IsWriteBlockingSqliteDeclaredType(Convert.ToString(reader["type"])))
                    return true;
            }

            return false;
        }

        public static bool IsWriteBlockingSqliteDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return IsUnsafeSqliteProviderSpecificDeclaredType(normalized);
        }

        public static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOMETRY")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOGRAPHY")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "POINT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "LINESTRING")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "POLYGON")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTIPOINT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTILINESTRING")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MULTIPOLYGON")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "GEOMETRYCOLLECTION")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "HIERARCHYID")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "SQL_VARIANT")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "INET")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "CIDR")
               || ContainsSqliteDeclaredTypeToken(normalizedDeclaredType, "MACADDR")
               || StartsWithSqliteDeclaredTypeToken(normalizedDeclaredType, "ENUM")
               || StartsWithSqliteDeclaredTypeToken(normalizedDeclaredType, "SET")
               || normalizedDeclaredType.EndsWith("[]", StringComparison.Ordinal);

        public static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
        {
            var start = 0;
            while (start < normalizedDeclaredType.Length)
            {
                var index = normalizedDeclaredType.IndexOf(token, start, StringComparison.Ordinal);
                if (index < 0)
                    return false;

                var before = index == 0 || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[index - 1]);
                var afterIndex = index + token.Length;
                var after = afterIndex == normalizedDeclaredType.Length || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[afterIndex]);
                if (before && after)
                    return true;

                start = index + token.Length;
            }

            return false;
        }

        public static bool IsSqliteUuidDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            return !IsUnsafeSqliteProviderSpecificDeclaredType(normalized)
                   && ContainsSqliteDeclaredTypeToken(normalized, "UUID");
        }

        private static bool StartsWithSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => normalizedDeclaredType.StartsWith(token, StringComparison.Ordinal)
               && (normalizedDeclaredType.Length == token.Length
                   || !IsSqliteDeclaredTypeTokenChar(normalizedDeclaredType[token.Length]));

        private static bool IsSqliteDeclaredTypeTokenChar(char ch)
            => (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
    }
}
