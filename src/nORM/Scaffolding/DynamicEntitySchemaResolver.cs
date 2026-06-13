#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using nORM.Core;

namespace nORM.Scaffolding
{
    internal static partial class DynamicEntitySchemaResolver
    {
        public static (string? Schema, string Table) SplitSchema(string identifier)
        {
            var idx = identifier.IndexOf('.');
            if (idx > 0 && idx < identifier.Length - 1)
                return (identifier[..idx], identifier[(idx + 1)..]);
            return (null, identifier);
        }

        public static (string? Schema, string Table, TColumns Columns, bool IsReadOnlyEntity) ResolveTableSchema<TColumns>(
            DbConnection connection,
            string tableName,
            Func<DbConnection, string?, string, TColumns> getTableSchema,
            Func<TColumns> emptyColumnsFactory,
            Func<DbConnection, string?, string, bool> isReadOnlyDynamicObject)
        {
            if (tableName.Contains('.', StringComparison.Ordinal))
            {
                var exactFound = TryGetTableSchema(connection, null, tableName, getTableSchema, emptyColumnsFactory, out var exactColumns);
                var (schemaName, bareTable) = SplitSchema(tableName);
                var schemaFound = TryGetTableSchema(connection, schemaName, bareTable, getTableSchema, emptyColumnsFactory, out var schemaColumns);

                if (exactFound && schemaFound)
                {
                    throw new NormConfigurationException(
                        $"Dynamic scaffolding table name '{tableName}' is ambiguous: it matches both a literal table name and a schema-qualified table. " +
                        "Use a typed model or remove the naming collision before using Query(string).");
                }

                if (exactFound)
                    return (null, tableName, exactColumns, isReadOnlyDynamicObject(connection, null, tableName));

                if (schemaFound)
                    return (schemaName, bareTable, schemaColumns, isReadOnlyDynamicObject(connection, schemaName, bareTable));
            }

            var (fallbackSchemaName, fallbackBareTable) = SplitSchema(tableName);
            if (fallbackSchemaName is null)
                fallbackSchemaName = ResolveUniqueUnqualifiedSchema(connection, fallbackBareTable);

            var columns = getTableSchema(connection, fallbackSchemaName, fallbackBareTable);
            return (fallbackSchemaName, fallbackBareTable, columns, isReadOnlyDynamicObject(connection, fallbackSchemaName, fallbackBareTable));
        }

        public static bool TryGetTableSchema<TColumns>(
            DbConnection connection,
            string? schemaName,
            string tableName,
            Func<DbConnection, string?, string, TColumns> getTableSchema,
            Func<TColumns> emptyColumnsFactory,
            out TColumns columns)
        {
            try
            {
                columns = getTableSchema(connection, schemaName, tableName);
                return true;
            }
            catch (DbException)
            {
                columns = emptyColumnsFactory();
                return false;
            }
        }

        public static string? ResolveUniqueUnqualifiedSchema(DbConnection connection, string tableName)
        {
            var schemas = GetMatchingObjectSchemas(connection, tableName);
            if (schemas.Count > 1)
            {
                throw new NormConfigurationException(
                    $"Dynamic scaffolding table name '{tableName}' is ambiguous because it exists in multiple schemas/catalogs: " +
                    string.Join(", ", schemas.Select(schema => "'" + schema + "'")) +
                    ". Use a schema-qualified table name before using Query(string).");
            }

            if (schemas.Count == 1)
            {
                if (DynamicEntityConnectionKind.IsSqlServer(connection) || DynamicEntityConnectionKind.IsPostgres(connection))
                    return schemas[0];
            }

            return null;
        }

        public static IReadOnlyList<string> GetMatchingObjectSchemas(DbConnection connection, string tableName)
        {
            if (DynamicEntityConnectionKind.IsSqlite(connection))
                return GetSqliteMatchingObjectSchemas(connection, tableName);

            if (DynamicEntityConnectionKind.IsSqlServer(connection))
                return GetSqlServerMatchingObjectSchemas(connection, tableName);

            if (DynamicEntityConnectionKind.IsPostgres(connection))
                return GetPostgresMatchingObjectSchemas(connection, tableName);

            return Array.Empty<string>();
        }
    }
}
