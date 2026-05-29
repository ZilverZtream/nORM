#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.ObjectPool;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace nORM.Scaffolding
{
    /// <summary>
    /// Provides reverse-engineering utilities that can scaffold entity classes and a DbContext
    /// from an existing database schema.
    /// </summary>
    public static class DatabaseScaffolder
    {
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="contextName">Name of the generated DbContext.</param>
        public static async Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, string contextName = "AppDbContext")
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (provider is null) throw new ArgumentNullException(nameof(provider));
            if (outputDirectory is null) throw new ArgumentNullException(nameof(outputDirectory));
            if (namespaceName is null) throw new ArgumentNullException(nameof(namespaceName));
            if (string.IsNullOrWhiteSpace(contextName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(contextName));

            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                Directory.CreateDirectory(outputDirectory);
                var tables = await GetTablesAsync(connection, provider).ConfigureAwait(false);
                var entityNames = new List<string>();
                var entityByTable = BuildEntityNameMap(tables);
                var columnPropertiesByTable = await GetColumnPropertyNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var foreignKeys = await GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);
                var relationships = BuildRelationships(foreignKeys, entityByTable, columnPropertiesByTable);

                foreach (var table in tables)
                {
                    var tableName = table.Name;
                    var schemaName = table.Schema;

                    var tableKey = TableKey(schemaName, tableName);
                    var entityName = entityByTable[tableKey];
                    entityNames.Add(entityName);

                    var references = relationships.Where(r => string.Equals(r.DependentTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                    var collections = relationships.Where(r => string.Equals(r.PrincipalTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                    columnPropertiesByTable.TryGetValue(tableKey, out var columnPropertyNames);
                    var entityCode = await ScaffoldEntityAsync(connection, provider, schemaName, tableName, entityName, namespaceName, columnPropertyNames, references, collections).ConfigureAwait(false);
                    await File.WriteAllTextAsync(Path.Combine(outputDirectory, entityName + ".cs"), entityCode).ConfigureAwait(false);
                }

                var ctxCode = ScaffoldContextWithRelationships(namespaceName, contextName, entityNames, relationships);
                await File.WriteAllTextAsync(Path.Combine(outputDirectory, contextName + ".cs"), ctxCode).ConfigureAwait(false);
            }
            finally
            {
                // Only close the connection if we opened it
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Generates C# source code for a single entity type based on the provided database schema information.
        /// </summary>
        /// <param name="connection">Active database connection.</param>
        /// <param name="provider">Database provider in use.</param>
        /// <param name="schemaName">Optional schema name.</param>
        /// <param name="tableName">Table name in the database.</param>
        /// <param name="entityName">Name of the entity class to produce.</param>
        /// <param name="namespaceName">Namespace for the generated entity.</param>
        /// <param name="columnPropertyNames">Optional map from database column names to generated C# property names.</param>
        /// <param name="references">Reference navigations from this entity to principal entities.</param>
        /// <param name="collections">Collection navigations from this entity to dependent entities.</param>
        /// <returns>A string containing the generated C# code.</returns>
        private static async Task<string> ScaffoldEntityAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            string entityName,
            string namespaceName,
            IReadOnlyDictionary<string, string>? columnPropertyNames = null,
            IReadOnlyList<ScaffoldRelationship>? references = null,
            IReadOnlyList<ScaffoldRelationship>? collections = null)
        {
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                sb.AppendLine("using System;");
                sb.AppendLine("using System.Collections.Generic;");
                sb.AppendLine("using System.ComponentModel.DataAnnotations;");
                sb.AppendLine("using System.ComponentModel.DataAnnotations.Schema;");
                sb.AppendLine();
                sb.AppendLine($"namespace {namespaceName};");
                sb.AppendLine();
                // Class-level [Table] with schema (if available); escape quotes to prevent code injection
                var safeTableName = tableName.Replace("\\", "\\\\").Replace("\"", "\\\"");
                var tableAttr = schemaName is not null
                    ? $"[Table(\"{safeTableName}\", Schema = \"{schemaName.Replace("\\", "\\\\").Replace("\"", "\\\"")}\")]"
                    : $"[Table(\"{safeTableName}\")]";
                sb.AppendLine(tableAttr);
                sb.AppendLine($"public class {EscapeCSharpIdentifier(entityName)}");
                sb.AppendLine("{");

                // Read schema using a zero-row query (fast) with key info
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {EscapeQualified(provider, schemaName, tableName)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                foreach (DataRow row in schema.Rows)
                {
                    var colName = row["ColumnName"]!.ToString()!;
                    var propName = columnPropertyNames is not null && columnPropertyNames.TryGetValue(colName, out var mappedProperty)
                        ? mappedProperty
                        : EscapeCSharpIdentifier(ToPascalCase(colName));
                    var clrType = (Type)row["DataType"]!;
                    var allowNull = row["AllowDBNull"] is bool b && b;

                    // Decide C# type name with correct nullability for value OR reference types
                    var typeName = GetTypeName(clrType, allowNull);

                    var isKey = row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                    var isAuto = row.Table.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai;

                    // String length if available
                    int? maxLength = null;
                    if (clrType == typeof(string) && row.Table.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                    {
                        if (int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0)
                            maxLength = size;
                    }

                    sb.AppendLine("    /// <summary>");
                    sb.AppendLine($"    /// Maps to column {colName}");
                    sb.AppendLine("    /// </summary>");
                    if (isKey)
                        sb.AppendLine("    [Key]");
                    if (isAuto)
                        sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]");
                    if (maxLength.HasValue)
                        sb.AppendLine($"    [MaxLength({maxLength.Value})]");
                    sb.AppendLine($"    [Column(\"{colName.Replace("\\", "\\\\").Replace("\"", "\\\"")}\")]");
                    var initializer = !clrType.IsValueType && !allowNull ? " = default!;" : string.Empty;
                    sb.AppendLine($"    public {typeName} {propName} {{ get; set; }}{initializer}");
                    sb.AppendLine();
                }

                foreach (var reference in references ?? Array.Empty<ScaffoldRelationship>())
                {
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(reference.PrincipalEntityName)}? {EscapeCSharpIdentifier(reference.ReferenceNavigationName)} {{ get; set; }}");
                    sb.AppendLine();
                }

                foreach (var collection in collections ?? Array.Empty<ScaffoldRelationship>())
                {
                    sb.AppendLine($"    public List<{EscapeCSharpIdentifier(collection.DependentEntityName)}> {EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
                    sb.AppendLine();
                }

                sb.AppendLine("}");
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        private static async Task<IReadOnlyList<ScaffoldTable>> GetTablesAsync(DbConnection connection, DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT NULL AS TABLE_SCHEMA, name AS TABLE_NAME FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").ConfigureAwait(false);
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT s.name AS TABLE_SCHEMA, t.name AS TABLE_NAME FROM sys.tables t INNER JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name").ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name").ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryTablesAsync(
                    connection,
                    "SELECT table_schema AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = DATABASE() ORDER BY table_name").ConfigureAwait(false);
            }

            return await GetSchemaTablesAsync(connection).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldForeignKey>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
            {
                var foreignKeys = new List<ScaffoldForeignKey>();
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = $"PRAGMA foreign_key_list({provider.Escape(table.Name)})";
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                    var rows = new List<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToInt64(reader["id"]),
                            Convert.ToInt64(reader["seq"]),
                            Convert.ToString(reader["table"]) ?? string.Empty,
                            Convert.ToString(reader["from"]) ?? string.Empty,
                            Convert.ToString(reader["to"]) ?? string.Empty));
                    }

                    foreach (var group in rows.GroupBy(r => r.Id).Where(g => g.Count() == 1))
                    {
                        var row = group.Single();
                        if (string.IsNullOrWhiteSpace(row.PrincipalTable)
                            || string.IsNullOrWhiteSpace(row.DependentColumn)
                            || string.IsNullOrWhiteSpace(row.PrincipalColumn))
                        {
                            continue;
                        }

                        foreignKeys.Add(new ScaffoldForeignKey(
                            DependentSchema: table.Schema,
                            DependentTable: table.Name,
                            DependentColumn: row.DependentColumn,
                            PrincipalSchema: null,
                            PrincipalTable: row.PrincipalTable,
                            PrincipalColumn: row.PrincipalColumn,
                            ConstraintName: "sqlite_fk_" + row.Id,
                            ColumnCount: 1));
                    }
                }

                return foreignKeys;
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryForeignKeysAsync(connection, """
                    SELECT
                        SCHEMA_NAME(dep.schema_id) AS DependentSchema,
                        dep.name AS DependentTable,
                        dep_col.name AS DependentColumn,
                        SCHEMA_NAME(principal.schema_id) AS PrincipalSchema,
                        principal.name AS PrincipalTable,
                        principal_col.name AS PrincipalColumn,
                        fk.name AS ConstraintName,
                        COUNT(*) OVER (PARTITION BY fk.object_id) AS ColumnCount
                    FROM sys.foreign_keys fk
                    INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                    INNER JOIN sys.tables dep ON dep.object_id = fk.parent_object_id
                    INNER JOIN sys.columns dep_col ON dep_col.object_id = dep.object_id AND dep_col.column_id = fkc.parent_column_id
                    INNER JOIN sys.tables principal ON principal.object_id = fk.referenced_object_id
                    INNER JOIN sys.columns principal_col ON principal_col.object_id = principal.object_id AND principal_col.column_id = fkc.referenced_column_id
                    WHERE dep.is_ms_shipped = 0 AND principal.is_ms_shipped = 0
                    ORDER BY SCHEMA_NAME(dep.schema_id), dep.name, fk.name, fkc.constraint_column_id
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryForeignKeysAsync(connection, """
                    SELECT
                        tc.table_schema AS DependentSchema,
                        tc.table_name AS DependentTable,
                        kcu.column_name AS DependentColumn,
                        ccu.table_schema AS PrincipalSchema,
                        ccu.table_name AS PrincipalTable,
                        ccu.column_name AS PrincipalColumn,
                        tc.constraint_name AS ConstraintName,
                        COUNT(*) OVER (PARTITION BY tc.constraint_schema, tc.constraint_name) AS ColumnCount
                    FROM information_schema.table_constraints tc
                    INNER JOIN information_schema.key_column_usage kcu
                        ON kcu.constraint_schema = tc.constraint_schema
                        AND kcu.constraint_name = tc.constraint_name
                        AND kcu.table_schema = tc.table_schema
                        AND kcu.table_name = tc.table_name
                    INNER JOIN information_schema.constraint_column_usage ccu
                        ON ccu.constraint_schema = tc.constraint_schema
                        AND ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryForeignKeysAsync(connection, """
                    SELECT
                        kcu.table_schema AS DependentSchema,
                        kcu.table_name AS DependentTable,
                        kcu.column_name AS DependentColumn,
                        kcu.referenced_table_schema AS PrincipalSchema,
                        kcu.referenced_table_name AS PrincipalTable,
                        kcu.referenced_column_name AS PrincipalColumn,
                        kcu.constraint_name AS ConstraintName,
                        COUNT(*) OVER (PARTITION BY kcu.constraint_schema, kcu.table_name, kcu.constraint_name) AS ColumnCount
                    FROM information_schema.key_column_usage kcu
                    WHERE kcu.table_schema = DATABASE()
                      AND kcu.referenced_table_name IS NOT NULL
                    ORDER BY kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position
                    """).ConfigureAwait(false);
            }

            return Array.Empty<ScaffoldForeignKey>();
        }

        private static async Task<IReadOnlyList<ScaffoldForeignKey>> QueryForeignKeysAsync(DbConnection connection, string sql)
        {
            var foreignKeys = new List<ScaffoldForeignKey>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var dependentTable = Convert.ToString(reader["DependentTable"]);
                var dependentColumn = Convert.ToString(reader["DependentColumn"]);
                var principalTable = Convert.ToString(reader["PrincipalTable"]);
                var principalColumn = Convert.ToString(reader["PrincipalColumn"]);
                if (string.IsNullOrWhiteSpace(dependentTable)
                    || string.IsNullOrWhiteSpace(dependentColumn)
                    || string.IsNullOrWhiteSpace(principalTable)
                    || string.IsNullOrWhiteSpace(principalColumn))
                {
                    continue;
                }

                var columnCount = Convert.ToInt32(reader["ColumnCount"]);
                if (columnCount != 1)
                    continue;

                foreignKeys.Add(new ScaffoldForeignKey(
                    NullIfWhiteSpace(Convert.ToString(reader["DependentSchema"])),
                    dependentTable,
                    dependentColumn,
                    NullIfWhiteSpace(Convert.ToString(reader["PrincipalSchema"])),
                    principalTable,
                    principalColumn,
                    Convert.ToString(reader["ConstraintName"]) ?? string.Empty,
                    columnCount));
            }

            return foreignKeys;
        }

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            var relationships = new List<ScaffoldRelationship>();
            var referenceNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var collectionNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var foreignKey in foreignKeys.Where(fk => fk.ColumnCount == 1))
            {
                var dependentKey = TableKey(foreignKey.DependentSchema, foreignKey.DependentTable);
                var principalKey = TableKey(foreignKey.PrincipalSchema, foreignKey.PrincipalTable);
                if (!entityByTable.TryGetValue(dependentKey, out var dependentEntity)
                    || !entityByTable.TryGetValue(principalKey, out var principalEntity))
                {
                    continue;
                }

                var foreignKeyProperty = GetColumnPropertyName(columnPropertiesByTable, dependentKey, foreignKey.DependentColumn);
                var principalKeyProperty = GetColumnPropertyName(columnPropertiesByTable, principalKey, foreignKey.PrincipalColumn);

                var referenceBase = principalEntity;
                if (referenceNames.TryGetValue(dependentKey, out var existingReferences)
                    && existingReferences.Contains(referenceBase))
                {
                    referenceBase = TrimIdSuffix(foreignKeyProperty);
                    if (string.IsNullOrWhiteSpace(referenceBase))
                        referenceBase = principalEntity + "Navigation";
                }

                existingReferences ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                referenceNames[dependentKey] = existingReferences;
                var referenceName = MakeUnique(referenceBase, existingReferences);

                var collectionBase = Pluralize(dependentEntity);
                if (collectionNames.TryGetValue(principalKey, out var existingCollections)
                    && existingCollections.Contains(collectionBase))
                {
                    collectionBase = Pluralize(dependentEntity) + "By" + foreignKeyProperty;
                }

                existingCollections ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                collectionNames[principalKey] = existingCollections;
                var collectionName = MakeUnique(collectionBase, existingCollections);

                relationships.Add(new ScaffoldRelationship(
                    dependentKey,
                    principalKey,
                    dependentEntity,
                    principalEntity,
                    foreignKeyProperty,
                    principalKeyProperty,
                    referenceName,
                    collectionName));
            }

            return relationships;
        }

        private static IReadOnlyDictionary<string, string> BuildEntityNameMap(IReadOnlyList<ScaffoldTable> tables)
        {
            var names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var existingNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                var baseName = EscapeCSharpIdentifier(ToPascalCase(table.Name));
                names[TableKey(table.Schema, table.Name)] = MakeUnique(baseName, existingNames);
            }

            return names;
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetColumnPropertyNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {EscapeQualified(provider, table.Schema, table.Name)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var existingNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var properties = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (DataRow row in schema.Rows)
                {
                    var columnName = row["ColumnName"]!.ToString()!;
                    var baseName = EscapeCSharpIdentifier(ToPascalCase(columnName));
                    properties[columnName] = MakeUnique(baseName, existingNames);
                }

                result[TableKey(table.Schema, table.Name)] = properties;
            }

            return result;
        }

        private static string GetColumnPropertyName(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            string tableKey,
            string columnName)
        {
            if (columnPropertiesByTable.TryGetValue(tableKey, out var properties)
                && properties.TryGetValue(columnName, out var propertyName))
            {
                return propertyName;
            }

            return EscapeCSharpIdentifier(ToPascalCase(columnName));
        }

        private static async Task<IReadOnlyList<ScaffoldTable>> QueryTablesAsync(DbConnection connection, string sql)
        {
            var tables = new List<ScaffoldTable>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = reader.IsDBNull(0) ? null : reader.GetString(0);
                var table = reader.GetString(1);
                tables.Add(new ScaffoldTable(table, string.IsNullOrWhiteSpace(schema) ? null : schema));
            }

            return tables;
        }

        private static async Task<IReadOnlyList<ScaffoldTable>> GetSchemaTablesAsync(DbConnection connection)
        {
            var schema = await connection.GetSchemaAsync("Tables").ConfigureAwait(false);
            var tables = new List<ScaffoldTable>();
            foreach (DataRow row in schema.Rows)
            {
                var tableType = row.Table.Columns.Contains("TABLE_TYPE") ? row["TABLE_TYPE"]?.ToString() : null;
                if (tableType != null && !string.Equals(tableType, "TABLE", StringComparison.OrdinalIgnoreCase))
                    continue;

                var tableName = row["TABLE_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(tableName))
                    continue;

                var schemaName = row.Table.Columns.Contains("TABLE_SCHEMA")
                    ? row["TABLE_SCHEMA"]?.ToString()
                    : null;

                tables.Add(new ScaffoldTable(tableName, string.IsNullOrWhiteSpace(schemaName) ? null : schemaName));
            }

            return tables;
        }

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
            => ScaffoldContextWithRelationships(namespaceName, contextName, entities, Array.Empty<ScaffoldRelationship>());

        private static string ScaffoldContextWithRelationships(
            string namespaceName,
            string contextName,
            IEnumerable<string> entities,
            IReadOnlyList<ScaffoldRelationship> relationships)
        {
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                sb.AppendLine("using System.Data.Common;");
                if (relationships.Count > 0)
                    sb.AppendLine("using System;");
                sb.AppendLine("using nORM.Core;");
                sb.AppendLine("using nORM.Configuration;");
                sb.AppendLine("using nORM.Providers;");
                sb.AppendLine();
                sb.AppendLine($"namespace {namespaceName};");
                sb.AppendLine();
                sb.AppendLine($"public class {EscapeCSharpIdentifier(contextName)} : DbContext");
                sb.AppendLine("{");
                if (relationships.Count == 0)
                {
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(contextName)}(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null) : base(cn, provider, options) {{ }}");
                }
                else
                {
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(contextName)}(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null) : base(cn, provider, ConfigureOptions(options)) {{ }}");
                }
                sb.AppendLine();
                var queryPropertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var entity in entities.OrderBy(e => e))
                {
                    var safeEntity = EscapeCSharpIdentifier(entity);
                    var queryProperty = MakeUnique(Pluralize(safeEntity), queryPropertyNames);
                    sb.AppendLine($"    public INormQueryable<{safeEntity}> {queryProperty} => this.Query<{safeEntity}>();");
                }

                if (relationships.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("    private static DbContextOptions ConfigureOptions(DbContextOptions? options)");
                    sb.AppendLine("    {");
                    sb.AppendLine("        options ??= new DbContextOptions();");
                    sb.AppendLine("        var configure = options.OnModelCreating;");
                    sb.AppendLine("        options.OnModelCreating = mb =>");
                    sb.AppendLine("        {");
                    sb.AppendLine("            configure?.Invoke(mb);");
                    foreach (var relationship in relationships.OrderBy(r => r.PrincipalEntityName, StringComparer.Ordinal).ThenBy(r => r.DependentEntityName, StringComparer.Ordinal))
                    {
                        var principal = EscapeCSharpIdentifier(relationship.PrincipalEntityName);
                        var dependent = EscapeCSharpIdentifier(relationship.DependentEntityName);
                        var collection = EscapeCSharpIdentifier(relationship.CollectionNavigationName);
                        var reference = EscapeCSharpIdentifier(relationship.ReferenceNavigationName);
                        var foreignKey = EscapeCSharpIdentifier(relationship.ForeignKeyPropertyName);
                        var principalKey = EscapeCSharpIdentifier(relationship.PrincipalKeyPropertyName);
                        sb.AppendLine($"            mb.Entity<{principal}>()");
                        sb.AppendLine($"                .HasMany(p => p.{collection})");
                        sb.AppendLine($"                .WithOne(d => d.{reference})");
                        sb.AppendLine($"                .HasForeignKey(d => d.{foreignKey}, p => p.{principalKey});");
                    }
                    sb.AppendLine("        };");
                    sb.AppendLine("        return options;");
                    sb.AppendLine("    }");
                }

                sb.AppendLine("}");
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Returns a C# type name with correct nullability for value or reference types.
        /// </summary>
        private static string GetTypeName(Type type, bool allowNull)
        {
            string name = type == typeof(byte[]) ? "byte[]" : type switch
            {
                var t when t == typeof(int) => "int",
                var t when t == typeof(long) => "long",
                var t when t == typeof(short) => "short",
                var t when t == typeof(byte) => "byte",
                var t when t == typeof(bool) => "bool",
                var t when t == typeof(string) => "string",
                var t when t == typeof(DateTime) => "DateTime",
                var t when t == typeof(decimal) => "decimal",
                var t when t == typeof(double) => "double",
                var t when t == typeof(float) => "float",
                var t when t == typeof(Guid) => "Guid",
                _ => type.FullName ?? type.Name
            };

            // Add '?' if the DB allows nulls (for both value types and reference types)
            if (allowNull)
            {
                name += "?";
            }

            return name;
        }

        /// <summary>
        /// Converts a database object name to PascalCase by removing separators and capitalizing
        /// the first letter of each segment.
        /// </summary>
        private static string ToPascalCase(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) return name;

            var sb = _stringBuilderPool.Get();
            try
            {
                var segmentStart = 0;
                for (var i = 0; i <= name.Length; i++)
                {
                    if (i < name.Length && char.IsLetterOrDigit(name[i]))
                        continue;

                    AppendPascalSegment(sb, name.AsSpan(segmentStart, i - segmentStart));
                    segmentStart = i + 1;
                }

                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Returns the last segment of a dot-delimited identifier.
        /// </summary>
        internal static string GetUnqualifiedName(string identifier)
        {
            var idx = identifier.LastIndexOf('.');
            return idx >= 0 ? identifier[(idx + 1)..] : identifier;
        }

        /// <summary>
        /// Returns the schema segment (before the first dot) or null if no dot or empty schema.
        /// </summary>
        internal static string? GetSchemaNameOrNull(string identifier)
        {
            var idx = identifier.IndexOf('.');
            return idx > 0 ? identifier[..idx] : null;
        }

        /// <summary>
        /// Combines schema and table with a dot separator when schema is present.
        /// No SQL escaping is applied.
        /// </summary>
        internal static string EscapeQualifiedIfNeeded(string? schema, string table)
            => string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";

        /// <summary>
        /// Escapes an identifier using the appropriate provider for the given connection.
        /// For schema-qualified names (containing a dot), both parts are escaped.
        /// </summary>
        internal static string EscapeIdentifier(DbConnection connection, string identifier)
        {
            DatabaseProvider provider = connection switch
            {
                Microsoft.Data.Sqlite.SqliteConnection => new SqliteProvider(),
                _ => new SqliteProvider() // default fallback
            };
            var dot = identifier.IndexOf('.');
            if (dot >= 0)
            {
                var schema = identifier[..dot];
                var table = identifier[(dot + 1)..];
                return $"{provider.Escape(schema)}.{provider.Escape(table)}";
            }
            return provider.Escape(identifier);
        }

        /// <summary>
        /// Combines schema and table with a dot separator. No SQL escaping.
        /// </summary>
        internal static string EscapeQualified(string schema, string table)
            => $"{schema}.{table}";

        /// <summary>
        /// Escapes schema and table identifiers using the given provider's rules.
        /// </summary>
        private static string EscapeQualified(DatabaseProvider provider, string? schema, string table)
            => string.IsNullOrEmpty(schema) ? provider.Escape(table) : $"{provider.Escape(schema!)}.{provider.Escape(table)}";

        /// <summary>
        /// Escapes an identifier so it is valid in generated C# code. Reserved keywords are
        /// prefixed with <c>@</c>; other invalid characters are replaced with underscores.
        /// </summary>
        private static string EscapeCSharpIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return "_";

            var sb = _stringBuilderPool.Get();
            try
            {
                for (var i = 0; i < identifier.Length; i++)
                {
                    var ch = identifier[i];
                    var valid = i == 0
                        ? char.IsLetter(ch) || ch == '_'
                        : char.IsLetterOrDigit(ch) || ch == '_';

                    if (valid)
                    {
                        sb.Append(ch);
                    }
                    else if (i == 0 && char.IsDigit(ch))
                    {
                        sb.Append('_').Append(ch);
                    }
                    else
                    {
                        sb.Append('_');
                    }
                }

                if (sb.Length == 0)
                    sb.Append('_');

                var escaped = sb.ToString();
                return _csharpKeywords.Contains(escaped) ? "@" + escaped : escaped;
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        private static void AppendPascalSegment(StringBuilder sb, ReadOnlySpan<char> segment)
        {
            if (segment.IsEmpty)
                return;

            var hasLower = false;
            for (var i = 0; i < segment.Length; i++)
            {
                if (char.IsLower(segment[i]))
                {
                    hasLower = true;
                    break;
                }
            }

            sb.Append(char.ToUpperInvariant(segment[0]));
            for (var i = 1; i < segment.Length; i++)
            {
                sb.Append(hasLower ? segment[i] : char.ToLowerInvariant(segment[i]));
            }
        }

        private static string TableKey(string? schema, string table)
            => string.IsNullOrWhiteSpace(schema) ? table : schema + "." + table;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;

        private static string TrimIdSuffix(string name)
        {
            if (name.EndsWith("Id", StringComparison.Ordinal) && name.Length > 2)
                return name[..^2];

            if (name.EndsWith("_id", StringComparison.OrdinalIgnoreCase) && name.Length > 3)
                return name[..^3];

            return name;
        }

        private static string MakeUnique(string baseName, HashSet<string> existingNames)
        {
            var candidate = EscapeCSharpIdentifier(string.IsNullOrWhiteSpace(baseName) ? "_" : baseName);
            var unique = candidate;
            var suffix = 2;
            while (existingNames.Contains(unique))
            {
                unique = candidate + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }

            existingNames.Add(unique);
            return unique;
        }

        private static string Pluralize(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return "Items";

            if (name.EndsWith("y", StringComparison.OrdinalIgnoreCase)
                && name.Length > 1
                && !"aeiou".Contains(char.ToLowerInvariant(name[^2]), StringComparison.Ordinal))
            {
                return name[..^1] + "ies";
            }

            if (name.EndsWith("s", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("x", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("z", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("ch", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith("sh", StringComparison.OrdinalIgnoreCase))
            {
                return name + "es";
            }

            return name + "s";
        }

        private static readonly HashSet<string> _csharpKeywords = new(StringComparer.Ordinal)
        {
            // Reserved keywords
            "abstract","as","base","bool","break","byte","case","catch","char","checked","class","const",
            "continue","decimal","default","delegate","do","double","else","enum","event","explicit","extern",
            "false","finally","fixed","float","for","foreach","goto","if","implicit","in","int","interface",
            "internal","is","lock","long","namespace","new","null","object","operator","out","override","params",
            "private","protected","public","readonly","ref","return","sbyte","sealed","short","sizeof","stackalloc",
            "static","string","struct","switch","this","throw","true","try","typeof","uint","ulong","unchecked",
            "unsafe","ushort","using","virtual","void","volatile","while",
            // Contextual keywords commonly used as identifiers in database column names
            "record","partial","var","dynamic","async","await","nameof","when","and","or","not","with",
            "init","required","file","scoped","global","managed","unmanaged","nint","nuint","value","yield"
        };

        private readonly record struct ScaffoldTable(string Name, string? Schema);

        private readonly record struct ScaffoldForeignKey(
            string? DependentSchema,
            string DependentTable,
            string DependentColumn,
            string? PrincipalSchema,
            string PrincipalTable,
            string PrincipalColumn,
            string ConstraintName,
            int ColumnCount);

        private readonly record struct ScaffoldRelationship(
            string DependentTableKey,
            string PrincipalTableKey,
            string DependentEntityName,
            string PrincipalEntityName,
            string ForeignKeyPropertyName,
            string PrincipalKeyPropertyName,
            string ReferenceNavigationName,
            string CollectionNavigationName);
    }
}
