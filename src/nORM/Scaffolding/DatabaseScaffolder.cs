#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.ObjectPool;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using nORM.Mapping;

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
            => await ScaffoldAsync(connection, provider, outputDirectory, namespaceName, contextName, options: null).ConfigureAwait(false);

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="options">Scaffolding options.</param>
        public static Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, ScaffoldOptions options)
            => ScaffoldAsync(connection, provider, outputDirectory, namespaceName, "AppDbContext", options);

        /// <summary>
        /// Generates entity classes and a DbContext based on the current database schema.
        /// </summary>
        /// <param name="connection">Open database connection.</param>
        /// <param name="provider">Database provider implementation.</param>
        /// <param name="outputDirectory">Directory to write generated files into.</param>
        /// <param name="namespaceName">Namespace for the generated classes.</param>
        /// <param name="contextName">Name of the generated DbContext.</param>
        /// <param name="options">Scaffolding options.</param>
        public static async Task ScaffoldAsync(DbConnection connection, DatabaseProvider provider, string outputDirectory, string namespaceName, string contextName, ScaffoldOptions? options)
        {
            if (connection is null) throw new ArgumentNullException(nameof(connection));
            if (provider is null) throw new ArgumentNullException(nameof(provider));
            if (outputDirectory is null) throw new ArgumentNullException(nameof(outputDirectory));
            if (namespaceName is null) throw new ArgumentNullException(nameof(namespaceName));
            if (string.IsNullOrWhiteSpace(contextName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(contextName));
            if (!IsValidNamespaceName(namespaceName))
                throw new NormConfigurationException(
                    $"Scaffold namespace '{namespaceName}' is not a valid C# namespace. " +
                    "Use a dot-separated namespace such as 'MyApp.Data'.");
            options ??= new ScaffoldOptions();
            var safeContextName = EscapeCSharpIdentifier(ToPascalCase(contextName));

            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                Directory.CreateDirectory(outputDirectory);
                var discoveredTables = await GetTablesAsync(connection, provider).ConfigureAwait(false);
                var discoveredSkippedObjects = await GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
                var tables = FilterTables(discoveredTables, discoveredSkippedObjects, options);
                EnsureNoTableKeyCollisions(tables);
                var skippedObjects = FilterSkippedObjects(discoveredSkippedObjects, options);
                var entityNames = new List<string>();
                var entityByTable = BuildEntityNameMap(tables);
                safeContextName = MakeUniqueContextName(safeContextName, entityByTable.Values);
                var columnPropertiesByTable = await GetColumnPropertyNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var memberNamesByTable = BuildMemberNameMap(columnPropertiesByTable);
                var primaryKeyColumnsByTable = await GetPrimaryKeyColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var indexes = await GetIndexesAsync(connection, provider, tables).ConfigureAwait(false);
                var foreignKeys = await GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);
                var unsupportedFeatures = (await GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
                AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable);
                AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
                var computedColumnsByTable = BuildFeatureNameMap(unsupportedFeatures, "Computed", "RowVersion");
                var rowVersionColumnsByTable = BuildFeatureNameMap(unsupportedFeatures, "RowVersion");
                var manyToManyJoins = BuildManyToManyJoins(foreignKeys, tables, entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, memberNamesByTable);
                var manyToManyJoinTableKeys = manyToManyJoins.Select(j => j.JoinTableKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var relationships = BuildRelationships(
                    foreignKeys.Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))).ToArray(),
                    entityByTable,
                    columnPropertiesByTable,
                    memberNamesByTable);
                var generatedFiles = new List<(string Path, string Content)>();

                foreach (var table in tables)
                {
                    var tableName = table.Name;
                    var schemaName = table.Schema;

                    var tableKey = TableKey(schemaName, tableName);
                    if (manyToManyJoinTableKeys.Contains(tableKey))
                        continue;

                    var entityName = entityByTable[tableKey];
                    entityNames.Add(entityName);

                    var references = relationships.Where(r => string.Equals(r.DependentTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                    var collections = relationships.Where(r => string.Equals(r.PrincipalTableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                    var manyToManyCollections = BuildManyToManyNavigations(manyToManyJoins, tableKey);
                    var tableIndexes = indexes.Where(i => string.Equals(i.TableKey, tableKey, StringComparison.OrdinalIgnoreCase)).ToArray();
                    columnPropertiesByTable.TryGetValue(tableKey, out var columnPropertyNames);
                    computedColumnsByTable.TryGetValue(tableKey, out var computedColumns);
                    rowVersionColumnsByTable.TryGetValue(tableKey, out var rowVersionColumns);
                    var entityCode = await ScaffoldEntityAsync(connection, provider, schemaName, tableName, entityName, namespaceName, columnPropertyNames, tableIndexes, references, collections, manyToManyCollections, computedColumns, rowVersionColumns).ConfigureAwait(false);
                    generatedFiles.Add((Path.Combine(outputDirectory, entityName + ".cs"), entityCode));
                }

                var ctxCode = ScaffoldContextWithRelationships(namespaceName, safeContextName, entityNames, relationships, manyToManyJoins);
                generatedFiles.Add((Path.Combine(outputDirectory, safeContextName + ".cs"), ctxCode));
                var diagnostics = ScaffoldDiagnostics(foreignKeys, unsupportedFeatures, skippedObjects, manyToManyJoinTableKeys);
                if (!string.IsNullOrWhiteSpace(diagnostics))
                {
                    generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"), diagnostics));
                    generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json"), ScaffoldDiagnosticsJson(foreignKeys, unsupportedFeatures, skippedObjects, manyToManyJoinTableKeys)));
                }

                EnsureNoOutputFileConflicts(generatedFiles.Select(file => file.Path), options);
                foreach (var (path, content) in generatedFiles)
                    await WriteGeneratedFileAsync(path, content).ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(diagnostics))
                {
                    if (options.FailOnWarnings)
                        throw new NormConfigurationException(
                            "Scaffolding produced warnings for schema features that cannot be emitted as runnable nORM model code. " +
                            "Review nORM.ScaffoldWarnings.md or disable ScaffoldOptions.FailOnWarnings.");
                }
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
        /// <param name="indexes">Index metadata for this entity's table.</param>
        /// <param name="references">Reference navigations from this entity to principal entities.</param>
        /// <param name="collections">Collection navigations from this entity to dependent entities.</param>
        /// <param name="manyToManyCollections">Many-to-many collection navigations from this entity through pure join tables.</param>
        /// <param name="computedColumns">Column names known to be database-computed/generated.</param>
        /// <param name="rowVersionColumns">Column names known to be database-managed rowversion/timestamp tokens.</param>
        /// <returns>A string containing the generated C# code.</returns>
        private static async Task<string> ScaffoldEntityAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            string entityName,
            string namespaceName,
            IReadOnlyDictionary<string, string>? columnPropertyNames = null,
            IReadOnlyList<ScaffoldIndex>? indexes = null,
            IReadOnlyList<ScaffoldRelationship>? references = null,
            IReadOnlyList<ScaffoldRelationship>? collections = null,
            IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections = null,
            IReadOnlySet<string>? computedColumns = null,
            IReadOnlySet<string>? rowVersionColumns = null)
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
                if (indexes?.Count > 0)
                    sb.AppendLine("using nORM.Configuration;");
                sb.AppendLine();
                sb.AppendLine($"namespace {namespaceName};");
                sb.AppendLine();
                // Class-level [Table] with schema (if available); escape quotes to prevent code injection
                var safeTableName = EscapeStringLiteral(tableName);
                var tableAttr = schemaName is not null
                    ? $"[Table(\"{safeTableName}\", Schema = \"{EscapeStringLiteral(schemaName)}\")]"
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
                    var isComputed = computedColumns?.Contains(colName) == true;
                    var isRowVersion = rowVersionColumns?.Contains(colName) == true;

                    // String length if available
                    int? maxLength = null;
                    if (clrType == typeof(string) && row.Table.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value)
                    {
                        if (int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0)
                            maxLength = size;
                    }

                    sb.AppendLine("    /// <summary>");
                    sb.AppendLine($"    /// Maps to column {EscapeXmlDocumentation(colName)}");
                    sb.AppendLine("    /// </summary>");
                    if (isKey)
                        sb.AppendLine("    [Key]");
                    if (isRowVersion)
                        sb.AppendLine("    [Timestamp]");
                    if (isAuto)
                        sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]");
                    else if (isComputed)
                        sb.AppendLine("    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]");
                    if (maxLength.HasValue)
                        sb.AppendLine($"    [MaxLength({maxLength.Value})]");
                    if (!clrType.IsValueType && !allowNull)
                        sb.AppendLine("    [Required]");
                    foreach (var index in (indexes ?? Array.Empty<ScaffoldIndex>())
                        .Where(i => string.Equals(i.ColumnName, colName, StringComparison.Ordinal))
                        .OrderBy(i => i.IndexName, StringComparer.Ordinal)
                        .ThenBy(i => i.Ordinal))
                    {
                        if (index.IndexName.Length == 0)
                            continue;
                        var safeIndexName = EscapeStringLiteral(index.IndexName);
                        var uniqueSuffix = index.IsUnique ? ", IsUnique = true" : string.Empty;
                        var orderSuffix = index.ColumnCount > 1 ? $", Order = {index.Ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture)}" : string.Empty;
                        sb.AppendLine($"    [Index(\"{safeIndexName}\"{uniqueSuffix}{orderSuffix})]");
                    }
                    sb.AppendLine($"    [Column(\"{EscapeStringLiteral(colName)}\")]");
                    var initializer = !clrType.IsValueType && !allowNull ? " = default!;" : string.Empty;
                    sb.AppendLine($"    public {typeName} {propName} {{ get; set; }}{initializer}");
                    sb.AppendLine();
                }

                foreach (var reference in references ?? Array.Empty<ScaffoldRelationship>())
                {
                    sb.AppendLine($"    [ForeignKey(nameof({EscapeCSharpIdentifier(reference.ForeignKeyPropertyName)}))]");
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(reference.PrincipalEntityName)}? {EscapeCSharpIdentifier(reference.ReferenceNavigationName)} {{ get; set; }}");
                    sb.AppendLine();
                }

                foreach (var collection in collections ?? Array.Empty<ScaffoldRelationship>())
                {
                    sb.AppendLine($"    public List<{EscapeCSharpIdentifier(collection.DependentEntityName)}> {EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
                    sb.AppendLine();
                }

                foreach (var collection in manyToManyCollections ?? Array.Empty<ScaffoldManyToManyNavigation>())
                {
                    sb.AppendLine($"    public List<{EscapeCSharpIdentifier(collection.TargetEntityName)}> {EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
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
                var tables = new List<ScaffoldTable>();
                foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
                {
                    tables.AddRange(await QueryTablesAsync(
                        connection,
                        $"SELECT {SqliteSchemaResult(schema)} AS TABLE_SCHEMA, name AS TABLE_NAME FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").ConfigureAwait(false));
                }

                return tables;
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
                    "SELECT NULL AS TABLE_SCHEMA, table_name AS TABLE_NAME FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = DATABASE() ORDER BY table_name").ConfigureAwait(false);
            }

            return await GetSchemaTablesAsync(connection).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObject>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
            {
                var objects = new List<ScaffoldSkippedObject>();
                foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
                {
                    objects.AddRange(await QuerySkippedObjectsAsync(
                        connection,
                        $"SELECT {SqliteSchemaResult(schema)} AS ObjectSchema, name AS ObjectName, 'View' AS Kind, 'SQLite view' AS Detail FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'view' ORDER BY name").ConfigureAwait(false));
                }

                return objects;
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectsAsync(connection, """
                    SELECT SCHEMA_NAME(v.schema_id) AS ObjectSchema, v.name AS ObjectName, 'View' AS Kind, 'SQL Server view' AS Detail
                    FROM sys.views v
                    WHERE v.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(p.schema_id), p.name, 'Routine', 'SQL Server stored procedure'
                    FROM sys.procedures p
                    WHERE p.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(s.schema_id), s.name, 'Sequence', 'SQL Server sequence'
                    FROM sys.sequences s
                    UNION ALL
                    SELECT SCHEMA_NAME(s.schema_id), s.name, 'Synonym', 'SQL Server synonym'
                    FROM sys.synonyms s
                    ORDER BY ObjectSchema, ObjectName
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectsAsync(connection, """
                    SELECT table_schema AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'PostgreSQL view' AS Detail
                    FROM information_schema.views
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT sequence_schema, sequence_name, 'Sequence', 'PostgreSQL sequence'
                    FROM information_schema.sequences
                    WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT schemaname, matviewname, 'MaterializedView', 'PostgreSQL materialized view'
                    FROM pg_matviews
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT routine_schema, routine_name, 'Routine', 'PostgreSQL routine'
                    FROM information_schema.routines
                    WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY ObjectSchema, ObjectName
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QuerySkippedObjectsAsync(connection, """
                    SELECT NULL AS ObjectSchema, table_name AS ObjectName, 'View' AS Kind, 'MySQL view' AS Detail
                    FROM information_schema.views
                    WHERE table_schema = DATABASE()
                    UNION ALL
                    SELECT NULL, routine_name, 'Routine', CONCAT('MySQL ', routine_type)
                    FROM information_schema.routines
                    WHERE routine_schema = DATABASE()
                    UNION ALL
                    SELECT NULL, event_name, 'Event', 'MySQL event'
                    FROM information_schema.events
                    WHERE event_schema = DATABASE()
                    ORDER BY ObjectSchema, ObjectName
                    """).ConfigureAwait(false);
            }

            return Array.Empty<ScaffoldSkippedObject>();
        }

        private static async Task<IReadOnlyList<ScaffoldSkippedObject>> QuerySkippedObjectsAsync(DbConnection connection, string sql)
        {
            var objects = new List<ScaffoldSkippedObject>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var objectName = Convert.ToString(reader["ObjectName"]);
                if (string.IsNullOrWhiteSpace(objectName))
                    continue;

                objects.Add(new ScaffoldSkippedObject(
                    NullIfWhiteSpace(Convert.ToString(reader["ObjectSchema"])),
                    objectName,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty));
            }

            return objects;
        }

        private static async Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
        {
            var schemas = new List<string>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA database_list";
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var schema = Convert.ToString(reader["name"]);
                if (string.IsNullOrWhiteSpace(schema)
                    || string.Equals(schema, "temp", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                schemas.Add(schema);
            }

            return schemas.Count == 0 ? new[] { "main" } : schemas;
        }

        private static string SqliteSchemaResult(string schema)
            => string.Equals(schema, "main", StringComparison.OrdinalIgnoreCase)
                ? "NULL"
                : "'" + schema.Replace("'", "''") + "'";

        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static IReadOnlyList<ScaffoldTable> FilterTables(
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options)
        {
            var requested = GetRequestedTableFilters(options);
            if (requested.Length == 0)
                return tables;

            var ambiguousRequests = requested
                .Select(request => new
                {
                    Request = request,
                    Matches = tables
                        .Where(table => MatchesTableFilter(table, request))
                        .GroupBy(table => (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                        .Select(group => DisplayTableMatch(group.First()))
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()
                })
                .Where(match => match.Matches.Length > 1)
                .ToArray();

            if (ambiguousRequests.Length > 0)
            {
                throw new NormConfigurationException(
                    "Scaffolding table filter is ambiguous because it matches multiple discovered tables: " +
                    string.Join("; ", ambiguousRequests.Select(match => $"{match.Request} matched {string.Join(", ", match.Matches)}")) +
                    ". Use schema-qualified table filters when the ambiguity is across schemas; literal dotted table names that collide with schema-qualified names must be scaffolded without a table filter.");
            }

            var selected = tables
                .Where(table => requested.Any(request => MatchesTableFilter(table, request)))
                .ToArray();

            var missing = requested
                .Where(request => !tables.Any(table => MatchesTableFilter(table, request)))
                .ToArray();

            if (missing.Length > 0)
            {
                var skippedMatches = skippedObjects
                    .Where(obj => missing.Any(request => MatchesSkippedObjectFilter(obj, request)))
                    .Select(obj => $"{obj.Kind} {TableKey(obj.Schema, obj.Name)}")
                    .OrderBy(value => value, StringComparer.Ordinal)
                    .ToArray();

                if (skippedMatches.Length > 0)
                {
                    throw new NormConfigurationException(
                        "Scaffolding table filter matched database object(s) that v1 scaffolding does not emit as entity classes: " +
                        string.Join(", ", skippedMatches) +
                        ". Scaffold base tables or create a provider-neutral entity manually.");
                }

                throw new NormConfigurationException(
                    "Scaffolding table filter did not match discovered table(s): " +
                    string.Join(", ", missing));
            }

            return selected;
        }

        private static bool MatchesTableFilter(ScaffoldTable table, string requested)
            => string.Equals(table.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(table.Schema, table.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static string DisplayTableMatch(ScaffoldTable table)
            => string.IsNullOrWhiteSpace(table.Schema)
                ? "<default>." + table.Name
                : TableKey(table.Schema, table.Name);

        private static void EnsureNoTableKeyCollisions(IReadOnlyList<ScaffoldTable> tables)
        {
            var collisions = tables
                .GroupBy(table => TableKey(table.Schema, table.Name), StringComparer.OrdinalIgnoreCase)
                .Select(group => new
                {
                    DisplayKey = group.Key,
                    Matches = group
                        .GroupBy(table => (table.Schema ?? string.Empty) + "\u001f" + table.Name, StringComparer.OrdinalIgnoreCase)
                        .Select(inner => DisplayTableMatch(inner.First()))
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()
                })
                .Where(group => group.Matches.Length > 1)
                .ToArray();

            if (collisions.Length == 0)
                return;

            throw new NormConfigurationException(
                "Scaffolding discovered tables whose display names collide with schema-qualified names: " +
                string.Join("; ", collisions.Select(c => $"{c.DisplayKey} matched {string.Join(", ", c.Matches)}")) +
                ". Rename one table or scaffold a provider-specific model manually; v1 table filters cannot disambiguate literal dotted table names from schema-qualified table names.");
        }

        private static IReadOnlyList<ScaffoldSkippedObject> FilterSkippedObjects(IReadOnlyList<ScaffoldSkippedObject> skippedObjects, ScaffoldOptions options)
        {
            var requested = GetRequestedTableFilters(options);
            if (requested.Length == 0)
                return skippedObjects;

            return skippedObjects
                .Where(obj => requested.Any(request => MatchesSkippedObjectFilter(obj, request)))
                .ToArray();
        }

        private static bool MatchesSkippedObjectFilter(ScaffoldSkippedObject obj, string requested)
            => string.Equals(obj.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(obj.Schema, obj.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static string[] GetRequestedTableFilters(ScaffoldOptions options)
        {
            if (options.Tables is null)
                return Array.Empty<string>();

            return options.Tables
                .Where(table => !string.IsNullOrWhiteSpace(table))
                .Select(table => table.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        private static void EnsureNoOutputFileConflicts(IEnumerable<string> paths, ScaffoldOptions options)
        {
            if (options.OverwriteFiles)
                return;

            foreach (var path in paths)
            {
                if (File.Exists(path))
                {
                    throw new NormConfigurationException(
                        $"Scaffolding output file already exists: '{path}'. Enable overwrite or choose an empty output directory.");
                }
            }
        }

        private static async Task WriteGeneratedFileAsync(string path, string content)
        {
            await File.WriteAllTextAsync(path, content).ConfigureAwait(false);
        }

        private static async Task<IReadOnlyList<ScaffoldIndex>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var providerName = provider.GetType().Name;
            if (provider is SqliteProvider)
            {
                var indexes = new List<ScaffoldIndex>();
                foreach (var table in tables)
                {
                    await using var listCommand = connection.CreateCommand();
                    listCommand.CommandText = SqlitePragma(provider, table.Schema, "index_list", table.Name);
                    await using var listReader = await listCommand.ExecuteReaderAsync().ConfigureAwait(false);
                    var tableIndexes = new List<(string Name, bool IsUnique, string Origin, bool IsPartial)>();
                    while (await listReader.ReadAsync().ConfigureAwait(false))
                    {
                        var name = Convert.ToString(listReader["name"]);
                        if (string.IsNullOrWhiteSpace(name))
                            continue;

                        tableIndexes.Add((
                            name,
                            Convert.ToInt32(listReader["unique"], System.Globalization.CultureInfo.InvariantCulture) != 0,
                            Convert.ToString(listReader["origin"]) ?? string.Empty,
                            ReaderHasColumn(listReader, "partial")
                                && Convert.ToInt32(listReader["partial"], System.Globalization.CultureInfo.InvariantCulture) != 0));
                    }

                    foreach (var (name, isUnique, origin, isPartial) in tableIndexes)
                    {
                        if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase) || isPartial)
                            continue;

                        await using var infoCommand = connection.CreateCommand();
                        infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", name);
                        await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);
                        var columns = new List<(int Ordinal, string Name)>();
                        var hasUnsupportedKeyPart = false;
                        while (await infoReader.ReadAsync().ConfigureAwait(false))
                        {
                            if (ReaderHasColumn(infoReader, "key")
                                && Convert.ToInt32(infoReader["key"], System.Globalization.CultureInfo.InvariantCulture) == 0)
                            {
                                continue;
                            }

                            if (ReaderHasColumn(infoReader, "cid")
                                && Convert.ToInt32(infoReader["cid"], System.Globalization.CultureInfo.InvariantCulture) < 0)
                            {
                                hasUnsupportedKeyPart = true;
                                continue;
                            }

                            var columnName = Convert.ToString(infoReader["name"]);
                            if (!string.IsNullOrWhiteSpace(columnName))
                            {
                                columns.Add((
                                    Convert.ToInt32(infoReader["seqno"], System.Globalization.CultureInfo.InvariantCulture),
                                    columnName));
                            }
                        }

                        if (hasUnsupportedKeyPart)
                            continue;

                        foreach (var column in columns.OrderBy(static c => c.Ordinal))
                            indexes.Add(new ScaffoldIndex(TableKey(table.Schema, table.Name), column.Name, name, isUnique, columns.Count, column.Ordinal));
                    }
                }

                return indexes;
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryIndexesAsync(connection, """
                    SELECT
                        SCHEMA_NAME(t.schema_id) AS TableSchema,
                        t.name AS TableName,
                        c.name AS ColumnName,
                        i.name AS IndexName,
                        i.is_unique AS IsUnique,
                        COUNT(*) OVER (PARTITION BY i.object_id, i.index_id) AS ColumnCount,
                        ic.key_ordinal - 1 AS Ordinal
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON t.object_id = i.object_id
                    INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                    WHERE t.is_ms_shipped = 0
                      AND i.is_primary_key = 0
                      AND i.is_hypothetical = 0
                      AND i.has_filter = 0
                      AND NOT EXISTS (
                          SELECT 1
                          FROM sys.index_columns included
                          WHERE included.object_id = i.object_id
                            AND included.index_id = i.index_id
                            AND included.is_included_column = 1
                      )
                      AND i.name IS NOT NULL
                      AND ic.is_included_column = 0
                    ORDER BY SCHEMA_NAME(t.schema_id), t.name, i.name, ic.key_ordinal
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryIndexesAsync(connection, """
                    SELECT
                        ns.nspname AS TableSchema,
                        tbl.relname AS TableName,
                        att.attname AS ColumnName,
                        idx.relname AS IndexName,
                        ix.indisunique AS IsUnique,
                        COUNT(*) OVER (PARTITION BY ix.indexrelid) AS ColumnCount,
                        key.ord - 1 AS Ordinal
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    INNER JOIN unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ord) ON true
                    INNER JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key.attnum
                    WHERE ix.indisprimary = false
                      AND ix.indpred IS NULL
                      AND ix.indexprs IS NULL
                      AND ix.indnatts = ix.indnkeyatts
                      AND key.ord <= ix.indnkeyatts
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY ns.nspname, tbl.relname, idx.relname, key.ord
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryIndexesAsync(connection, """
                    SELECT
                        NULL AS TableSchema,
                        s.table_name AS TableName,
                        s.column_name AS ColumnName,
                        s.index_name AS IndexName,
                        CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS IsUnique,
                        COUNT(*) OVER (PARTITION BY s.table_schema, s.table_name, s.index_name) AS ColumnCount,
                        s.seq_in_index - 1 AS Ordinal
                    FROM information_schema.statistics s
                    WHERE s.table_schema = DATABASE()
                      AND s.index_name <> 'PRIMARY'
                    ORDER BY s.table_schema, s.table_name, s.index_name, s.seq_in_index
                    """).ConfigureAwait(false);
            }

            return Array.Empty<ScaffoldIndex>();
        }

        private static async Task<IReadOnlyList<ScaffoldIndex>> QueryIndexesAsync(DbConnection connection, string sql)
        {
            var indexes = new List<ScaffoldIndex>();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableName = Convert.ToString(reader["TableName"]);
                var columnName = Convert.ToString(reader["ColumnName"]);
                var indexName = Convert.ToString(reader["IndexName"]);
                if (string.IsNullOrWhiteSpace(tableName)
                    || string.IsNullOrWhiteSpace(columnName)
                    || string.IsNullOrWhiteSpace(indexName))
                {
                    continue;
                }

                var columnCount = Convert.ToInt32(reader["ColumnCount"], System.Globalization.CultureInfo.InvariantCulture);
                indexes.Add(new ScaffoldIndex(
                    TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), tableName),
                    columnName,
                    indexName,
                    Convert.ToBoolean(reader["IsUnique"], System.Globalization.CultureInfo.InvariantCulture),
                    columnCount,
                    Convert.ToInt32(reader["Ordinal"], System.Globalization.CultureInfo.InvariantCulture)));
            }

            return indexes;
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
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "foreign_key_list", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                    var rows = new List<(long Id, long Seq, string PrincipalTable, string DependentColumn, string PrincipalColumn, string OnUpdate, string OnDelete)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToInt64(reader["id"]),
                            Convert.ToInt64(reader["seq"]),
                            Convert.ToString(reader["table"]) ?? string.Empty,
                            Convert.ToString(reader["from"]) ?? string.Empty,
                            Convert.ToString(reader["to"]) ?? string.Empty,
                            Convert.ToString(reader["on_update"]) ?? string.Empty,
                            Convert.ToString(reader["on_delete"]) ?? string.Empty));
                    }

                    foreach (var group in rows.GroupBy(r => r.Id))
                    {
                        var groupRows = group.OrderBy(r => r.Seq).ToArray();
                        foreach (var row in groupRows)
                        {
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
                                PrincipalSchema: table.Schema,
                                PrincipalTable: row.PrincipalTable,
                                PrincipalColumn: row.PrincipalColumn,
                                ConstraintName: "sqlite_fk_" + row.Id,
                                ColumnCount: groupRows.Length,
                                OnDelete: NormalizeReferentialAction(row.OnDelete),
                                OnUpdate: NormalizeReferentialAction(row.OnUpdate)));
                        }
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
                        COUNT(*) OVER (PARTITION BY fk.object_id) AS ColumnCount,
                        fk.delete_referential_action_desc AS OnDelete,
                        fk.update_referential_action_desc AS OnUpdate
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
                        dep_ns.nspname AS DependentSchema,
                        dep.relname AS DependentTable,
                        dep_att.attname AS DependentColumn,
                        principal_ns.nspname AS PrincipalSchema,
                        principal.relname AS PrincipalTable,
                        principal_att.attname AS PrincipalColumn,
                        con.conname AS ConstraintName,
                        array_length(con.conkey, 1) AS ColumnCount,
                        CASE con.confdeltype
                            WHEN 'c' THEN 'CASCADE'
                            WHEN 'n' THEN 'SET NULL'
                            WHEN 'd' THEN 'SET DEFAULT'
                            WHEN 'r' THEN 'RESTRICT'
                            ELSE 'NO ACTION'
                        END AS OnDelete,
                        CASE con.confupdtype
                            WHEN 'c' THEN 'CASCADE'
                            WHEN 'n' THEN 'SET NULL'
                            WHEN 'd' THEN 'SET DEFAULT'
                            WHEN 'r' THEN 'RESTRICT'
                            ELSE 'NO ACTION'
                        END AS OnUpdate
                    FROM pg_constraint con
                    INNER JOIN pg_class dep ON dep.oid = con.conrelid
                    INNER JOIN pg_namespace dep_ns ON dep_ns.oid = dep.relnamespace
                    INNER JOIN pg_class principal ON principal.oid = con.confrelid
                    INNER JOIN pg_namespace principal_ns ON principal_ns.oid = principal.relnamespace
                    INNER JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS key_pair(dep_attnum, principal_attnum, ord) ON true
                    INNER JOIN pg_attribute dep_att ON dep_att.attrelid = dep.oid AND dep_att.attnum = key_pair.dep_attnum
                    INNER JOIN pg_attribute principal_att ON principal_att.attrelid = principal.oid AND principal_att.attnum = key_pair.principal_attnum
                    WHERE con.contype = 'f'
                      AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY dep_ns.nspname, dep.relname, con.conname, key_pair.ord
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryForeignKeysAsync(connection, """
                    SELECT
                        NULL AS DependentSchema,
                        kcu.table_name AS DependentTable,
                        kcu.column_name AS DependentColumn,
                        NULL AS PrincipalSchema,
                        kcu.referenced_table_name AS PrincipalTable,
                        kcu.referenced_column_name AS PrincipalColumn,
                        kcu.constraint_name AS ConstraintName,
                        COUNT(*) OVER (PARTITION BY kcu.constraint_schema, kcu.table_name, kcu.constraint_name) AS ColumnCount,
                        rc.delete_rule AS OnDelete,
                        rc.update_rule AS OnUpdate
                    FROM information_schema.key_column_usage kcu
                    INNER JOIN information_schema.referential_constraints rc
                        ON rc.constraint_schema = kcu.constraint_schema
                       AND rc.constraint_name = kcu.constraint_name
                       AND rc.table_name = kcu.table_name
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

                foreignKeys.Add(new ScaffoldForeignKey(
                    NullIfWhiteSpace(Convert.ToString(reader["DependentSchema"])),
                    dependentTable,
                    dependentColumn,
                    NullIfWhiteSpace(Convert.ToString(reader["PrincipalSchema"])),
                    principalTable,
                    principalColumn,
                    Convert.ToString(reader["ConstraintName"]) ?? string.Empty,
                    Convert.ToInt32(reader["ColumnCount"], System.Globalization.CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "OnDelete") ? NormalizeReferentialAction(Convert.ToString(reader["OnDelete"])) : "NO ACTION",
                    ReaderHasColumn(reader, "OnUpdate") ? NormalizeReferentialAction(Convert.ToString(reader["OnUpdate"])) : "NO ACTION"));
            }

            return foreignKeys;
        }

        private static string NormalizeReferentialAction(string? action)
        {
            if (string.IsNullOrWhiteSpace(action))
                return "NO ACTION";

            return action.Replace('_', ' ').Trim().ToUpperInvariant();
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var features = new List<ScaffoldUnsupportedFeature>();
            var providerName = provider.GetType().Name;

            if (provider is SqliteProvider)
            {
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var name = Convert.ToString(reader["name"]) ?? string.Empty;
                        var defaultValue = Convert.ToString(reader["dflt_value"]);
                        var hidden = Convert.ToInt32(reader["hidden"], System.Globalization.CultureInfo.InvariantCulture);
                        if (!string.IsNullOrWhiteSpace(defaultValue))
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "Default", name, defaultValue));
                        if (hidden is 2 or 3)
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "Computed", name, "SQLite generated column"));
                    }
                }

                foreach (var table in tables)
                {
                    await using var checkCommand = connection.CreateCommand();
                    var schema = string.IsNullOrWhiteSpace(table.Schema) ? "main" : table.Schema!;
                    checkCommand.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
                    var tableNameParameter = checkCommand.CreateParameter();
                    tableNameParameter.ParameterName = "@tableName";
                    tableNameParameter.Value = table.Name;
                    checkCommand.Parameters.Add(tableNameParameter);
                    var createSql = Convert.ToString(await checkCommand.ExecuteScalarAsync().ConfigureAwait(false));
                    if (ContainsCheckConstraint(createSql))
                    {
                        features.Add(new ScaffoldUnsupportedFeature(
                            TableKey(table.Schema, table.Name),
                            "CheckConstraint",
                            table.Name,
                            "SQLite CHECK constraint"));
                    }

                    if (ContainsCollation(createSql))
                    {
                        features.Add(new ScaffoldUnsupportedFeature(
                            TableKey(table.Schema, table.Name),
                            "Collation",
                            table.Name,
                            "SQLite COLLATE clause"));
                    }
                }

                foreach (var schema in await GetSqliteSchemasAsync(connection).ConfigureAwait(false))
                {
                    await using var triggerCmd = connection.CreateCommand();
                    triggerCmd.CommandText = $"SELECT {SqliteSchemaResult(schema)} AS TableSchema, tbl_name AS TableName, name AS TriggerName FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'trigger'";
                    await using var triggerReader = await triggerCmd.ExecuteReaderAsync().ConfigureAwait(false);
                    while (await triggerReader.ReadAsync().ConfigureAwait(false))
                    {
                        var tableName = Convert.ToString(triggerReader["TableName"]);
                        var triggerName = Convert.ToString(triggerReader["TriggerName"]);
                        var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(triggerReader["TableSchema"])), tableName ?? string.Empty);
                        if (!string.IsNullOrWhiteSpace(tableName) && tableKeys.Contains(tableKey))
                            features.Add(new ScaffoldUnsupportedFeature(tableKey, "Trigger", triggerName ?? string.Empty, "SQLite trigger"));
                    }
                }

                foreach (var table in tables)
                {
                    await using var listCommand = connection.CreateCommand();
                    listCommand.CommandText = SqlitePragma(provider, table.Schema, "index_list", table.Name);
                    await using var listReader = await listCommand.ExecuteReaderAsync().ConfigureAwait(false);
                    var indexRows = new List<(string Name, bool IsPartial, string Origin)>();
                    while (await listReader.ReadAsync().ConfigureAwait(false))
                    {
                        var indexName = Convert.ToString(listReader["name"]);
                        if (string.IsNullOrWhiteSpace(indexName))
                            continue;

                        indexRows.Add((
                            indexName,
                            ReaderHasColumn(listReader, "partial")
                                && Convert.ToInt32(listReader["partial"], System.Globalization.CultureInfo.InvariantCulture) != 0,
                            Convert.ToString(listReader["origin"]) ?? string.Empty));
                    }

                    foreach (var (indexName, isPartial, origin) in indexRows)
                    {
                        if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase))
                            continue;

                        if (isPartial)
                        {
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "PartialIndex", indexName, "SQLite partial index"));
                            continue;
                        }

                        await using var infoCommand = connection.CreateCommand();
                        infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", indexName);
                        await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);
                        var reportedDescending = false;
                        while (await infoReader.ReadAsync().ConfigureAwait(false))
                        {
                            if (ReaderHasColumn(infoReader, "key")
                                && Convert.ToInt32(infoReader["key"], System.Globalization.CultureInfo.InvariantCulture) == 0)
                            {
                                continue;
                            }

                            if (ReaderHasColumn(infoReader, "cid")
                                && Convert.ToInt32(infoReader["cid"], System.Globalization.CultureInfo.InvariantCulture) < 0)
                            {
                                features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "ExpressionIndex", indexName, "SQLite expression index"));
                                break;
                            }

                            if (!reportedDescending
                                && ReaderHasColumn(infoReader, "desc")
                                && Convert.ToInt32(infoReader["desc"], System.Globalization.CultureInfo.InvariantCulture) != 0)
                            {
                                features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "DescendingIndex", indexName, "SQLite descending index key"));
                                reportedDescending = true;
                            }
                        }
                    }
                }

                return features;
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                await AddUnsupportedFeaturesAsync(connection, features, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ObjectName, 'Default' AS Kind, dc.definition AS Detail
                    FROM sys.default_constraints dc
                    INNER JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                    INNER JOIN sys.tables t ON t.object_id = dc.parent_object_id
                    WHERE t.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Computed', cc.definition
                    FROM sys.computed_columns cc
                    INNER JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                    INNER JOIN sys.tables t ON t.object_id = cc.object_id
                    WHERE t.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, cc.name, 'CheckConstraint', cc.definition
                    FROM sys.check_constraints cc
                    INNER JOIN sys.tables t ON t.object_id = cc.parent_object_id
                    WHERE t.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Collation', c.collation_name
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    WHERE t.is_ms_shipped = 0
                      AND c.collation_name IS NOT NULL
                      AND c.collation_name <> CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation'))
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'ProviderSpecificColumnType', ty.name
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    WHERE t.is_ms_shipped = 0
                      AND (ty.is_user_defined = 1 OR ty.name IN ('geography', 'geometry', 'hierarchyid', 'sql_variant', 'xml'))
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'PrecisionScale',
                        ty.name + '(' + CONVERT(varchar(10), c.precision) + ',' + CONVERT(varchar(10), c.scale) + ')'
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    WHERE t.is_ms_shipped = 0
                      AND ty.name IN ('decimal', 'numeric')
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'RowVersion', ty.name
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    WHERE t.is_ms_shipped = 0
                      AND ty.name IN ('timestamp', 'rowversion')
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'IdentityStrategy',
                        'IDENTITY(' + CONVERT(varchar(40), ic.seed_value) + ',' + CONVERT(varchar(40), ic.increment_value) + ')'
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    WHERE t.is_ms_shipped = 0
                      AND (CONVERT(decimal(38,0), ic.seed_value) <> 1 OR CONVERT(decimal(38,0), ic.increment_value) <> 1)
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, tr.name, 'Trigger', 'SQL Server trigger'
                    FROM sys.triggers tr
                    INNER JOIN sys.tables t ON t.object_id = tr.parent_id
                    WHERE t.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, t.name, 'TemporalTable',
                        CASE t.temporal_type WHEN 1 THEN 'SQL Server temporal history table' ELSE 'SQL Server system-versioned temporal table' END
                    FROM sys.tables t
                    WHERE t.is_ms_shipped = 0 AND t.temporal_type <> 0
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'PartialIndex', 'SQL Server filtered index'
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON t.object_id = i.object_id
                    WHERE t.is_ms_shipped = 0
                      AND i.is_primary_key = 0
                      AND i.has_filter = 1
                      AND i.name IS NOT NULL
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'IncludedColumnIndex', 'SQL Server index with included columns'
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON t.object_id = i.object_id
                    WHERE t.is_ms_shipped = 0
                      AND i.is_primary_key = 0
                      AND i.name IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM sys.index_columns included
                          WHERE included.object_id = i.object_id
                            AND included.index_id = i.index_id
                            AND included.is_included_column = 1
                      )
                    UNION ALL
                    SELECT SCHEMA_NAME(t.schema_id), t.name, i.name, 'DescendingIndex', 'SQL Server descending index key'
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON t.object_id = i.object_id
                    WHERE t.is_ms_shipped = 0
                      AND i.is_primary_key = 0
                      AND i.name IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM sys.index_columns ic
                          WHERE ic.object_id = i.object_id
                            AND ic.index_id = i.index_id
                            AND ic.key_ordinal > 0
                            AND ic.is_descending_key = 1
                      )
                    """).ConfigureAwait(false);
                return features;
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                await AddUnsupportedFeaturesAsync(connection, features, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind, column_default AS Detail
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND column_default IS NOT NULL
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'Computed', generation_expression
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND is_generated <> 'NEVER'
                    UNION ALL
                    SELECT event_object_schema, event_object_table, trigger_name, 'Trigger', 'PostgreSQL trigger'
                    FROM information_schema.triggers
                    WHERE event_object_schema NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT table_schema, table_name, constraint_name, 'CheckConstraint', 'PostgreSQL CHECK constraint'
                    FROM information_schema.table_constraints
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND constraint_type = 'CHECK'
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'Collation', collation_name
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND collation_name IS NOT NULL
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'ProviderSpecificColumnType',
                        CASE WHEN udt_name IS NULL OR udt_name = '' THEN data_type ELSE data_type || ' (' || udt_name || ')' END
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          data_type IN ('ARRAY', 'USER-DEFINED', 'json', 'jsonb', 'xml')
                          OR udt_name IN ('json', 'jsonb', 'inet', 'cidr', 'macaddr', 'macaddr8', 'tsvector', 'tsquery')
                      )
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'PrecisionScale',
                        'numeric(' || numeric_precision || ',' || numeric_scale || ')'
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND data_type = 'numeric'
                      AND numeric_precision IS NOT NULL
                      AND numeric_scale IS NOT NULL
                    UNION ALL
                    SELECT ns.nspname, tbl.relname, idx.relname, 'PartialIndex', 'PostgreSQL partial index'
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    WHERE ix.indisprimary = false
                      AND ix.indpred IS NOT NULL
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT ns.nspname, tbl.relname, idx.relname, 'ExpressionIndex', 'PostgreSQL expression index'
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    WHERE ix.indisprimary = false
                      AND ix.indexprs IS NOT NULL
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT ns.nspname, tbl.relname, idx.relname, 'IncludedColumnIndex', 'PostgreSQL index with included columns'
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    WHERE ix.indisprimary = false
                      AND ix.indnatts <> ix.indnkeyatts
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT ns.nspname, tbl.relname, idx.relname, 'DescendingIndex', 'PostgreSQL descending index key'
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    WHERE ix.indisprimary = false
                      AND pg_get_indexdef(ix.indexrelid) ILIKE '% DESC%'
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    """).ConfigureAwait(false);
                return features;
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                await AddUnsupportedFeaturesAsync(connection, features, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ObjectName, 'Default' AS Kind, column_default AS Detail
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE() AND column_default IS NOT NULL
                    UNION ALL
                    SELECT NULL, table_name, column_name, 'Computed', generation_expression
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE() AND generation_expression IS NOT NULL AND generation_expression <> ''
                    UNION ALL
                    SELECT NULL, event_object_table, trigger_name, 'Trigger', 'MySQL trigger'
                    FROM information_schema.triggers
                    WHERE trigger_schema = DATABASE()
                    UNION ALL
                    SELECT NULL, table_name, constraint_name, 'CheckConstraint', 'MySQL CHECK constraint'
                    FROM information_schema.table_constraints
                    WHERE table_schema = DATABASE() AND constraint_type = 'CHECK'
                    UNION ALL
                    SELECT NULL, c.table_name, c.column_name, 'Collation', c.collation_name
                    FROM information_schema.columns c
                    INNER JOIN information_schema.schemata s ON s.schema_name = c.table_schema
                    WHERE c.table_schema = DATABASE()
                      AND c.collation_name IS NOT NULL
                      AND c.collation_name <> s.default_collation_name
                    UNION ALL
                    SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType', data_type
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND data_type IN (
                          'json',
                          'geometry',
                          'point',
                          'linestring',
                          'polygon',
                          'multipoint',
                          'multilinestring',
                          'multipolygon',
                          'geometrycollection',
                          'enum',
                          'set',
                          'year'
                      )
                    UNION ALL
                    SELECT NULL, table_name, column_name, 'PrecisionScale',
                        CONCAT(data_type, '(', numeric_precision, ',', numeric_scale, ')')
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND data_type IN ('decimal', 'numeric')
                      AND numeric_precision IS NOT NULL
                      AND numeric_scale IS NOT NULL
                    UNION ALL
                    SELECT NULL, table_name, index_name, 'DescendingIndex', 'MySQL descending index key'
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND index_name <> 'PRIMARY'
                      AND collation = 'D'
                    """).ConfigureAwait(false);
            }

            return features;
        }

        private static async Task AddUnsupportedFeaturesAsync(
            DbConnection connection,
            List<ScaffoldUnsupportedFeature> features,
            HashSet<string> tableKeys,
            string sql)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;
                features.Add(new ScaffoldUnsupportedFeature(
                    tableKey,
                    Convert.ToString(reader["Kind"]) ?? string.Empty,
                    Convert.ToString(reader["ObjectName"]) ?? string.Empty,
                    Convert.ToString(reader["Detail"]) ?? string.Empty));
            }
        }

        private static void AddMissingPrimaryKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlySet<string>> primaryKeyColumnsByTable)
        {
            foreach (var table in tables)
            {
                var tableKey = TableKey(table.Schema, table.Name);
                if (primaryKeyColumnsByTable.TryGetValue(tableKey, out var keys) && keys.Count > 0)
                    continue;

                features.Add(new ScaffoldUnsupportedFeature(
                    tableKey,
                    "MissingPrimaryKey",
                    table.Name,
                    "Table has no primary key; generated entity is a query/bootstrap artifact until a key is configured."));
            }
        }

        private static void AddReferentialActionDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys)
        {
            foreach (var group in foreignKeys.GroupBy(
                fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}",
                StringComparer.OrdinalIgnoreCase))
            {
                var fk = group.First();
                var onDelete = NormalizeReferentialAction(fk.OnDelete);
                var onUpdate = NormalizeReferentialAction(fk.OnUpdate);
                if (string.Equals(onDelete, "CASCADE", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (string.Equals(onDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                features.Add(new ScaffoldUnsupportedFeature(
                    TableKey(fk.DependentSchema, fk.DependentTable),
                    "ReferentialAction",
                    fk.ConstraintName,
                    $"ON DELETE {onDelete}; ON UPDATE {onUpdate}"));
            }
        }

        private static bool ContainsCheckConstraint(string? createTableSql)
            => !string.IsNullOrWhiteSpace(createTableSql)
               && System.Text.RegularExpressions.Regex.IsMatch(
                   createTableSql,
                   @"\bCHECK\s*\(",
                   System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        private static bool ContainsCollation(string? createTableSql)
            => !string.IsNullOrWhiteSpace(createTableSql)
               && System.Text.RegularExpressions.Regex.IsMatch(
                   createTableSql,
                   @"\bCOLLATE\s+(?:""[^""]+""|\[[^\]]+\]|`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)",
                   System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

        private static string ScaffoldDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
        {
            var compositeForeignKeys = foreignKeys
                .Where(fk => fk.ColumnCount > 1)
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase)
                .OrderBy(g => g.First().DependentSchema, StringComparer.Ordinal)
                .ThenBy(g => g.First().DependentTable, StringComparer.Ordinal)
                .ThenBy(g => g.First().ConstraintName, StringComparer.Ordinal)
                .ToArray();
            var possibleJoinTables = foreignKeys
                .Where(fk => fk.ColumnCount == 1)
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase)
                .Select(g => new
                {
                    TableKey = g.Key,
                    PrincipalTables = g.Select(fk => TableKey(fk.PrincipalSchema, fk.PrincipalTable)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray(),
                    ConstraintNames = g.Select(fk => fk.ConstraintName).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray()
                })
                .Where(g => g.PrincipalTables.Length == 2 && g.ConstraintNames.Length >= 2)
                .Where(g => emittedManyToManyJoinTableKeys is null || !emittedManyToManyJoinTableKeys.Contains(g.TableKey))
                .OrderBy(g => g.TableKey, StringComparer.Ordinal)
                .ToArray();

            if (compositeForeignKeys.Length == 0 && possibleJoinTables.Length == 0 && unsupportedFeatures.Count == 0 && skippedObjects.Count == 0)
                return string.Empty;

            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("# nORM Scaffold Warnings");
                sb.AppendLine();
                sb.AppendLine("The scaffolder detected database features that were not converted into runnable nORM model code.");
                sb.AppendLine("Review these items before using the generated model for migrations or navigation queries.");

                if (compositeForeignKeys.Length > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Composite Foreign Keys");
                    sb.AppendLine();
                    sb.AppendLine("Composite foreign keys are discovered, but v1 navigation generation only supports single-column relationships.");
                    sb.AppendLine("The generated entity classes keep the scalar columns, and no relationship navigation is emitted for these constraints.");
                    sb.AppendLine();
                    sb.AppendLine("| Constraint | Dependent | Columns | Principal | Principal Columns | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- | --- |");
                    foreach (var group in compositeForeignKeys)
                    {
                        var rows = group.ToArray();
                        var first = rows[0];
                        var dependent = TableKey(first.DependentSchema, first.DependentTable);
                        var principal = TableKey(first.PrincipalSchema, first.PrincipalTable);
                        sb.AppendLine($"| {EscapeMarkdown(first.ConstraintName)} | {EscapeMarkdown(dependent)} | {EscapeMarkdown(string.Join(", ", rows.Select(r => r.DependentColumn)))} | {EscapeMarkdown(principal)} | {EscapeMarkdown(string.Join(", ", rows.Select(r => r.PrincipalColumn)))} | {EscapeMarkdown(SuggestedActionForCompositeForeignKey())} |");
                    }
                }

                if (possibleJoinTables.Length > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Possible Many-To-Many Join Tables");
                    sb.AppendLine();
                    sb.AppendLine("These tables have two single-column foreign key constraints. They are scaffolded as normal entities; review them if you want nORM fluent many-to-many mapping instead.");
                    sb.AppendLine();
                    sb.AppendLine("| Table | Principal Tables | Constraints | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- |");
                    foreach (var table in possibleJoinTables)
                        sb.AppendLine($"| {EscapeMarkdown(table.TableKey)} | {EscapeMarkdown(string.Join(", ", table.PrincipalTables))} | {EscapeMarkdown(string.Join(", ", table.ConstraintNames))} | {EscapeMarkdown(SuggestedActionForPossibleJoinTable())} |");
                }

                if (unsupportedFeatures.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Provider-Owned Schema Features");
                    sb.AppendLine();
                    sb.AppendLine("Defaults, computed/generated columns, check constraints, collations, provider-specific column types, numeric precision/scale, rowversion/timestamp columns, non-default identity seed/increment settings, non-default FK referential actions, triggers, provider-native temporal tables, and tables without primary keys are discovered for review, but are not emitted as complete provider-neutral nORM model code.");
                    sb.AppendLine();
                    sb.AppendLine("| Kind | Table | Object | Detail | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- |");
                    foreach (var feature in unsupportedFeatures
                        .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                        .ThenBy(f => f.Kind, StringComparer.Ordinal)
                        .ThenBy(f => f.Name, StringComparer.Ordinal))
                    {
                        sb.AppendLine($"| {EscapeMarkdown(feature.Kind)} | {EscapeMarkdown(feature.TableKey)} | {EscapeMarkdown(feature.Name)} | {EscapeMarkdown(feature.Detail)} | {EscapeMarkdown(SuggestedActionForUnsupportedFeature(feature.Kind))} |");
                    }
                }

                if (skippedObjects.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Skipped Database Objects");
                    sb.AppendLine();
                    sb.AppendLine("Views, routines, and sequences are discovered for review, but v1 scaffolding emits entity classes only for base tables.");
                    sb.AppendLine();
                    sb.AppendLine("| Kind | Name | Detail | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- |");
                    foreach (var obj in skippedObjects
                        .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                        .ThenBy(o => o.Kind, StringComparer.Ordinal))
                    {
                        sb.AppendLine($"| {EscapeMarkdown(obj.Kind)} | {EscapeMarkdown(TableKey(obj.Schema, obj.Name))} | {EscapeMarkdown(obj.Detail)} | {EscapeMarkdown(SuggestedActionForSkippedObject(obj.Kind))} |");
                    }
                }

                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        private static string EscapeMarkdown(string value)
            => value
                .Replace("\\", "\\\\")
                .Replace("|", "\\|")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        private static string EscapeStringLiteral(string value)
            => value
                .Replace("\\", "\\\\")
                .Replace("\"", "\\\"")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        private static string EscapeXmlDocumentation(string value)
            => value
                .Replace("&", "&amp;")
                .Replace("<", "&lt;")
                .Replace(">", "&gt;")
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static string SuggestedActionForCompositeForeignKey()
            => "Keep scalar columns and add the composite relationship manually, or simplify the relationship to a single-column surrogate key before relying on generated navigations.";

        private static string SuggestedActionForPossibleJoinTable()
            => "If this is a pure join table, replace the scaffolded entity with an explicit UsingTable mapping; keep it as an entity if it carries payload or domain behavior.";

        private static string SuggestedActionForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "Move default semantics into application/model configuration or keep provider DDL in migrations and treat the column as database-owned.",
                "Computed" => "Keep the generated expression in provider migrations and model the column as database-owned/read-only.",
                "CheckConstraint" => "Keep the CHECK constraint in provider migrations and duplicate critical validation in application code or explicit model configuration.",
                "Collation" => "Keep collation-sensitive behavior in provider migrations and add explicit application/query tests before relying on generated code for comparisons or ordering.",
                "ProviderSpecificColumnType" => "Keep this provider-specific type behind explicit provider migrations/converters or remodel it to a portable CLR/database shape before claiming provider mobility.",
                "PrecisionScale" => "Preserve numeric precision/scale intentionally in migrations or add explicit validation/tests before relying on regenerated decimal columns.",
                "ReferentialAction" => "Review generated relationship cascade behavior and keep non-default FK referential actions in provider migrations or explicit model configuration.",
                "RowVersion" => "Keep provider-managed rowversion/timestamp semantics in migrations; scaffolded code marks the column as [Timestamp] and database-generated but cannot recreate provider DDL.",
                "IdentityStrategy" => "Keep non-default identity seed/increment settings in provider migrations; scaffolded code marks the column as identity but does not recreate provider-specific seed metadata.",
                "Trigger" => "Keep the trigger in provider migrations and add integration tests for any side effects nORM cannot infer.",
                "PartialIndex" => "Keep the filtered/partial index in provider migrations; v1 scaffolding emits only provider-neutral column indexes.",
                "ExpressionIndex" => "Keep the expression index in provider migrations or replace it with a provider-neutral persisted column plus a normal index.",
                "IncludedColumnIndex" => "Keep included-column index tuning in provider migrations; v1 scaffolding emits only key-column index metadata.",
                "DescendingIndex" => "Keep index sort direction in provider migrations; v1 scaffolding emits key-column index membership but not per-column ASC/DESC metadata.",
                "TemporalTable" => "Choose provider-native temporal intentionally or migrate to nORM-managed temporal history; do not assume scaffolding round-trips native temporal DDL.",
                "MissingPrimaryKey" => "Add a primary key or configure the generated type as a read-only/query artifact before using writes or navigations.",
                _ => "Review the provider-owned object and add explicit model configuration or migration code for the intended behavior."
            };

        private static string SuggestedActionForSkippedObject(string kind)
            => kind switch
            {
                "View" => "Map a supported table-backed model or hand-write a read-only query surface for the view; v1 scaffolding emits base-table entities only.",
                "Routine" => "Keep routine calls behind explicit raw SQL/stored-procedure code and document the provider-bound contract.",
                "Sequence" => "Configure generated-key behavior explicitly or keep sequence DDL in provider migrations.",
                "Synonym" => "Resolve the synonym to a supported base table or keep it behind provider-bound integration code.",
                "MaterializedView" => "Map a supported base table or hand-write a provider-bound refresh/query path for the materialized view.",
                "Event" => "Keep scheduled event behavior in provider operations/migrations; v1 scaffolding emits table models only.",
                _ => "Keep this database object in provider migrations or hand-written integration code."
            };

        private static string ScaffoldDiagnosticsJson(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
        {
            var compositeForeignKeys = foreignKeys
                .Where(fk => fk.ColumnCount > 1)
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase)
                .OrderBy(g => g.First().DependentSchema, StringComparer.Ordinal)
                .ThenBy(g => g.First().DependentTable, StringComparer.Ordinal)
                .ThenBy(g => g.First().ConstraintName, StringComparer.Ordinal)
                .Select(g =>
                {
                    var rows = g.ToArray();
                    var first = rows[0];
                    return new
                    {
                        constraint = first.ConstraintName,
                        dependentTable = TableKey(first.DependentSchema, first.DependentTable),
                        dependentColumns = rows.Select(r => r.DependentColumn).ToArray(),
                        principalTable = TableKey(first.PrincipalSchema, first.PrincipalTable),
                        principalColumns = rows.Select(r => r.PrincipalColumn).ToArray(),
                        suggestedAction = SuggestedActionForCompositeForeignKey()
                    };
                })
                .ToArray();

            var possibleJoinTables = foreignKeys
                .Where(fk => fk.ColumnCount == 1)
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase)
                .Select(g => new
                {
                    TableKey = g.Key,
                    PrincipalTables = g.Select(fk => TableKey(fk.PrincipalSchema, fk.PrincipalTable)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray(),
                    ConstraintNames = g.Select(fk => fk.ConstraintName).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray()
                })
                .Where(g => g.PrincipalTables.Length == 2 && g.ConstraintNames.Length >= 2)
                .Where(g => emittedManyToManyJoinTableKeys is null || !emittedManyToManyJoinTableKeys.Contains(g.TableKey))
                .OrderBy(g => g.TableKey, StringComparer.Ordinal)
                .Select(g => new
                {
                    table = g.TableKey,
                    principalTables = g.PrincipalTables,
                    constraints = g.ConstraintNames,
                    suggestedAction = SuggestedActionForPossibleJoinTable()
                })
                .ToArray();

            var providerOwnedSchemaFeatures = unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal)
                .Select(f => new
                {
                    kind = f.Kind,
                    table = f.TableKey,
                    name = f.Name,
                    detail = f.Detail,
                    suggestedAction = SuggestedActionForUnsupportedFeature(f.Kind)
                })
                .ToArray();

            var skippedDatabaseObjects = skippedObjects
                .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                .ThenBy(o => o.Kind, StringComparer.Ordinal)
                .Select(o => new
                {
                    kind = o.Kind,
                    name = TableKey(o.Schema, o.Name),
                    detail = o.Detail,
                    suggestedAction = SuggestedActionForSkippedObject(o.Kind)
                })
                .ToArray();

            return JsonSerializer.Serialize(
                new
                {
                    version = 1,
                    compositeForeignKeys,
                    possibleManyToManyJoinTables = possibleJoinTables,
                    providerOwnedSchemaFeatures,
                    skippedDatabaseObjects
                },
                new JsonSerializerOptions { WriteIndented = true });
        }

        private static IReadOnlyList<ScaffoldManyToManyJoin> BuildManyToManyJoins(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> primaryKeyColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var joins = new List<ScaffoldManyToManyJoin>();

            foreach (var group in foreignKeys
                .Where(fk => fk.ColumnCount == 1)
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase))
            {
                var joinTableKey = group.Key;
                if (!tableKeys.Contains(joinTableKey))
                    continue;

                var fkRows = OrderManyToManyForeignKeys(
                    GetUnqualifiedName(joinTableKey),
                    group
                    .GroupBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.First())
                    .ToArray());

                if (fkRows.Length != 2)
                    continue;

                if (!columnPropertiesByTable.TryGetValue(joinTableKey, out var joinColumns))
                    continue;

                var fkColumnNames = fkRows.Select(fk => fk.DependentColumn).ToHashSet(StringComparer.OrdinalIgnoreCase);
                if (joinColumns.Count != 2 || joinColumns.Keys.Any(column => !fkColumnNames.Contains(column)))
                    continue;

                var left = fkRows[0];
                var right = fkRows[1];
                var leftTableKey = TableKey(left.PrincipalSchema, left.PrincipalTable);
                var rightTableKey = TableKey(right.PrincipalSchema, right.PrincipalTable);
                if (!entityByTable.TryGetValue(leftTableKey, out var leftEntity)
                    || !entityByTable.TryGetValue(rightTableKey, out var rightEntity))
                {
                    continue;
                }

                if (!HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, leftTableKey, left.PrincipalColumn)
                    || !HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, rightTableKey, right.PrincipalColumn))
                {
                    continue;
                }

                var existingNames = GetOrCreateMemberNames(memberNamesByTable, leftTableKey);
                var leftCollectionName = MakeUnique(Pluralize(rightEntity), existingNames);
                var existingInverseNames = GetOrCreateMemberNames(memberNamesByTable, rightTableKey);
                var rightCollectionName = MakeUnique(Pluralize(leftEntity), existingInverseNames);
                joins.Add(new ScaffoldManyToManyJoin(
                    joinTableKey,
                    leftTableKey,
                    rightTableKey,
                    joinTableKey,
                    leftEntity,
                    rightEntity,
                    left.DependentColumn,
                    right.DependentColumn,
                    leftCollectionName,
                    rightCollectionName));
            }

            return joins;
        }

        private static IReadOnlyList<ScaffoldManyToManyNavigation> BuildManyToManyNavigations(
            IReadOnlyList<ScaffoldManyToManyJoin> joins,
            string tableKey)
        {
            var navigations = new List<ScaffoldManyToManyNavigation>();
            foreach (var join in joins)
            {
                if (string.Equals(join.LeftTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new ScaffoldManyToManyNavigation(
                        join.RightEntityName,
                        join.LeftCollectionNavigationName));
                }

                if (string.Equals(join.RightTableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                {
                    navigations.Add(new ScaffoldManyToManyNavigation(
                        join.LeftEntityName,
                        join.RightCollectionNavigationName));
                }
            }

            return navigations;
        }

        private static ScaffoldForeignKey[] OrderManyToManyForeignKeys(string joinTableName, ScaffoldForeignKey[] foreignKeys)
        {
            return foreignKeys
                .OrderBy(fk => PrincipalNamePosition(joinTableName, fk.PrincipalTable))
                .ThenBy(fk => PrincipalNamePosition(joinTableName, TrimIdSuffix(fk.DependentColumn)))
                .ThenBy(fk => fk.DependentColumn, StringComparer.Ordinal)
                .ToArray();
        }

        private static int PrincipalNamePosition(string joinTableName, string principalTable)
        {
            var position = joinTableName.IndexOf(principalTable, StringComparison.OrdinalIgnoreCase);
            return position < 0 ? int.MaxValue : position;
        }

        private static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlySet<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
        {
            return primaryKeyColumnsByTable.TryGetValue(tableKey, out var keyColumns)
                   && keyColumns.Count == 1
                   && keyColumns.Contains(columnName);
        }

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var relationships = new List<ScaffoldRelationship>();
            var relationshipPairCounts = foreignKeys
                .Where(fk => fk.ColumnCount == 1)
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable) + "\u001f" + TableKey(fk.PrincipalSchema, fk.PrincipalTable), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

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
                var hasMultipleRelationshipsToSamePrincipal = relationshipPairCounts.TryGetValue(
                    dependentKey + "\u001f" + principalKey,
                    out var relationshipPairCount)
                    && relationshipPairCount > 1;

                var dependentMemberNames = GetOrCreateMemberNames(memberNamesByTable, dependentKey);
                var referenceBase = principalEntity;
                if (hasMultipleRelationshipsToSamePrincipal
                    || dependentMemberNames.Contains(referenceBase))
                {
                    referenceBase = TrimIdSuffix(foreignKeyProperty);
                    if (string.IsNullOrWhiteSpace(referenceBase))
                        referenceBase = principalEntity + "Navigation";
                }

                var referenceName = MakeUnique(referenceBase, dependentMemberNames);

                var principalMemberNames = GetOrCreateMemberNames(memberNamesByTable, principalKey);
                var collectionBase = Pluralize(dependentEntity);
                if (hasMultipleRelationshipsToSamePrincipal
                    || principalMemberNames.Contains(collectionBase))
                {
                    collectionBase = Pluralize(dependentEntity) + "By" + foreignKeyProperty;
                }

                var collectionName = MakeUnique(collectionBase, principalMemberNames);

                relationships.Add(new ScaffoldRelationship(
                    dependentKey,
                    principalKey,
                    dependentEntity,
                    principalEntity,
                    foreignKeyProperty,
                    principalKeyProperty,
                    referenceName,
                    collectionName,
                    string.Equals(foreignKey.OnDelete, "CASCADE", StringComparison.OrdinalIgnoreCase)));
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
                var existingNames = CreateReservedMemberNameSet();
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

        private static Dictionary<string, HashSet<string>> BuildMemberNameMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var (tableKey, columns) in columnPropertiesByTable)
            {
                var names = CreateReservedMemberNameSet();
                foreach (var column in columns.Values)
                    names.Add(column);
                result[tableKey] = names;
            }

            return result;
        }

        private static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
        {
            var kindSet = kinds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var feature in features)
            {
                if (!kindSet.Contains(feature.Kind)
                    || string.IsNullOrWhiteSpace(feature.Name))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var names))
                {
                    names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = names;
                }

                names.Add(feature.Name);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlySet<string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static HashSet<string> GetOrCreateMemberNames(
            Dictionary<string, HashSet<string>> memberNamesByTable,
            string tableKey)
        {
            if (!memberNamesByTable.TryGetValue(tableKey, out var names))
            {
                names = CreateReservedMemberNameSet();
                memberNamesByTable[tableKey] = names;
            }

            return names;
        }

        private static HashSet<string> CreateReservedMemberNameSet()
            => typeof(object)
                .GetMembers(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                .Select(member => member.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var result = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {EscapeQualified(provider, table.Schema, table.Name)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var keyColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (DataRow row in schema.Rows)
                {
                    if (row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool isKey && isKey)
                        keyColumns.Add(row["ColumnName"]!.ToString()!);
                }

                result[TableKey(table.Schema, table.Name)] = keyColumns;
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
            => ScaffoldContextWithRelationships(namespaceName, contextName, entities, Array.Empty<ScaffoldRelationship>(), Array.Empty<ScaffoldManyToManyJoin>());

        private static string ScaffoldContextWithRelationships(
            string namespaceName,
            string contextName,
            IEnumerable<string> entities,
            IReadOnlyList<ScaffoldRelationship> relationships,
            IReadOnlyList<ScaffoldManyToManyJoin> manyToManyJoins)
        {
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                sb.AppendLine("using System.Data.Common;");
                sb.AppendLine("using System.Linq;");
                if (relationships.Count > 0 || manyToManyJoins.Count > 0)
                    sb.AppendLine("using System;");
                sb.AppendLine("using nORM.Core;");
                sb.AppendLine("using nORM.Configuration;");
                sb.AppendLine("using nORM.Providers;");
                sb.AppendLine();
                sb.AppendLine($"namespace {namespaceName};");
                sb.AppendLine();
                sb.AppendLine($"public class {EscapeCSharpIdentifier(contextName)} : DbContext");
                sb.AppendLine("{");
                if (relationships.Count == 0 && manyToManyJoins.Count == 0)
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
                    sb.AppendLine($"    public IQueryable<{safeEntity}> {queryProperty} => this.Query<{safeEntity}>();");
                }

                if (relationships.Count > 0 || manyToManyJoins.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("    private static DbContextOptions ConfigureOptions(DbContextOptions? options)");
                    sb.AppendLine("    {");
                    sb.AppendLine("        var configuredOptions = options?.Clone() ?? new DbContextOptions();");
                    sb.AppendLine("        var configure = configuredOptions.OnModelCreating;");
                    sb.AppendLine("        configuredOptions.OnModelCreating = mb =>");
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
                        var cascadeSuffix = relationship.CascadeDelete ? string.Empty : ", cascadeDelete: false";
                        sb.AppendLine($"                .HasForeignKey(d => d.{foreignKey}, p => p.{principalKey}{cascadeSuffix});");
                    }
                    foreach (var join in manyToManyJoins.OrderBy(j => j.LeftEntityName, StringComparer.Ordinal).ThenBy(j => j.RightEntityName, StringComparer.Ordinal))
                    {
                        var left = EscapeCSharpIdentifier(join.LeftEntityName);
                        var right = EscapeCSharpIdentifier(join.RightEntityName);
                        var collection = EscapeCSharpIdentifier(join.LeftCollectionNavigationName);
                        var inverseCollection = EscapeCSharpIdentifier(join.RightCollectionNavigationName);
                        var joinTable = EscapeStringLiteral(join.JoinTableName);
                        var leftFk = EscapeStringLiteral(join.LeftForeignKeyColumn);
                        var rightFk = EscapeStringLiteral(join.RightForeignKeyColumn);
                        sb.AppendLine($"            mb.Entity<{left}>()");
                        sb.AppendLine($"                .HasMany<{right}>(p => p.{collection})");
                        sb.AppendLine($"                .WithMany(p => p.{inverseCollection})");
                        sb.AppendLine($"                .UsingTable(\"{joinTable}\", \"{leftFk}\", \"{rightFk}\");");
                    }
                    sb.AppendLine("        };");
                    sb.AppendLine("        return configuredOptions;");
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
                var t when t == typeof(sbyte) => "sbyte",
                var t when t == typeof(uint) => "uint",
                var t when t == typeof(ulong) => "ulong",
                var t when t == typeof(ushort) => "ushort",
                var t when t == typeof(bool) => "bool",
                var t when t == typeof(char) => "char",
                var t when t == typeof(string) => "string",
                var t when t == typeof(DateTime) => "DateTime",
                var t when t == typeof(DateOnly) => "DateOnly",
                var t when t == typeof(DateTimeOffset) => "DateTimeOffset",
                var t when t == typeof(TimeOnly) => "TimeOnly",
                var t when t == typeof(TimeSpan) => "TimeSpan",
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
            => string.IsNullOrEmpty(schema)
                ? IdentifierEscaping.EscapeSingle(provider, table)
                : $"{provider.Escape(schema!)}.{IdentifierEscaping.EscapeSingle(provider, table)}";

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

        private static bool IsValidNamespaceName(string namespaceName)
        {
            if (string.IsNullOrWhiteSpace(namespaceName))
                return false;

            foreach (var segment in namespaceName.Split('.'))
            {
                if (!IsValidNamespaceSegment(segment))
                    return false;
            }

            return true;
        }

        private static bool IsValidNamespaceSegment(string segment)
        {
            if (string.IsNullOrWhiteSpace(segment))
                return false;

            var start = segment[0] == '@' ? 1 : 0;
            if (start == segment.Length)
                return false;

            if (!(char.IsLetter(segment[start]) || segment[start] == '_'))
                return false;

            for (var i = start + 1; i < segment.Length; i++)
            {
                if (!(char.IsLetterOrDigit(segment[i]) || segment[i] == '_'))
                    return false;
            }

            if (start == 0 && _csharpKeywords.Contains(segment))
                return false;

            return true;
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

        private static string MakeUniqueContextName(string contextName, IEnumerable<string> entityNames)
        {
            var existingNames = new HashSet<string>(entityNames, StringComparer.OrdinalIgnoreCase);
            if (!existingNames.Contains(contextName))
                return contextName;

            var preferred = contextName.EndsWith("Context", StringComparison.Ordinal)
                ? contextName
                : contextName + "Context";
            return MakeUnique(preferred, existingNames);
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

        private readonly record struct ScaffoldSkippedObject(
            string? Schema,
            string Name,
            string Kind,
            string Detail);

        private readonly record struct ScaffoldForeignKey(
            string? DependentSchema,
            string DependentTable,
            string DependentColumn,
            string? PrincipalSchema,
            string PrincipalTable,
            string PrincipalColumn,
            string ConstraintName,
            int ColumnCount,
            string OnDelete = "NO ACTION",
            string OnUpdate = "NO ACTION");

        private readonly record struct ScaffoldIndex(
            string TableKey,
            string ColumnName,
            string IndexName,
            bool IsUnique,
            int ColumnCount,
            int Ordinal);

        private readonly record struct ScaffoldRelationship(
            string DependentTableKey,
            string PrincipalTableKey,
            string DependentEntityName,
            string PrincipalEntityName,
            string ForeignKeyPropertyName,
            string PrincipalKeyPropertyName,
            string ReferenceNavigationName,
            string CollectionNavigationName,
            bool CascadeDelete);

        private readonly record struct ScaffoldManyToManyJoin(
            string JoinTableKey,
            string LeftTableKey,
            string RightTableKey,
            string JoinTableName,
            string LeftEntityName,
            string RightEntityName,
            string LeftForeignKeyColumn,
            string RightForeignKeyColumn,
            string LeftCollectionNavigationName,
            string RightCollectionNavigationName);

        private readonly record struct ScaffoldManyToManyNavigation(
            string TargetEntityName,
            string CollectionNavigationName);

        private readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail);
    }
}
