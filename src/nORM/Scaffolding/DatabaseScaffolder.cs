#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Migration;
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
                if (!options.DryRun)
                    Directory.CreateDirectory(outputDirectory);
                var discoveredTables = await GetTablesAsync(connection, provider).ConfigureAwait(false);
                var discoveredSkippedObjects = await GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
                var emitQueryArtifacts = options.EmitViewEntities || options.EmitQueryArtifacts;
                var emittedViewObjects = emitQueryArtifacts
                    ? discoveredSkippedObjects.Where(IsQueryArtifactObject).ToArray()
                    : Array.Empty<ScaffoldSkippedObject>();
                var queryArtifactTableKeys = emittedViewObjects
                    .Select(obj => TableKey(obj.Schema, obj.Name))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var discoveredTablesAndViews = discoveredTables
                    .Concat(emittedViewObjects.Select(obj => new ScaffoldTable(obj.Name, obj.Schema)))
                    .ToArray();
                var tables = FilterTables(discoveredTablesAndViews, discoveredSkippedObjects, options);
                EnsureNoTableKeyCollisions(tables);
                var skippedObjects = FilterSkippedObjects(
                    discoveredSkippedObjects.Where(obj => !emittedViewObjects.Contains(obj)).ToArray(),
                    options,
                    emittedViewObjects);
                var entityNames = new List<string>();
                var entityByTable = BuildEntityNameMap(tables);
                safeContextName = MakeUniqueContextName(safeContextName, entityByTable.Values);
                var columnPropertiesByTable = await GetColumnPropertyNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var memberNamesByTable = BuildMemberNameMap(columnPropertiesByTable);
                var primaryKeyColumnsByTable = await GetPrimaryKeyColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var nonNullableColumnsByTable = await GetNonNullableColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var sqliteDeclaredTypesByTable = provider is SqliteProvider
                    ? await GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false)
                    : new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
                var identityColumnsByTable = await GetIdentityColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var indexes = await GetIndexesAsync(connection, provider, tables).ConfigureAwait(false);
                var foreignKeys = await GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);
                var unsupportedFeatures = (await GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
                RemoveSupportedDescendingIndexDiagnostics(unsupportedFeatures, indexes);
                RemoveSupportedIncludedColumnIndexDiagnostics(unsupportedFeatures, indexes);
                RemoveSupportedPartialIndexDiagnostics(unsupportedFeatures, indexes);
                AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable);
                AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
                AddRelationshipPrincipalKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable, indexes);
                var providerSpecificColumnTypesByTable = BuildFeatureDetailMap(unsupportedFeatures, "ProviderSpecificColumnType");
                RemoveSupportedProviderSpecificColumnTypeDiagnostics(unsupportedFeatures, columnPropertiesByTable);
                var defaultValuesByTable = BuildScaffoldDefaultValueMap(unsupportedFeatures, columnPropertiesByTable);
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                    && defaultValuesByTable.TryGetValue(feature.TableKey, out var defaults)
                    && defaults.ContainsKey(feature.Name));
                var checkConstraints = BuildCheckConstraintConfigurations(entityByTable, unsupportedFeatures);
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                    && checkConstraints.Any(check =>
                        string.Equals(check.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(check.Name, feature.Name, StringComparison.OrdinalIgnoreCase)));
                var expressionIndexConfigurations = BuildExpressionIndexConfigurations(entityByTable, unsupportedFeatures);
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                    && expressionIndexConfigurations.Any(index =>
                        string.Equals(index.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(index.Name, feature.Name, StringComparison.OrdinalIgnoreCase)));
                var collationConfigurations = BuildCollationConfigurations(entityByTable, columnPropertiesByTable, unsupportedFeatures);
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "Collation", StringComparison.OrdinalIgnoreCase)
                    && collationConfigurations.Any(collation =>
                        string.Equals(collation.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(collation.ColumnName, feature.Name, StringComparison.OrdinalIgnoreCase)));
                var computedColumnConfigurations = BuildComputedColumnConfigurations(entityByTable, columnPropertiesByTable, unsupportedFeatures);
                var computedColumnsByTable = BuildFeatureNameMap(unsupportedFeatures, "Computed", "RowVersion");
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "Computed", StringComparison.OrdinalIgnoreCase)
                    && computedColumnConfigurations.Any(computed =>
                        string.Equals(computed.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(computed.ColumnName, feature.Name, StringComparison.OrdinalIgnoreCase)));
                var decimalPrecisionByTable = BuildDecimalPrecisionMap(unsupportedFeatures);
                unsupportedFeatures.RemoveAll(static feature =>
                    string.Equals(feature.Kind, "PrecisionScale", StringComparison.OrdinalIgnoreCase)
                    && TryParseDecimalPrecision(feature.Detail, out _, out _));
                var rowVersionColumnsByTable = BuildFeatureNameMap(unsupportedFeatures, "RowVersion");
                var providerNativeTemporalHistoryTableKeys = BuildProviderNativeTemporalHistoryTableKeys(unsupportedFeatures);
                var manyToManyJoins = BuildManyToManyJoins(foreignKeys, tables, entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, identityColumnsByTable, indexes, nonNullableColumnsByTable, memberNamesByTable);
                var manyToManyJoinTableKeys = manyToManyJoins.Select(j => j.JoinTableKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var relationships = BuildRelationships(
                    foreignKeys.Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))).ToArray(),
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    indexes,
                    memberNamesByTable);
                var compositePrimaryKeys = BuildCompositePrimaryKeys(entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, manyToManyJoinTableKeys);
                var defaultValueConfigurations = BuildDefaultValueConfigurations(entityByTable, columnPropertiesByTable, defaultValuesByTable);
                var identityOptionConfigurations = BuildIdentityOptionConfigurations(entityByTable, columnPropertiesByTable, unsupportedFeatures);
                unsupportedFeatures.RemoveAll(feature =>
                    string.Equals(feature.Kind, "IdentityStrategy", StringComparison.OrdinalIgnoreCase)
                    && identityOptionConfigurations.Any(identity =>
                        string.Equals(identity.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(identity.ColumnName, feature.Name, StringComparison.OrdinalIgnoreCase)));
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
                    identityColumnsByTable.TryGetValue(tableKey, out var identityColumns);
                    decimalPrecisionByTable.TryGetValue(tableKey, out var decimalPrecisions);
                    sqliteDeclaredTypesByTable.TryGetValue(tableKey, out var sqliteDeclaredTypes);
                    providerSpecificColumnTypesByTable.TryGetValue(tableKey, out var providerSpecificColumnTypes);
                    var isReadOnlyEntity = queryArtifactTableKeys.Contains(tableKey)
                        || providerNativeTemporalHistoryTableKeys.Contains(tableKey)
                        || !primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
                        || primaryKeyColumns.Count == 0;
                    var entityCode = await ScaffoldEntityAsync(connection, provider, schemaName, tableName, entityName, namespaceName, columnPropertyNames, tableIndexes, references, collections, manyToManyCollections, computedColumns, rowVersionColumns, identityColumns, decimalPrecisions, isReadOnlyEntity, sqliteDeclaredTypes, providerSpecificColumnTypes).ConfigureAwait(false);
                    generatedFiles.Add((Path.Combine(outputDirectory, entityName + ".cs"), entityCode));
                }

                var routineStubs = options.EmitRoutineStubs
                    ? skippedObjects.Where(obj => string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase)).ToArray()
                    : Array.Empty<ScaffoldSkippedObject>();
                var sequenceStubs = options.EmitSequenceStubs
                    ? skippedObjects.Where(obj => string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase)).ToArray()
                    : Array.Empty<ScaffoldSkippedObject>();
                var ctxCode = ScaffoldContextWithRelationships(namespaceName, safeContextName, entityNames, relationships, manyToManyJoins, routineStubs, compositePrimaryKeys, defaultValueConfigurations, checkConstraints, computedColumnConfigurations, expressionIndexConfigurations, collationConfigurations, sequenceStubs, identityOptionConfigurations);
                generatedFiles.Add((Path.Combine(outputDirectory, safeContextName + ".cs"), ctxCode));
                var diagnostics = ScaffoldDiagnostics(foreignKeys, unsupportedFeatures, skippedObjects, primaryKeyColumnsByTable, indexes, columnPropertiesByTable, nonNullableColumnsByTable, manyToManyJoinTableKeys);
                if (!string.IsNullOrWhiteSpace(diagnostics))
                {
                    generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"), diagnostics));
                    generatedFiles.Add((Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json"), ScaffoldDiagnosticsJson(foreignKeys, unsupportedFeatures, skippedObjects, primaryKeyColumnsByTable, indexes, columnPropertiesByTable, nonNullableColumnsByTable, manyToManyJoinTableKeys)));
                }
                else
                {
                    if (!options.DryRun)
                        EnsureNoStaleScaffoldWarningReports(outputDirectory, options);
                }

                if (!options.DryRun)
                {
                    EnsureNoOutputFileConflicts(generatedFiles.Select(file => file.Path), options);
                    foreach (var (path, content) in generatedFiles)
                        await WriteGeneratedFileAsync(path, content).ConfigureAwait(false);
                }

                if (!string.IsNullOrWhiteSpace(diagnostics))
                {
                    if (options.FailOnWarnings)
                        throw new NormConfigurationException(
                            "Scaffolding produced warnings for schema features that cannot be emitted as runnable nORM model code. " +
                            (options.DryRun
                                ? "Rerun without ScaffoldOptions.DryRun to write nORM.ScaffoldWarnings.md, or disable ScaffoldOptions.FailOnWarnings."
                                : "Review nORM.ScaffoldWarnings.md or disable ScaffoldOptions.FailOnWarnings."));
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
        /// <param name="identityColumns">Column names known to be database-generated identity/auto-increment values.</param>
        /// <param name="decimalPrecisions">Decimal precision/scale metadata keyed by database column name.</param>
        /// <param name="isReadOnlyEntity">Whether the generated type should reject nORM write operations.</param>
        /// <param name="sqliteDeclaredTypes">SQLite declared column type names keyed by database column name.</param>
        /// <param name="providerSpecificColumnTypes">Provider-specific column type details keyed by database column name.</param>
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
            IReadOnlySet<string>? rowVersionColumns = null,
            IReadOnlySet<string>? identityColumns = null,
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions = null,
            bool isReadOnlyEntity = false,
            IReadOnlyDictionary<string, string>? sqliteDeclaredTypes = null,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes = null)
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
                if ((indexes?.Count > 0) || isReadOnlyEntity)
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
                if (isReadOnlyEntity)
                    sb.AppendLine("[ReadOnlyEntity]");
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
                    var allowNull = row["AllowDBNull"] is bool b && b;

                    var isKey = row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool key && key;
                    var isAuto = (row.Table.Columns.Contains("IsAutoIncrement") && row["IsAutoIncrement"] is bool ai && ai)
                        || identityColumns?.Contains(colName) == true;
                    var isComputed = computedColumns?.Contains(colName) == true;
                    var isRowVersion = rowVersionColumns?.Contains(colName) == true;
                    var effectiveAllowNull = allowNull && !isKey;
                    string? declaredType = null;
                    sqliteDeclaredTypes?.TryGetValue(colName, out declaredType);
                    string? providerSpecificType = null;
                    providerSpecificColumnTypes?.TryGetValue(colName, out providerSpecificType);
                    var clrType = NormalizeScaffoldClrType(provider, (Type)row["DataType"]!, effectiveAllowNull, isKey, isAuto, declaredType, providerSpecificType);

                    // Decide C# type name with correct nullability for value OR reference types
                    var typeName = GetTypeName(clrType, effectiveAllowNull);

                    var maxLength = GetScaffoldMaxLength(clrType, row);

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
                    if (!clrType.IsValueType && !effectiveAllowNull)
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
                        var orderSuffix = index.ColumnCount > 1 && !index.IsIncluded ? $", Order = {index.Ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture)}" : string.Empty;
                        var descendingSuffix = index.IsDescending ? ", IsDescending = true" : string.Empty;
                        var includedSuffix = index.IsIncluded ? ", IsIncluded = true" : string.Empty;
                        var filterSuffix = string.IsNullOrWhiteSpace(index.FilterSql) ? string.Empty : $", FilterSql = \"{EscapeStringLiteral(index.FilterSql)}\"";
                        sb.AppendLine($"    [Index(\"{safeIndexName}\"{uniqueSuffix}{orderSuffix}{descendingSuffix}{includedSuffix}{filterSuffix})]");
                    }
                    if (clrType == typeof(decimal)
                        && decimalPrecisions is not null
                        && decimalPrecisions.TryGetValue(colName, out var decimalPrecision))
                    {
                        sb.AppendLine($"    [Column(\"{EscapeStringLiteral(colName)}\", TypeName = \"decimal({decimalPrecision.Precision.ToString(System.Globalization.CultureInfo.InvariantCulture)},{decimalPrecision.Scale.ToString(System.Globalization.CultureInfo.InvariantCulture)})\")]");
                    }
                    else
                    {
                        sb.AppendLine($"    [Column(\"{EscapeStringLiteral(colName)}\")]");
                    }
                    var initializer = !clrType.IsValueType && !effectiveAllowNull ? " = default!;" : string.Empty;
                    sb.AppendLine($"    public {typeName} {propName} {{ get; set; }}{initializer}");
                    sb.AppendLine();
                }

                foreach (var reference in (references ?? Array.Empty<ScaffoldRelationship>())
                    .OrderBy(r => r.ReferenceNavigationName, StringComparer.Ordinal)
                    .ThenBy(r => r.ForeignKeyPropertyName, StringComparer.Ordinal))
                {
                    if (!reference.IsComposite)
                        sb.AppendLine($"    [ForeignKey(nameof({EscapeCSharpIdentifier(reference.ForeignKeyPropertyName)}))]");
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(reference.PrincipalEntityName)}? {EscapeCSharpIdentifier(reference.ReferenceNavigationName)} {{ get; set; }}");
                    sb.AppendLine();
                }

                foreach (var collection in (collections ?? Array.Empty<ScaffoldRelationship>())
                    .OrderBy(r => r.CollectionNavigationName, StringComparer.Ordinal)
                    .ThenBy(r => r.ForeignKeyPropertyName, StringComparer.Ordinal))
                {
                    sb.AppendLine($"    public List<{EscapeCSharpIdentifier(collection.DependentEntityName)}> {EscapeCSharpIdentifier(collection.CollectionNavigationName)} {{ get; set; }} = new();");
                    sb.AppendLine();
                }

                foreach (var collection in (manyToManyCollections ?? Array.Empty<ScaffoldManyToManyNavigation>())
                    .OrderBy(n => n.CollectionNavigationName, StringComparer.Ordinal)
                    .ThenBy(n => n.TargetEntityName, StringComparer.Ordinal))
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
                        $"""
                        SELECT {SqliteSchemaResult(schema)} AS TABLE_SCHEMA, m.name AS TABLE_NAME
                        FROM {provider.Escape(schema)}.sqlite_master m
                        WHERE m.type = 'table'
                          AND m.name NOT LIKE 'sqlite_%'
                          AND UPPER(COALESCE(m.sql, '')) NOT LIKE 'CREATE VIRTUAL TABLE%'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {provider.Escape(schema)}.sqlite_master vt
                              WHERE vt.type = 'table'
                                AND UPPER(COALESCE(vt.sql, '')) LIKE 'CREATE VIRTUAL TABLE%'
                                AND m.name IN (
                                    vt.name || '_data',
                                    vt.name || '_idx',
                                    vt.name || '_content',
                                    vt.name || '_docsize',
                                    vt.name || '_config',
                                    vt.name || '_segments',
                                    vt.name || '_segdir',
                                    vt.name || '_stat',
                                    vt.name || '_node',
                                    vt.name || '_parent',
                                    vt.name || '_rowid'
                                )
                          )
                        ORDER BY m.name
                        """).ConfigureAwait(false));
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
                        $"""
                        SELECT {SqliteSchemaResult(schema)} AS ObjectSchema, name AS ObjectName, 'View' AS Kind, 'SQLite view' AS Detail
                        FROM {provider.Escape(schema)}.sqlite_master
                        WHERE type = 'view'
                        UNION ALL
                        SELECT {SqliteSchemaResult(schema)}, name, 'VirtualTable', 'SQLite virtual table'
                        FROM {provider.Escape(schema)}.sqlite_master
                        WHERE type = 'table' AND UPPER(sql) LIKE 'CREATE VIRTUAL TABLE%'
                        UNION ALL
                        SELECT {SqliteSchemaResult(schema)}, m.name, 'VirtualTableShadow', 'SQLite virtual table shadow table'
                        FROM {provider.Escape(schema)}.sqlite_master m
                        WHERE m.type = 'table'
                          AND m.name NOT LIKE 'sqlite_%'
                          AND UPPER(COALESCE(m.sql, '')) NOT LIKE 'CREATE VIRTUAL TABLE%'
                          AND EXISTS (
                              SELECT 1
                              FROM {provider.Escape(schema)}.sqlite_master vt
                              WHERE vt.type = 'table'
                                AND UPPER(COALESCE(vt.sql, '')) LIKE 'CREATE VIRTUAL TABLE%'
                                AND m.name IN (
                                    vt.name || '_data',
                                    vt.name || '_idx',
                                    vt.name || '_content',
                                    vt.name || '_docsize',
                                    vt.name || '_config',
                                    vt.name || '_segments',
                                    vt.name || '_segdir',
                                    vt.name || '_stat',
                                    vt.name || '_node',
                                    vt.name || '_parent',
                                    vt.name || '_rowid'
                                )
                          )
                        ORDER BY ObjectName
                        """).ConfigureAwait(false));
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
                    SELECT SCHEMA_NAME(p.schema_id), p.name, 'Routine',
                           CONCAT('SQL Server stored procedure; parameters=',
                                  (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = p.object_id),
                                  '; outputParameters=',
                                  (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = p.object_id AND pa.is_output = 1),
                                  '; parameterModes=',
                                  COALESCE((
                                      SELECT STRING_AGG(CONCAT(
                                          pa.name, ':', CASE WHEN pa.is_output = 1 THEN 'INOUT' ELSE 'IN' END, ':',
                                          CASE
                                              WHEN ty.is_table_type = 1 THEN CONCAT('table type (', SCHEMA_NAME(ty.schema_id), '.', ty.name, ')')
                                              ELSE COALESCE(base_ty.name, ty.name)
                                          END,
                                          CASE
                                              WHEN ty.is_table_type = 1 THEN ''
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length) END, ')')
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length / 2) END, ')')
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric') THEN CONCAT('(', pa.precision, ',', pa.scale, ')')
                                              ELSE ''
                                          END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                                      FROM sys.parameters pa
                                      INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                      LEFT JOIN sys.types base_ty
                                        ON ty.is_user_defined = 1
                                       AND ty.is_table_type = 0
                                       AND base_ty.user_type_id = ty.system_type_id
                                       AND base_ty.is_user_defined = 0
                                      WHERE pa.object_id = p.object_id
                                  ), ''))
                    FROM sys.procedures p
                    WHERE p.is_ms_shipped = 0
                    UNION ALL
                    SELECT SCHEMA_NAME(o.schema_id), o.name, 'Routine',
                           CONCAT('SQL Server ',
                                  CASE
                                      WHEN o.type IN ('IF', 'TF') THEN 'table-valued function'
                                      ELSE 'scalar function'
                                  END,
                                  '; parameters=',
                                  (SELECT COUNT(*) FROM sys.parameters pa WHERE pa.object_id = o.object_id AND pa.parameter_id > 0),
                                  '; outputParameters=',
                                  CASE WHEN o.type = 'FN' THEN 1 ELSE 0 END,
                                  '; callShape=',
                                  CASE
                                      WHEN o.type IN ('IF', 'TF') THEN 'table-valued-function'
                                      ELSE 'scalar-function'
                                  END,
                                  '; parameterModes=',
                                  COALESCE((
                                      SELECT STRING_AGG(CONCAT(
                                          pa.name, ':',
                                          CASE WHEN pa.parameter_id = 0 THEN 'RETURN' WHEN pa.is_output = 1 THEN 'INOUT' ELSE 'IN' END,
                                          ':',
                                          CASE
                                              WHEN ty.is_table_type = 1 THEN CONCAT('table type (', SCHEMA_NAME(ty.schema_id), '.', ty.name, ')')
                                              ELSE COALESCE(base_ty.name, ty.name)
                                          END,
                                          CASE
                                              WHEN ty.is_table_type = 1 THEN ''
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('varchar', 'char', 'varbinary', 'binary') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length) END, ')')
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('nvarchar', 'nchar') THEN CONCAT('(', CASE WHEN pa.max_length = -1 THEN 'max' ELSE CONVERT(varchar(11), pa.max_length / 2) END, ')')
                                              WHEN COALESCE(base_ty.name, ty.name) IN ('decimal', 'numeric') THEN CONCAT('(', pa.precision, ',', pa.scale, ')')
                                              ELSE ''
                                          END), ',') WITHIN GROUP (ORDER BY pa.parameter_id)
                                      FROM sys.parameters pa
                                      INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                      LEFT JOIN sys.types base_ty
                                        ON ty.is_user_defined = 1
                                       AND ty.is_table_type = 0
                                       AND base_ty.user_type_id = ty.system_type_id
                                       AND base_ty.is_user_defined = 0
                                      WHERE pa.object_id = o.object_id
                                  ), ''),
                                  '; dataType=',
                                  COALESCE((
                                      SELECT TOP (1) ty.name
                                      FROM sys.parameters pa
                                      INNER JOIN sys.types ty ON pa.user_type_id = ty.user_type_id
                                      WHERE pa.object_id = o.object_id
                                        AND pa.parameter_id = 0
                                  ), CASE WHEN o.type IN ('IF', 'TF') THEN 'TABLE' ELSE '' END))
                    FROM sys.objects o
                    WHERE o.is_ms_shipped = 0
                      AND o.type IN ('FN', 'IF', 'TF')
                    UNION ALL
                    SELECT SCHEMA_NAME(s.schema_id), s.name, 'Sequence',
                           CONCAT('SQL Server sequence; dataType=', ty.name,
                                  CASE
                                      WHEN ty.name IN ('decimal', 'numeric') THEN CONCAT('(', s.precision, ',', s.scale, ')')
                                      ELSE ''
                                  END)
                    FROM sys.sequences s
                    INNER JOIN sys.types ty ON ty.user_type_id = s.user_type_id
                    UNION ALL
                    SELECT SCHEMA_NAME(s.schema_id), s.name, 'Synonym',
                           CONCAT('SQL Server synonym; baseObject=', s.base_object_name,
                                  '; baseType=', COALESCE(CONVERT(nvarchar(20), OBJECTPROPERTYEX(OBJECT_ID(s.base_object_name), 'BaseType')), ''))
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
                    SELECT sequence_schema, sequence_name, 'Sequence', 'PostgreSQL sequence; dataType=' || data_type
                    FROM information_schema.sequences seq
                    WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pg_class sequence_class
                          INNER JOIN pg_namespace sequence_schema_ns ON sequence_schema_ns.oid = sequence_class.relnamespace
                          INNER JOIN pg_depend dependency ON dependency.objid = sequence_class.oid
                          WHERE sequence_class.relkind = 'S'
                            AND sequence_schema_ns.nspname = seq.sequence_schema
                            AND sequence_class.relname = seq.sequence_name
                            AND dependency.deptype IN ('a', 'i')
                      )
                    UNION ALL
                    SELECT schemaname, matviewname, 'MaterializedView', 'PostgreSQL materialized view'
                    FROM pg_matviews
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT r.routine_schema, r.routine_name, 'Routine',
                           'PostgreSQL ' || LOWER(r.routine_type) || '; parameters=' ||
                           COALESCE((
                               SELECT COUNT(*)
                               FROM information_schema.parameters p
                               WHERE p.specific_schema = r.specific_schema
                                 AND p.specific_name = r.specific_name
                           ), 0)::text ||
                           '; outputParameters=' ||
                           COALESCE((
                               SELECT COUNT(*)
                               FROM information_schema.parameters p
                               WHERE p.specific_schema = r.specific_schema
                                 AND p.specific_name = r.specific_name
                                 AND p.parameter_mode IN ('OUT', 'INOUT')
                           ), 0)::text ||
                           '; parameterModes=' ||
                           COALESCE((
                               SELECT string_agg(
                                   COALESCE(p.parameter_name, 'return') || ':' || COALESCE(p.parameter_mode, 'RETURN') || ':' ||
                                   CASE
                                       WHEN p.data_type IN ('ARRAY', 'USER-DEFINED')
                                            AND p.udt_name IS NOT NULL
                                            AND p.udt_name <> ''
                                       THEN p.data_type || ' (' || p.udt_name || ')'
                                       ELSE COALESCE(p.data_type, '')
                                   END ||
                                   CASE
                                       WHEN p.character_maximum_length IS NOT NULL THEN '(' || p.character_maximum_length::text || ')'
                                       WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN '(' || p.numeric_precision::text || ',' || p.numeric_scale::text || ')'
                                       ELSE ''
                                   END,
                                   ',' ORDER BY p.ordinal_position)
                               FROM information_schema.parameters p
                               WHERE p.specific_schema = r.specific_schema
                                 AND p.specific_name = r.specific_name
                           ), '') ||
                           '; callShape=' ||
                           CASE
                               WHEN UPPER(r.routine_type) = 'FUNCTION' AND EXISTS (
                                   SELECT 1
                                   FROM pg_proc routine_proc
                                   INNER JOIN pg_namespace routine_ns ON routine_ns.oid = routine_proc.pronamespace
                                   WHERE routine_ns.nspname = r.specific_schema
                                     AND routine_proc.proname = r.routine_name
                                     AND routine_proc.proretset
                               ) THEN 'table-valued-function'
                               WHEN UPPER(r.routine_type) = 'FUNCTION' AND LOWER(COALESCE(r.data_type, '')) IN ('record', 'table') THEN 'table-valued-function'
                               WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                               ELSE ''
                           END ||
                           '; dataType=' || COALESCE(r.data_type, '')
                    FROM information_schema.routines r
                    WHERE r.routine_schema NOT IN ('pg_catalog', 'information_schema')
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
                    SELECT NULL, r.routine_name, 'Routine',
                           CONCAT('MySQL ', r.routine_type, '; parameters=',
                                  (SELECT COUNT(*)
                                   FROM information_schema.parameters p
                                   WHERE p.specific_schema = r.routine_schema
                                     AND p.specific_name = r.specific_name
                                     AND p.parameter_mode IS NOT NULL),
                                  '; outputParameters=',
                                  (SELECT COUNT(*)
                                   FROM information_schema.parameters p
                                   WHERE p.specific_schema = r.routine_schema
                                     AND p.specific_name = r.specific_name
                                     AND p.parameter_mode IN ('OUT', 'INOUT')),
                                  '; parameterModes=',
                                  COALESCE((SELECT GROUP_CONCAT(CONCAT(
                                                COALESCE(p.parameter_name, 'return'), ':', COALESCE(p.parameter_mode, 'RETURN'), ':',
                                                CASE
                                                    WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN COALESCE(p.dtd_identifier, p.data_type, '')
                                                    ELSE COALESCE(p.data_type, '')
                                                END,
                                                CASE
                                                    WHEN LOWER(COALESCE(p.dtd_identifier, '')) LIKE '%unsigned%' THEN ''
                                                    WHEN p.character_maximum_length IS NOT NULL THEN CONCAT('(', p.character_maximum_length, ')')
                                                    WHEN p.numeric_precision IS NOT NULL AND p.numeric_scale IS NOT NULL THEN CONCAT('(', p.numeric_precision, ',', p.numeric_scale, ')')
                                                    ELSE ''
                                                END) ORDER BY p.ordinal_position SEPARATOR ',')
                                            FROM information_schema.parameters p
                                            WHERE p.specific_schema = r.routine_schema
                                              AND p.specific_name = r.specific_name
                                              AND p.parameter_mode IS NOT NULL), ''),
                                  '; callShape=',
                                  CASE
                                      WHEN UPPER(r.routine_type) = 'FUNCTION' THEN 'scalar-function'
                                      ELSE ''
                                  END,
                                  '; dataType=', COALESCE(r.data_type, ''))
                    FROM information_schema.routines r
                    WHERE r.routine_schema = DATABASE()
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

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetSqliteDeclaredColumnTypesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var result = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var name = Convert.ToString(reader["name"]);
                    var type = Convert.ToString(reader["type"]);
                    if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(type))
                        columns[name!] = type!;
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
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

        private static IReadOnlyList<ScaffoldSkippedObject> FilterSkippedObjects(
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options,
            IReadOnlyList<ScaffoldSkippedObject>? emittedQueryArtifacts = null)
        {
            var requested = GetRequestedTableFilters(options);
            if (requested.Length == 0)
                return skippedObjects;

            var emittedVirtualTables = (emittedQueryArtifacts ?? Array.Empty<ScaffoldSkippedObject>())
                .Where(obj => string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase))
                .ToArray();

            return skippedObjects
                .Where(obj => requested.Any(request => MatchesSkippedObjectFilter(obj, request))
                              || IsShadowOfEmittedVirtualTable(obj, emittedVirtualTables))
                .ToArray();
        }

        private static bool IsShadowOfEmittedVirtualTable(
            ScaffoldSkippedObject obj,
            IReadOnlyList<ScaffoldSkippedObject> emittedVirtualTables)
            => string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase)
               && emittedVirtualTables.Any(vt =>
                   string.Equals(vt.Schema ?? string.Empty, obj.Schema ?? string.Empty, StringComparison.OrdinalIgnoreCase)
                   && obj.Name.StartsWith(vt.Name + "_", StringComparison.OrdinalIgnoreCase));

        private static bool MatchesSkippedObjectFilter(ScaffoldSkippedObject obj, string requested)
            => string.Equals(obj.Name, requested, StringComparison.OrdinalIgnoreCase)
               || string.Equals(TableKey(obj.Schema, obj.Name), requested, StringComparison.OrdinalIgnoreCase);

        private static bool IsQueryArtifactObject(ScaffoldSkippedObject obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase) && IsTableLikeSqlServerSynonym(obj.Detail));

        private static bool IsTableLikeSqlServerSynonym(string detail)
        {
            if (!detail.StartsWith("SQL Server synonym", StringComparison.OrdinalIgnoreCase))
                return false;

            var baseType = ParseSemicolonValue(detail, "baseType");
            return string.Equals(baseType, "U", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(baseType, "V", StringComparison.OrdinalIgnoreCase);
        }

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

        private static void EnsureNoStaleScaffoldWarningReports(string outputDirectory, ScaffoldOptions options)
        {
            var warningPaths = new[]
            {
                Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.md"),
                Path.Combine(outputDirectory, "nORM.ScaffoldWarnings.json")
            };
            var existingWarnings = warningPaths.Where(File.Exists).ToArray();
            if (existingWarnings.Length == 0)
                return;

            if (!options.OverwriteFiles)
            {
                throw new NormConfigurationException(
                    "Scaffolding produced no warnings, but stale scaffold warning report files already exist: " +
                    string.Join(", ", existingWarnings.Select(path => $"'{path}'")) +
                    ". Enable overwrite or remove the stale report files before running with overwrite protection.");
            }

            foreach (var path in existingWarnings)
                File.Delete(path);
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
                        if (string.Equals(origin, "pk", StringComparison.OrdinalIgnoreCase))
                            continue;
                        var filterSql = isPartial
                            ? await GetSqliteIndexFilterSqlAsync(connection, provider, table.Schema, name).ConfigureAwait(false)
                            : null;

                        await using var infoCommand = connection.CreateCommand();
                        infoCommand.CommandText = SqlitePragma(provider, table.Schema, "index_xinfo", name);
                        await using var infoReader = await infoCommand.ExecuteReaderAsync().ConfigureAwait(false);
                        var columns = new List<(int Ordinal, string Name, bool IsDescending)>();
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
                                    columnName,
                                    ReaderHasColumn(infoReader, "desc")
                                        && Convert.ToInt32(infoReader["desc"], System.Globalization.CultureInfo.InvariantCulture) != 0));
                            }
                        }

                        if (hasUnsupportedKeyPart)
                            continue;

                        foreach (var column in columns.OrderBy(static c => c.Ordinal))
                            indexes.Add(new ScaffoldIndex(TableKey(table.Schema, table.Name), column.Name, name, isUnique, columns.Count, column.Ordinal, column.IsDescending, false, filterSql));
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
                        SUM(CASE WHEN ic.is_included_column = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY i.object_id, i.index_id) AS ColumnCount,
                        CASE WHEN ic.is_included_column = 1 THEN 2147483647 ELSE ic.key_ordinal - 1 END AS Ordinal,
                        ic.is_descending_key AS IsDescending,
                        ic.is_included_column AS IsIncluded,
                        i.filter_definition AS FilterSql
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON t.object_id = i.object_id
                    INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
                    WHERE t.is_ms_shipped = 0
                      AND i.is_primary_key = 0
                      AND i.is_hypothetical = 0
                      AND i.name IS NOT NULL
                    ORDER BY SCHEMA_NAME(t.schema_id), t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
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
                        ix.indnkeyatts AS ColumnCount,
                        CASE WHEN key.ord > ix.indnkeyatts THEN 2147483647 ELSE key.ord - 1 END AS Ordinal,
                        CASE WHEN key.ord <= ix.indnkeyatts AND (ix.indoption[key.ord - 1] & 1) = 1 THEN 1 ELSE 0 END AS IsDescending,
                        CASE WHEN key.ord > ix.indnkeyatts THEN 1 ELSE 0 END AS IsIncluded,
                        CASE WHEN ix.indpred IS NULL THEN NULL ELSE pg_get_expr(ix.indpred, ix.indrelid) END AS FilterSql
                    FROM pg_index ix
                    INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
                    INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    INNER JOIN unnest(ix.indkey) WITH ORDINALITY AS key(attnum, ord) ON true
                    INNER JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key.attnum
                    WHERE ix.indisprimary = false
                      AND ix.indexprs IS NULL
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
                        s.seq_in_index - 1 AS Ordinal,
                        CASE WHEN UPPER(COALESCE(s.collation, 'A')) = 'D' THEN 1 ELSE 0 END AS IsDescending
                    FROM information_schema.statistics s
                    WHERE s.table_schema = DATABASE()
                      AND s.index_name <> 'PRIMARY'
                      AND s.sub_part IS NULL
                    ORDER BY s.table_schema, s.table_name, s.index_name, s.seq_in_index
                    """).ConfigureAwait(false);
            }

            return Array.Empty<ScaffoldIndex>();
        }

        private static async Task<string?> GetSqliteIndexFilterSqlAsync(DbConnection connection, DatabaseProvider provider, string? schemaName, string indexName)
        {
            var sql = await GetSqliteIndexSqlAsync(connection, provider, schemaName, indexName).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            var where = sql.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            return where < 0 ? null : sql[(where + 7)..].Trim();
        }

        private static async Task<string?> GetSqliteIndexSqlAsync(DbConnection connection, DatabaseProvider provider, string? schemaName, string indexName)
        {
            await using var cmd = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(schemaName) ? "main" : schemaName!;
            cmd.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'index' AND name = @name";
            var p = cmd.CreateParameter();
            p.ParameterName = "@name";
            p.Value = indexName;
            cmd.Parameters.Add(p);
            return Convert.ToString(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
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
                    Convert.ToInt32(reader["Ordinal"], System.Globalization.CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "IsDescending")
                        && Convert.ToBoolean(reader["IsDescending"], System.Globalization.CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "IsIncluded")
                        && Convert.ToBoolean(reader["IsIncluded"], System.Globalization.CultureInfo.InvariantCulture),
                    ReaderHasColumn(reader, "FilterSql") ? NullIfWhiteSpace(Convert.ToString(reader["FilterSql"])) : null));
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

        private static bool TryParseReferentialAction(string? action, out ReferentialAction referentialAction)
        {
            switch (NormalizeReferentialAction(action))
            {
                case "CASCADE":
                    referentialAction = ReferentialAction.Cascade;
                    return true;
                case "SET NULL":
                    referentialAction = ReferentialAction.SetNull;
                    return true;
                case "RESTRICT":
                    referentialAction = ReferentialAction.Restrict;
                    return true;
                case "SET DEFAULT":
                    referentialAction = ReferentialAction.SetDefault;
                    return true;
                case "NO ACTION":
                    referentialAction = ReferentialAction.NoAction;
                    return true;
                default:
                    referentialAction = ReferentialAction.NoAction;
                    return false;
            }
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
                var sqliteCreateSqlByTable = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    var tableKey = TableKey(table.Schema, table.Name);
                    var createSql = await GetSqliteCreateTableSqlAsync(connection, provider, table).ConfigureAwait(false);
                    sqliteCreateSqlByTable[tableKey] = createSql;
                    var generatedColumns = ExtractSqliteGeneratedColumns(createSql);
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var name = Convert.ToString(reader["name"]) ?? string.Empty;
                        var declaredType = Convert.ToString(reader["type"]);
                        var defaultValue = Convert.ToString(reader["dflt_value"]);
                        var hidden = Convert.ToInt32(reader["hidden"], System.Globalization.CultureInfo.InvariantCulture);
                        if (!string.IsNullOrWhiteSpace(defaultValue))
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "Default", name, defaultValue));
                        if (hidden is 2 or 3)
                        {
                            var detail = generatedColumns.TryGetValue(name, out var generated)
                                ? generated.Sql + (generated.Stored ? " STORED" : " VIRTUAL")
                                : "SQLite generated column";
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "Computed", name, detail));
                        }
                        if (IsSqliteProviderSpecificDeclaredType(declaredType))
                            features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "ProviderSpecificColumnType", name, declaredType!));
                    }
                }

                foreach (var table in tables)
                {
                    sqliteCreateSqlByTable.TryGetValue(TableKey(table.Schema, table.Name), out var createSql);
                    foreach (var check in ExtractSqliteCheckConstraints(table.Name, createSql))
                    {
                        features.Add(new ScaffoldUnsupportedFeature(
                            TableKey(table.Schema, table.Name),
                            "CheckConstraint",
                            check.Name,
                            check.Sql));
                    }

                    foreach (var (columnName, collation) in ExtractSqliteColumnCollations(createSql))
                    {
                        features.Add(new ScaffoldUnsupportedFeature(
                            TableKey(table.Schema, table.Name),
                            "Collation",
                            columnName,
                            collation));
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
                                var indexSql = await GetSqliteIndexSqlAsync(connection, provider, table.Schema, indexName).ConfigureAwait(false);
                                features.Add(new ScaffoldUnsupportedFeature(TableKey(table.Schema, table.Name), "ExpressionIndex", indexName, ExtractCreateIndexExpressionSql(indexSql) ?? "SQLite expression index"));
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
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'Computed',
                        CONCAT(cc.definition, CASE WHEN cc.is_persisted = 1 THEN ' PERSISTED' ELSE '' END)
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
                    SELECT SCHEMA_NAME(t.schema_id), t.name, c.name, 'ProviderSpecificColumnType',
                        CASE
                            WHEN ty.is_user_defined = 1
                            THEN CONCAT(
                                'user-defined type (',
                                SCHEMA_NAME(ty.schema_id),
                                '.',
                                ty.name,
                                CASE WHEN base_ty.name IS NULL THEN '' ELSE CONCAT(' -> ', base_ty.name) END,
                                ')')
                            ELSE ty.name
                        END
                    FROM sys.columns c
                    INNER JOIN sys.tables t ON t.object_id = c.object_id
                    INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
                    LEFT JOIN sys.types base_ty
                      ON ty.is_user_defined = 1
                     AND base_ty.user_type_id = ty.system_type_id
                     AND base_ty.is_user_defined = 0
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
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND column_default IS NOT NULL
                      AND is_identity <> 'YES'
                      AND column_default NOT LIKE 'nextval(%'
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'Computed', generation_expression || ' STORED'
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND is_generated <> 'NEVER'
                    UNION ALL
                    SELECT event_object_schema, event_object_table, trigger_name, 'Trigger', 'PostgreSQL trigger'
                    FROM information_schema.triggers
                    WHERE event_object_schema NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT ns.nspname, tbl.relname, con.conname, 'CheckConstraint', pg_get_constraintdef(con.oid)
                    FROM pg_constraint con
                    INNER JOIN pg_class tbl ON tbl.oid = con.conrelid
                    INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    WHERE con.contype = 'c'
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'Collation', collation_name
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema') AND collation_name IS NOT NULL
                    UNION ALL
                    SELECT table_schema, table_name, column_name, 'ProviderSpecificColumnType',
                        CASE
                            WHEN domain_name IS NOT NULL AND domain_name <> ''
                            THEN 'DOMAIN (' ||
                                 CASE WHEN domain_schema IS NOT NULL AND domain_schema <> '' THEN domain_schema || '.' ELSE '' END ||
                                 domain_name || ' -> ' ||
                                 CASE WHEN udt_name IS NULL OR udt_name = '' THEN data_type ELSE data_type || ' (' || udt_name || ')' END ||
                                 ')'
                            WHEN udt_name IS NULL OR udt_name = '' THEN data_type
                            ELSE data_type || ' (' || udt_name || ')'
                        END
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          domain_name IS NOT NULL
                          OR
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
                    SELECT ns.nspname, tbl.relname, idx.relname, 'ExpressionIndex', pg_get_indexdef(ix.indexrelid)
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
                    SELECT NULL, table_name, column_name, 'Computed',
                        CONCAT(
                            generation_expression,
                            CASE
                                WHEN LOWER(COALESCE(extra, '')) LIKE '%stored generated%' THEN ' STORED'
                                WHEN LOWER(COALESCE(extra, '')) LIKE '%virtual generated%' THEN ' VIRTUAL'
                                ELSE ''
                            END)
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE() AND generation_expression IS NOT NULL AND generation_expression <> ''
                    UNION ALL
                    SELECT NULL, event_object_table, trigger_name, 'Trigger', 'MySQL trigger'
                    FROM information_schema.triggers
                    WHERE trigger_schema = DATABASE()
                    UNION ALL
                    SELECT NULL, tc.table_name, tc.constraint_name, 'CheckConstraint', cc.check_clause
                    FROM information_schema.table_constraints tc
                    INNER JOIN information_schema.check_constraints cc
                        ON cc.constraint_schema = tc.constraint_schema
                       AND cc.constraint_name = tc.constraint_name
                    WHERE tc.table_schema = DATABASE() AND tc.constraint_type = 'CHECK'
                    UNION ALL
                    SELECT NULL, c.table_name, c.column_name, 'Collation', c.collation_name
                    FROM information_schema.columns c
                    INNER JOIN information_schema.schemata s ON s.schema_name = c.table_schema
                    WHERE c.table_schema = DATABASE()
                      AND c.collation_name IS NOT NULL
                      AND c.collation_name <> s.default_collation_name
                    UNION ALL
                    SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType',
                        CASE
                            WHEN data_type IN ('enum', 'set')
                                 AND column_type IS NOT NULL
                                 AND column_type <> ''
                            THEN column_type
                            ELSE data_type
                        END
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
                    SELECT NULL, table_name, column_name, 'ProviderSpecificColumnType', column_type
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND LOWER(COALESCE(column_type, '')) LIKE '%unsigned%'
                      AND data_type IN ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint')
                    UNION ALL
                    SELECT NULL, table_name, column_name, 'PrecisionScale',
                        CONCAT(data_type, '(', numeric_precision, ',', numeric_scale, ')')
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND data_type IN ('decimal', 'numeric')
                      AND numeric_precision IS NOT NULL
                      AND numeric_scale IS NOT NULL
                    UNION ALL
                    SELECT DISTINCT NULL, table_name, index_name, 'DescendingIndex', 'MySQL descending index key'
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND index_name <> 'PRIMARY'
                      AND collation = 'D'
                    UNION ALL
                    SELECT DISTINCT NULL, table_name, index_name, 'PrefixIndex', 'MySQL prefix index'
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND index_name <> 'PRIMARY'
                      AND sub_part IS NOT NULL
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
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
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
                if (TryParseReferentialAction(onDelete, out _)
                    && TryParseReferentialAction(onUpdate, out _))
                    continue;

                features.Add(new ScaffoldUnsupportedFeature(
                    TableKey(fk.DependentSchema, fk.DependentTable),
                    "ReferentialAction",
                    fk.ConstraintName,
                    $"ON DELETE {onDelete}; ON UPDATE {onUpdate}"));
            }
        }

        private static void RemoveSupportedDescendingIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            var supportedDescending = indexes
                .Where(static index => index.IsDescending)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "DescendingIndex", StringComparison.OrdinalIgnoreCase)
                && supportedDescending.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        private static void RemoveSupportedIncludedColumnIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            var supportedIncluded = indexes
                .Where(static index => index.IsIncluded)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "IncludedColumnIndex", StringComparison.OrdinalIgnoreCase)
                && supportedIncluded.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        private static void RemoveSupportedPartialIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            var supportedPartial = indexes
                .Where(static index => !string.IsNullOrWhiteSpace(index.FilterSql))
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "PartialIndex", StringComparison.OrdinalIgnoreCase)
                && supportedPartial.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        private static void AddRelationshipPrincipalKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            foreach (var group in foreignKeys
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase))
            {
                if (ReferencesScaffoldablePrincipalKey(group, primaryKeyColumnsByTable, indexes))
                    continue;

                var fk = group.First();
                var principalKey = TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var principalColumns = string.Join(", ", group.Select(row => row.PrincipalColumn));
                features.Add(new ScaffoldUnsupportedFeature(
                    TableKey(fk.DependentSchema, fk.DependentTable),
                    "RelationshipPrincipalKey",
                    fk.ConstraintName,
                    $"FK references {principalKey}.({principalColumns}), which is neither the generated principal primary key nor an exact ordered unfiltered unique index."));
            }
        }

        private static IReadOnlyList<(string Name, string Sql)> ExtractSqliteCheckConstraints(string tableName, string? createTableSql)
        {
            if (string.IsNullOrWhiteSpace(createTableSql))
                return Array.Empty<(string Name, string Sql)>();

            var result = new List<(string Name, string Sql)>();
            var regex = new System.Text.RegularExpressions.Regex(
                @"(?:(?:CONSTRAINT)\s+(?:""(?<name>[^""]+)""|\[(?<name>[^\]]+)\]|`(?<name>[^`]+)`|(?<name>[A-Za-z_][A-Za-z0-9_]*))\s+)?CHECK\s*\(",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            var ordinal = 0;
            foreach (System.Text.RegularExpressions.Match match in regex.Matches(createTableSql))
            {
                var openIndex = createTableSql.IndexOf('(', match.Index + match.Length - 1);
                if (openIndex < 0)
                    continue;

                var closeIndex = FindMatchingParenthesis(createTableSql, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var name = match.Groups["name"].Success
                    ? match.Groups["name"].Value
                    : $"CK_{ToPascalCase(tableName)}_{++ordinal}";
                var sql = createTableSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                if (!string.IsNullOrWhiteSpace(sql))
                    result.Add((name, sql));
            }

            return result;
        }

        private static IReadOnlyDictionary<string, (string Sql, bool Stored)> ExtractSqliteGeneratedColumns(string? createTableSql)
        {
            var result = new Dictionary<string, (string Sql, bool Stored)>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(createTableSql))
                return result;

            var regex = new System.Text.RegularExpressions.Regex(
                @"(?:""(?<name>[^""]+)""|\[(?<name>[^\]]+)\]|`(?<name>[^`]+)`|(?<name>[A-Za-z_][A-Za-z0-9_]*))(?:(?!,).)*?\bGENERATED\s+ALWAYS\s+AS\s*\(",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant | System.Text.RegularExpressions.RegexOptions.Singleline);

            foreach (System.Text.RegularExpressions.Match match in regex.Matches(createTableSql))
            {
                if (!match.Groups["name"].Success)
                    continue;

                var openIndex = createTableSql.IndexOf('(', match.Index + match.Length - 1);
                if (openIndex < 0)
                    continue;

                var closeIndex = FindMatchingParenthesis(createTableSql, openIndex);
                if (closeIndex <= openIndex)
                    continue;

                var suffixEnd = createTableSql.IndexOf(',', closeIndex + 1);
                if (suffixEnd < 0)
                    suffixEnd = createTableSql.Length;
                var suffix = createTableSql.Substring(closeIndex + 1, suffixEnd - closeIndex - 1);
                var stored = suffix.Contains("STORED", StringComparison.OrdinalIgnoreCase);
                var sql = createTableSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
                if (!string.IsNullOrWhiteSpace(sql))
                    result[match.Groups["name"].Value] = (sql, stored);
            }

            return result;
        }

        private static async Task<string?> GetSqliteCreateTableSqlAsync(DbConnection connection, DatabaseProvider provider, ScaffoldTable table)
        {
            await using var command = connection.CreateCommand();
            var schema = string.IsNullOrWhiteSpace(table.Schema) ? "main" : table.Schema!;
            command.CommandText = $"SELECT sql FROM {provider.Escape(schema)}.sqlite_master WHERE type = 'table' AND name = @tableName";
            var tableNameParameter = command.CreateParameter();
            tableNameParameter.ParameterName = "@tableName";
            tableNameParameter.Value = table.Name;
            command.Parameters.Add(tableNameParameter);
            return Convert.ToString(await command.ExecuteScalarAsync().ConfigureAwait(false));
        }

        private static int FindMatchingParenthesis(string sql, int openIndex)
        {
            var depth = 0;
            var inString = false;
            for (var i = openIndex; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (ch == '\'')
                {
                    if (inString && i + 1 < sql.Length && sql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }
                    inString = !inString;
                    continue;
                }
                if (inString)
                    continue;
                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')')
                {
                    depth--;
                    if (depth == 0)
                        return i;
                }
            }

            return -1;
        }

        private static IReadOnlyDictionary<string, string> ExtractSqliteColumnCollations(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(createTableSql))
                return result;

            var open = createTableSql.IndexOf('(');
            if (open < 0)
                return result;
            var close = FindMatchingParenthesis(createTableSql, open);
            if (close <= open)
                return result;

            foreach (var part in SplitTopLevelCommaSeparated(createTableSql.Substring(open + 1, close - open - 1)))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0 || StartsWithTableConstraint(trimmed))
                    continue;

                if (!TryReadLeadingSqlIdentifier(trimmed, out var columnName, out _))
                    continue;

                var match = System.Text.RegularExpressions.Regex.Match(
                    trimmed,
                    @"\bCOLLATE\s+(?<name>""[^""]+""|\[[^\]]+\]|`[^`]+`|[A-Za-z_][A-Za-z0-9_-]*)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
                if (!match.Success)
                    continue;

                result[columnName] = UnquoteSqlIdentifier(match.Groups["name"].Value);
            }

            return result;
        }

        private static bool StartsWithTableConstraint(string value)
            => value.StartsWith("CONSTRAINT ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("PRIMARY ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("FOREIGN ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("UNIQUE ", StringComparison.OrdinalIgnoreCase)
               || value.StartsWith("CHECK ", StringComparison.OrdinalIgnoreCase);

        private static bool TryReadLeadingSqlIdentifier(string value, out string identifier, out int nextIndex)
        {
            identifier = string.Empty;
            nextIndex = 0;
            if (string.IsNullOrWhiteSpace(value))
                return false;

            var i = 0;
            while (i < value.Length && char.IsWhiteSpace(value[i]))
                i++;
            if (i >= value.Length)
                return false;

            var ch = value[i];
            if (ch is '"' or '`' or '[')
            {
                var close = ch == '[' ? ']' : ch;
                var start = ++i;
                var sb = new System.Text.StringBuilder();
                while (i < value.Length)
                {
                    if (value[i] == close)
                    {
                        if (i + 1 < value.Length && value[i + 1] == close)
                        {
                            sb.Append(close);
                            i += 2;
                            continue;
                        }

                        identifier = sb.ToString();
                        nextIndex = i + 1;
                        return identifier.Length > 0;
                    }

                    sb.Append(value[i++]);
                }

                nextIndex = start;
                return false;
            }

            var begin = i;
            while (i < value.Length && (char.IsLetterOrDigit(value[i]) || value[i] == '_' || value[i] == '$'))
                i++;
            if (i == begin)
                return false;

            identifier = value.Substring(begin, i - begin);
            nextIndex = i;
            return true;
        }

        private static string UnquoteSqlIdentifier(string value)
        {
            var trimmed = value.Trim();
            if (trimmed.Length >= 2)
            {
                var first = trimmed[0];
                var last = trimmed[^1];
                if ((first == '"' && last == '"') || (first == '`' && last == '`'))
                    return trimmed.Substring(1, trimmed.Length - 2).Replace(new string(first, 2), first.ToString(), StringComparison.Ordinal);
                if (first == '[' && last == ']')
                    return trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]", StringComparison.Ordinal);
            }

            return trimmed;
        }

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string sql)
        {
            var result = new List<string>();
            var start = 0;
            var depth = 0;
            var inString = false;
            for (var i = 0; i < sql.Length; i++)
            {
                var ch = sql[i];
                if (ch == '\'')
                {
                    if (inString && i + 1 < sql.Length && sql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }
                    inString = !inString;
                    continue;
                }
                if (inString)
                    continue;
                if (ch == '(')
                    depth++;
                else if (ch == ')')
                    depth--;
                else if (ch == ',' && depth == 0)
                {
                    result.Add(sql.Substring(start, i - start));
                    start = i + 1;
                }
            }
            result.Add(sql[start..]);
            return result;
        }

        private static bool IsSqliteProviderSpecificDeclaredType(string? declaredType)
        {
            if (string.IsNullOrWhiteSpace(declaredType))
                return false;

            var normalized = declaredType.Trim().ToUpperInvariant();
            if (IsSqliteScaffoldableScalarDeclaredType(normalized))
                return false;

            return normalized.Contains("GEOMETRY", StringComparison.Ordinal)
                   || normalized.Contains("GEOGRAPHY", StringComparison.Ordinal)
                   || normalized.Contains("HIERARCHYID", StringComparison.Ordinal)
                   || normalized.Contains("SQL_VARIANT", StringComparison.Ordinal)
                   || normalized.Contains("INET", StringComparison.Ordinal)
                   || normalized.Contains("CIDR", StringComparison.Ordinal)
                   || normalized.Contains("MACADDR", StringComparison.Ordinal)
                   || normalized.StartsWith("ENUM", StringComparison.Ordinal)
                   || normalized.StartsWith("SET", StringComparison.Ordinal)
                   || normalized.EndsWith("[]", StringComparison.Ordinal);
        }

        private static bool IsSqliteScaffoldableScalarDeclaredType(string normalizedDeclaredType)
            => normalizedDeclaredType.Contains("JSON", StringComparison.Ordinal)
               || normalizedDeclaredType.Contains("XML", StringComparison.Ordinal)
               || normalizedDeclaredType.Contains("UUID", StringComparison.Ordinal);

        private static void RemoveSupportedProviderSpecificColumnTypeDiagnostics(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            unsupportedFeatures.RemoveAll(feature =>
                string.Equals(feature.Kind, "ProviderSpecificColumnType", StringComparison.OrdinalIgnoreCase)
                && IsScaffoldableProviderSpecificColumnType(feature.Detail)
                && columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                && properties.ContainsKey(feature.Name));
        }

        private static bool IsScaffoldableProviderSpecificColumnType(string? detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = detail.Trim().ToLowerInvariant();
            return normalized == "xml"
                   || normalized == "json"
                   || normalized == "jsonb"
                   || normalized == "uuid"
                   || normalized == "user-defined (uuid)"
                   || normalized == "year";
        }

        private static string ScaffoldDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
        {
            var compositeForeignKeys = foreignKeys
                .Where(fk => fk.ColumnCount > 1)
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase)
                .Where(g => !ReferencesScaffoldablePrincipalKey(g, primaryKeyColumnsByTable, indexes))
                .OrderBy(g => g.First().DependentSchema, StringComparer.Ordinal)
                .ThenBy(g => g.First().DependentTable, StringComparer.Ordinal)
                .ThenBy(g => g.First().ConstraintName, StringComparer.Ordinal)
                .ToArray();
            var possibleJoinTables = BuildPossibleJoinTableDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                emittedManyToManyJoinTableKeys);

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
                    sb.AppendLine("These composite foreign keys do not target the generated principal primary key or an exact ordered unfiltered unique index, so v1 scaffolding keeps them diagnostic.");
                    sb.AppendLine("The generated entity classes keep the scalar columns, and no relationship navigation is emitted for these constraints.");
                    sb.AppendLine();
                    sb.AppendLine("| Code | Severity | Category | Constraint | Dependent | Columns | Principal | Principal Columns | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
                    foreach (var group in compositeForeignKeys)
                    {
                        var rows = group.ToArray();
                        var first = rows[0];
                        var dependent = TableKey(first.DependentSchema, first.DependentTable);
                        var principal = TableKey(first.PrincipalSchema, first.PrincipalTable);
                        sb.AppendLine($"| {ScaffoldDiagnosticCodeForCompositeForeignKey()} | {ScaffoldDiagnosticSeverity()} | {ScaffoldDiagnosticCategoryForCompositeForeignKey()} | {EscapeMarkdown(first.ConstraintName)} | {EscapeMarkdown(dependent)} | {EscapeMarkdown(string.Join(", ", rows.Select(r => r.DependentColumn)))} | {EscapeMarkdown(principal)} | {EscapeMarkdown(string.Join(", ", rows.Select(r => r.PrincipalColumn)))} | {EscapeMarkdown(SuggestedActionForCompositeForeignKey())} |");
                    }
                }

                if (possibleJoinTables.Length > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Possible Many-To-Many Join Tables");
                    sb.AppendLine();
                    sb.AppendLine("These tables look like join-table candidates but were scaffolded as normal entities because at least one safe `UsingTable` requirement was not met.");
                    sb.AppendLine();
                    sb.AppendLine("| Code | Severity | Category | Table | Principal Tables | Constraints | Reason Codes | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
                    foreach (var table in possibleJoinTables)
                        sb.AppendLine($"| {ScaffoldDiagnosticCodeForPossibleJoinTable()} | {ScaffoldDiagnosticSeverity()} | {ScaffoldDiagnosticCategoryForPossibleJoinTable()} | {EscapeMarkdown(table.TableKey)} | {EscapeMarkdown(string.Join(", ", table.PrincipalTables))} | {EscapeMarkdown(string.Join(", ", table.ConstraintNames))} | {EscapeMarkdown(string.Join(", ", table.Reasons))} | {EscapeMarkdown(SuggestedActionForPossibleJoinTable())} |");
                }

                if (unsupportedFeatures.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Provider-Owned Schema Features");
                    sb.AppendLine();
                    sb.AppendLine("Defaults, ordinary table CHECK constraints, and computed/generated column expressions are emitted as migration metadata when possible. Collations, scaffoldable JSON/XML/UUID scalar storage, and parsed SQL Server identity seed/increment settings are emitted when a generated property can safely own them. Remaining provider-specific column types, rowversion/timestamp columns, unparsed identity strategies, non-default FK referential actions, relationships that do not target the generated principal primary key or an exact ordered unfiltered unique index, triggers, provider-native temporal tables, and tables without primary keys are discovered for review, but are not emitted as complete provider-neutral nORM model code.");
                    sb.AppendLine();
                    sb.AppendLine("| Code | Severity | Category | Kind | Table | Object | Detail | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- | --- |");
                    foreach (var feature in unsupportedFeatures
                        .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                        .ThenBy(f => f.Kind, StringComparer.Ordinal)
                        .ThenBy(f => f.Name, StringComparer.Ordinal))
                    {
                        sb.AppendLine($"| {ScaffoldDiagnosticCodeForUnsupportedFeature(feature.Kind)} | {ScaffoldDiagnosticSeverity()} | {ScaffoldDiagnosticCategoryForUnsupportedFeature(feature.Kind)} | {EscapeMarkdown(feature.Kind)} | {EscapeMarkdown(feature.TableKey)} | {EscapeMarkdown(feature.Name)} | {EscapeMarkdown(feature.Detail)} | {EscapeMarkdown(SuggestedActionForUnsupportedFeature(feature.Kind))} |");
                    }
                }

                if (skippedObjects.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("## Skipped Database Objects");
                    sb.AppendLine();
                    sb.AppendLine("Views/materialized views can be emitted as opt-in query artifacts, routines can be emitted as opt-in provider-bound stubs, and sequences/synonyms/events remain provider-owned review items.");
                    sb.AppendLine();
                    sb.AppendLine("| Code | Severity | Category | Kind | Name | Detail | Suggested Action |");
                    sb.AppendLine("| --- | --- | --- | --- | --- | --- | --- |");
                    foreach (var obj in skippedObjects
                        .OrderBy(o => TableKey(o.Schema, o.Name), StringComparer.Ordinal)
                        .ThenBy(o => o.Kind, StringComparer.Ordinal))
                    {
                        sb.AppendLine($"| {ScaffoldDiagnosticCodeForSkippedObject(obj.Kind)} | {ScaffoldDiagnosticSeverity()} | {ScaffoldDiagnosticCategoryForSkippedObject(obj.Kind)} | {EscapeMarkdown(obj.Kind)} | {EscapeMarkdown(TableKey(obj.Schema, obj.Name))} | {EscapeMarkdown(obj.Detail)} | {EscapeMarkdown(SuggestedActionForSkippedObject(obj.Kind))} |");
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

        private static ScaffoldPossibleJoinTableDiagnostic[] BuildPossibleJoinTableDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
        {
            return foreignKeys
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase)
                .Select(g =>
                {
                    var rows = g.ToArray();
                    var tableKey = g.Key;
                    var principalTables = rows.Select(fk => TableKey(fk.PrincipalSchema, fk.PrincipalTable)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray();
                    var constraintNames = rows.Select(fk => fk.ConstraintName).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x, StringComparer.Ordinal).ToArray();
                    var reasons = BuildPossibleJoinTableReasons(tableKey, rows, primaryKeyColumnsByTable, columnPropertiesByTable, nonNullableColumnsByTable);
                    return new ScaffoldPossibleJoinTableDiagnostic(tableKey, principalTables, constraintNames, reasons);
                })
                .Where(g => g.ConstraintNames.Length >= 2 && g.PrincipalTables.Length is 1 or 2)
                .Where(g => emittedManyToManyJoinTableKeys is null || !emittedManyToManyJoinTableKeys.Contains(g.TableKey))
                .OrderBy(g => g.TableKey, StringComparer.Ordinal)
                .ToArray();
        }

        private static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
        {
            var reasons = new HashSet<string>(StringComparer.Ordinal);
            var fkColumns = foreignKeys.Select(fk => fk.DependentColumn).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var constraints = foreignKeys.GroupBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase).ToArray();

            if (constraints.Length != 2)
                reasons.Add("not-two-foreign-keys");
            if (constraints.Any(g => g.Count() == 0 || g.Any(fk => fk.ColumnCount != g.Count())))
                reasons.Add("foreign-key-metadata-incomplete");

            if (columnPropertiesByTable.TryGetValue(tableKey, out var columns)
                && (columns.Count != fkColumns.Count || columns.Keys.Any(column => !fkColumns.Contains(column))))
            {
                reasons.Add("payload-columns");
            }

            if (!primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
                || primaryKeyColumns.Count == 0)
            {
                reasons.Add("missing-primary-key");
            }
            else if (primaryKeyColumns.Count != fkColumns.Count || primaryKeyColumns.Any(column => !fkColumns.Contains(column)))
            {
                reasons.Add("primary-key-not-exact-bridge-columns");
            }

            if (nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
                && fkColumns.Any(column => !nonNullableColumns.Contains(column)))
            {
                reasons.Add("nullable-foreign-key");
            }

            foreach (var fk in constraints.Select(g => g.First()))
            {
                var principalTableKey = TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var principalColumns = foreignKeys
                    .Where(row => string.Equals(row.ConstraintName, fk.ConstraintName, StringComparison.OrdinalIgnoreCase))
                    .Select(row => row.PrincipalColumn)
                    .ToArray();
                if (!HasPrimaryKeyColumns(primaryKeyColumnsByTable, principalTableKey, principalColumns))
                    reasons.Add("principal-key-not-primary-key");
            }

            if (reasons.Count == 0)
                reasons.Add("review-shape");

            return reasons.OrderBy(reason => reason, StringComparer.Ordinal).ToArray();
        }

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
            => "If this is a safe pure join table, verify all FK columns are NOT NULL, both FKs target generated primary keys or exact ordered unfiltered unique indexes, and the bridge uses either an FK-column primary key or a generated surrogate primary key plus an exact unfiltered unique index over the FK columns; then use the generated UsingTable mapping. Keep payload, duplicate-pair, or domain-behavior bridges as explicit join entities.";

        private static string ScaffoldDiagnosticSeverity()
            => "Warning";

        private static string ScaffoldDiagnosticCodeForCompositeForeignKey()
            => "SCF001";

        private static string ScaffoldDiagnosticCategoryForCompositeForeignKey()
            => "relationship";

        private static string ScaffoldDiagnosticCodeForPossibleJoinTable()
            => "SCF002";

        private static string ScaffoldDiagnosticCategoryForPossibleJoinTable()
            => "many-to-many";

        private static string ScaffoldDiagnosticCodeForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "SCF100",
                "Computed" => "SCF101",
                "CheckConstraint" => "SCF102",
                "Collation" => "SCF103",
                "ProviderSpecificColumnType" => "SCF104",
                "ReferentialAction" => "SCF106",
                "RelationshipPrincipalKey" => "SCF107",
                "RowVersion" => "SCF108",
                "IdentityStrategy" => "SCF109",
                "Trigger" => "SCF110",
                "PartialIndex" => "SCF111",
                "ExpressionIndex" => "SCF112",
                "IncludedColumnIndex" => "SCF113",
                "DescendingIndex" => "SCF114",
                "TemporalTable" => "SCF115",
                "MissingPrimaryKey" => "SCF116",
                "PrefixIndex" => "SCF117",
                _ => "SCF199"
            };

        private static string ScaffoldDiagnosticCategoryForUnsupportedFeature(string kind)
            => kind switch
            {
                "ReferentialAction" or "RelationshipPrincipalKey" => "relationship",
                "PartialIndex" or "ExpressionIndex" or "IncludedColumnIndex" or "DescendingIndex" or "PrefixIndex" => "index",
                "Trigger" or "TemporalTable" => "database-object",
                "MissingPrimaryKey" => "table-shape",
                _ => "schema-feature"
            };

        private static string ScaffoldDiagnosticCodeForSkippedObject(string kind)
            => kind switch
            {
                "View" => "SCF200",
                "Routine" => "SCF201",
                "Sequence" => "SCF202",
                "Synonym" => "SCF203",
                "MaterializedView" => "SCF204",
                "Event" => "SCF205",
                "VirtualTable" => "SCF206",
                "VirtualTableShadow" => "SCF207",
                _ => "SCF299"
            };

        private static string ScaffoldDiagnosticCategoryForSkippedObject(string kind)
            => kind switch
            {
                "View" or "MaterializedView" => "query-object",
                "Routine" or "Event" => "routine",
                "Sequence" => "key-generation",
                "VirtualTable" or "VirtualTableShadow" => "virtual-table",
                _ => "database-object"
            };

        private static string SuggestedActionForUnsupportedFeature(string kind)
            => kind switch
            {
                "Default" => "Move default semantics into application/model configuration or keep provider DDL in migrations and treat the column as database-owned.",
                "Computed" => "Use explicit HasComputedColumnSql model configuration or keep the provider-specific generated expression in migrations.",
                "CheckConstraint" => "Use explicit HasCheckConstraint model configuration or keep the provider-specific CHECK predicate in migrations.",
                "Collation" => "Keep collation-sensitive behavior in provider migrations and add explicit application/query tests before relying on generated code for comparisons or ordering.",
                "ProviderSpecificColumnType" => "Keep this provider-specific type behind explicit provider migrations/converters or remodel it to a portable CLR/database shape before claiming provider mobility.",
                "ReferentialAction" => "Review the provider-specific FK referential action token; common actions are generated, but unrecognized actions need explicit migration/model handling.",
                "RelationshipPrincipalKey" => "Add a primary key or exact ordered unfiltered unique index for the referenced principal columns, or configure the relationship manually before relying on generated navigations.",
                "RowVersion" => "Keep provider-managed rowversion/timestamp semantics in migrations; scaffolded code marks the column as [Timestamp] and database-generated but cannot recreate provider DDL.",
                "IdentityStrategy" => "Parsed SQL Server IDENTITY(seed, increment) metadata is scaffolded into HasIdentityOptions; review any unparsed provider-specific identity strategy manually.",
                "Trigger" => "Keep the trigger in provider migrations and add integration tests for any side effects nORM cannot infer.",
                "PartialIndex" => "Keep the filtered/partial index in provider migrations; v1 scaffolding emits only provider-neutral column indexes.",
                "ExpressionIndex" => "Keep the expression index in provider migrations or replace it with a provider-neutral persisted column plus a normal index.",
                "IncludedColumnIndex" => "Keep included-column index tuning in provider migrations; v1 scaffolding emits only key-column index metadata.",
                "DescendingIndex" => "Review this descending index shape; ordinary column-key descending indexes are generated, but this one was not safe to map as provider-neutral index metadata.",
                "PrefixIndex" => "Keep the MySQL prefix index in provider migrations; prefix uniqueness is not full-column uniqueness and is not used for generated alternate-key relationships.",
                "TemporalTable" => "Choose provider-native temporal intentionally or migrate to nORM-managed temporal history; do not assume scaffolding round-trips native temporal DDL.",
                "MissingPrimaryKey" => "Generated code marks this type with [ReadOnlyEntity] so query materialization works but nORM writes are rejected; add a primary key before using generated writes or navigations.",
                _ => "Review the provider-owned object and add explicit model configuration or migration code for the intended behavior."
            };

        private static string SuggestedActionForSkippedObject(string kind)
            => kind switch
            {
                "View" => "Use --emit-view-entities/ScaffoldOptions.EmitViewEntities to generate a read-oriented query artifact, or keep the view behind explicit provider-bound query code.",
                "Routine" => "Keep routine calls behind explicit raw SQL/stored-procedure code and document the provider-bound contract.",
                "Sequence" => "Configure generated-key behavior explicitly or keep sequence DDL in provider migrations.",
                "Synonym" => "Use --emit-query-artifacts for local table/view synonyms, resolve the synonym to a supported base table, or keep non-query/remote synonyms behind provider-bound integration code.",
                "MaterializedView" => "Use --emit-view-entities/ScaffoldOptions.EmitViewEntities for a read-oriented query artifact and keep refresh behavior provider-bound.",
                "Event" => "Keep scheduled event behavior in provider operations/migrations; v1 scaffolding emits table models only.",
                "VirtualTable" => "Use --emit-query-artifacts/ScaffoldOptions.EmitQueryArtifacts for a read-oriented query artifact, or keep the virtual table behind provider-bound query/index code.",
                "VirtualTableShadow" => "Do not map SQLite virtual-table shadow storage as domain entities; keep it provider-owned with the virtual table.",
                _ => "Keep this database object in provider migrations or hand-written integration code."
            };

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(ScaffoldSkippedObject obj)
        {
            if (!string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase))
                return new Dictionary<string, object?>(0, StringComparer.Ordinal);

            var segments = obj.Detail.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 1; i < segments.Length; i++)
            {
                var pair = segments[i].Split('=', 2);
                if (pair.Length == 2)
                    values[pair[0].Trim()] = pair[1].Trim();
            }

            var provider = ParseRoutineProvider(segments.Length > 0 ? segments[0] : string.Empty);
            var routineType = ParseRoutineType(segments.Length > 0 ? segments[0] : string.Empty);
            values.TryGetValue("dataType", out var dataType);
            var metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["provider"] = provider,
                ["routineType"] = routineType,
                ["parameterCount"] = ParseNullableInt(values.TryGetValue("parameters", out var parameterCount) ? parameterCount : null),
                ["outputParameterCount"] = ParseNullableInt(values.TryGetValue("outputParameters", out var outputParameterCount) ? outputParameterCount : null)
            };

            if (!string.IsNullOrWhiteSpace(dataType))
                metadata["dataType"] = dataType;

            if (values.TryGetValue("callShape", out var callShape) && !string.IsNullOrWhiteSpace(callShape))
                metadata["callShape"] = callShape;
            else
            {
                var inferredCallShape = InferRoutineCallShape(provider, routineType, dataType);
                if (!string.IsNullOrWhiteSpace(inferredCallShape))
                    metadata["callShape"] = inferredCallShape;
            }

            if (values.TryGetValue("parameterModes", out var parameterModes))
                metadata["parameters"] = ParseRoutineParameters(parameterModes);

            return metadata;
        }

        private static string InferRoutineCallShape(string provider, string routineType, string? dataType)
        {
            if (!routineType.Contains("function", StringComparison.OrdinalIgnoreCase))
                return string.Empty;

            if (provider.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
                && (string.Equals(dataType, "record", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(dataType, "table", StringComparison.OrdinalIgnoreCase)))
            {
                return "table-valued-function";
            }

            return "scalar-function";
        }

        private static string ParseRoutineProvider(string header)
        {
            if (header.StartsWith("SQL Server ", StringComparison.OrdinalIgnoreCase)) return "SQL Server";
            if (header.StartsWith("PostgreSQL ", StringComparison.OrdinalIgnoreCase)) return "PostgreSQL";
            if (header.StartsWith("MySQL ", StringComparison.OrdinalIgnoreCase)) return "MySQL";
            var space = header.IndexOf(' ');
            return space > 0 ? header[..space] : header;
        }

        private static string ParseRoutineType(string header)
        {
            foreach (var provider in new[] { "SQL Server ", "PostgreSQL ", "MySQL " })
            {
                if (header.StartsWith(provider, StringComparison.OrdinalIgnoreCase))
                    return header[provider.Length..];
            }

            var space = header.IndexOf(' ');
            return space > 0 ? header[(space + 1)..] : header;
        }

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParseRoutineParameters(string parameterModes)
        {
            if (string.IsNullOrWhiteSpace(parameterModes))
                return Array.Empty<IReadOnlyDictionary<string, object?>>();

            return SplitRoutineParameterModes(parameterModes)
                .Select(raw =>
                {
                    var parts = raw.Split(':', 3);
                    var parameter = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["name"] = parts.Length > 0 ? parts[0] : string.Empty,
                        ["mode"] = parts.Length > 1 ? parts[1] : string.Empty
                    };
                    if (parts.Length > 2 && !string.IsNullOrWhiteSpace(parts[2]))
                        parameter["dataType"] = parts[2];
                    return (IReadOnlyDictionary<string, object?>)parameter;
                })
                .ToArray();
        }

        private static IReadOnlyList<string> SplitRoutineParameterModes(string parameterModes)
        {
            var parts = new List<string>();
            var start = 0;
            var depth = 0;
            for (var i = 0; i < parameterModes.Length; i++)
            {
                var ch = parameterModes[i];
                if (ch == '(')
                {
                    depth++;
                }
                else if (ch == ')' && depth > 0)
                {
                    depth--;
                }
                else if (ch == ',' && depth == 0)
                {
                    AddRoutineParameterModePart(parts, parameterModes[start..i]);
                    start = i + 1;
                }
            }

            AddRoutineParameterModePart(parts, parameterModes[start..]);
            return parts;
        }

        private static void AddRoutineParameterModePart(List<string> parts, string value)
        {
            var trimmed = value.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
                parts.Add(trimmed);
        }

        private static string ScaffoldDiagnosticsJson(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
        {
            var compositeForeignKeys = foreignKeys
                .Where(fk => fk.ColumnCount > 1)
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase)
                .Where(g => !ReferencesScaffoldablePrincipalKey(g, primaryKeyColumnsByTable, indexes))
                .OrderBy(g => g.First().DependentSchema, StringComparer.Ordinal)
                .ThenBy(g => g.First().DependentTable, StringComparer.Ordinal)
                .ThenBy(g => g.First().ConstraintName, StringComparer.Ordinal)
                .Select(g =>
                {
                    var rows = g.ToArray();
                    var first = rows[0];
                    return new
                    {
                        code = ScaffoldDiagnosticCodeForCompositeForeignKey(),
                        severity = ScaffoldDiagnosticSeverity(),
                        category = ScaffoldDiagnosticCategoryForCompositeForeignKey(),
                        constraint = first.ConstraintName,
                        dependentTable = TableKey(first.DependentSchema, first.DependentTable),
                        dependentColumns = rows.Select(r => r.DependentColumn).ToArray(),
                        principalTable = TableKey(first.PrincipalSchema, first.PrincipalTable),
                        principalColumns = rows.Select(r => r.PrincipalColumn).ToArray(),
                        suggestedAction = SuggestedActionForCompositeForeignKey()
                    };
                })
                .ToArray();

            var possibleJoinTables = BuildPossibleJoinTableDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                emittedManyToManyJoinTableKeys)
                .Select(g => new
                {
                    code = ScaffoldDiagnosticCodeForPossibleJoinTable(),
                    severity = ScaffoldDiagnosticSeverity(),
                    category = ScaffoldDiagnosticCategoryForPossibleJoinTable(),
                    table = g.TableKey,
                    principalTables = g.PrincipalTables,
                    constraints = g.ConstraintNames,
                    reasons = g.Reasons,
                    suggestedAction = SuggestedActionForPossibleJoinTable()
                })
                .ToArray();

            var providerOwnedSchemaFeatures = unsupportedFeatures
                .OrderBy(f => f.TableKey, StringComparer.Ordinal)
                .ThenBy(f => f.Kind, StringComparer.Ordinal)
                .ThenBy(f => f.Name, StringComparer.Ordinal)
                .Select(f => new
                {
                    code = ScaffoldDiagnosticCodeForUnsupportedFeature(f.Kind),
                    severity = ScaffoldDiagnosticSeverity(),
                    category = ScaffoldDiagnosticCategoryForUnsupportedFeature(f.Kind),
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
                    code = ScaffoldDiagnosticCodeForSkippedObject(o.Kind),
                    severity = ScaffoldDiagnosticSeverity(),
                    category = ScaffoldDiagnosticCategoryForSkippedObject(o.Kind),
                    kind = o.Kind,
                    name = TableKey(o.Schema, o.Name),
                    detail = o.Detail,
                    metadata = BuildSkippedObjectMetadata(o),
                    suggestedAction = SuggestedActionForSkippedObject(o.Kind)
                })
                .ToArray();

            var diagnosticCodes = compositeForeignKeys
                .Select(item => item.code)
                .Concat(possibleJoinTables.Select(item => item.code))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.code))
                .Concat(skippedDatabaseObjects.Select(item => item.code))
                .GroupBy(code => code, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            var diagnosticCategories = compositeForeignKeys
                .Select(item => item.category)
                .Concat(possibleJoinTables.Select(item => item.category))
                .Concat(providerOwnedSchemaFeatures.Select(item => item.category))
                .Concat(skippedDatabaseObjects.Select(item => item.category))
                .GroupBy(category => category, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            return JsonSerializer.Serialize(
                new
                {
                    version = 1,
                    summary = new
                    {
                        totalWarnings = compositeForeignKeys.Length
                            + possibleJoinTables.Length
                            + providerOwnedSchemaFeatures.Length
                            + skippedDatabaseObjects.Length,
                        sectionCounts = new
                        {
                            compositeForeignKeys = compositeForeignKeys.Length,
                            possibleManyToManyJoinTables = possibleJoinTables.Length,
                            providerOwnedSchemaFeatures = providerOwnedSchemaFeatures.Length,
                            skippedDatabaseObjects = skippedDatabaseObjects.Length
                        },
                        codes = diagnosticCodes,
                        categories = diagnosticCategories
                    },
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
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var joins = new List<ScaffoldManyToManyJoin>();

            foreach (var group in foreignKeys
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable), StringComparer.OrdinalIgnoreCase))
            {
                var joinTableKey = group.Key;
                if (!tableKeys.Contains(joinTableKey))
                    continue;

                var fkGroups = OrderManyToManyForeignKeyGroups(
                    GetUnqualifiedName(joinTableKey),
                    group
                    .GroupBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.ToArray())
                    .ToArray());

                if (fkGroups.Length != 2 || fkGroups.Any(rows => rows.Length == 0 || rows.Any(row => row.ColumnCount != rows.Length)))
                    continue;

                if (!columnPropertiesByTable.TryGetValue(joinTableKey, out var joinColumns))
                    continue;

                var fkColumnNames = fkGroups.SelectMany(rows => rows.Select(fk => fk.DependentColumn)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                if (!primaryKeyColumnsByTable.TryGetValue(joinTableKey, out var joinPrimaryKeyColumns))
                {
                    continue;
                }

                var hasExactBridgePrimaryKey = joinPrimaryKeyColumns.Count == fkColumnNames.Count
                    && joinPrimaryKeyColumns.All(column => fkColumnNames.Contains(column));
                var extraColumns = joinColumns.Keys.Where(column => !fkColumnNames.Contains(column)).ToArray();
                var hasGeneratedSurrogatePrimaryKey = extraColumns.Length == 1
                    && joinPrimaryKeyColumns.Count == 1
                    && string.Equals(joinPrimaryKeyColumns[0], extraColumns[0], StringComparison.OrdinalIgnoreCase)
                    && identityColumnsByTable.TryGetValue(joinTableKey, out var identityColumns)
                    && identityColumns.Contains(extraColumns[0])
                    && HasExactUniqueIndex(indexes, joinTableKey, fkColumnNames);

                if (!(joinColumns.Count == fkColumnNames.Count && joinColumns.Keys.All(column => fkColumnNames.Contains(column)))
                    && !hasGeneratedSurrogatePrimaryKey)
                    continue;

                if (!hasExactBridgePrimaryKey && !hasGeneratedSurrogatePrimaryKey)
                    continue;

                if (!nonNullableColumnsByTable.TryGetValue(joinTableKey, out var nonNullableColumns)
                    || fkColumnNames.Any(column => !nonNullableColumns.Contains(column)))
                {
                    continue;
                }

                var leftGroup = fkGroups[0];
                var rightGroup = fkGroups[1];
                var left = leftGroup[0];
                var right = rightGroup[0];
                var leftTableKey = TableKey(left.PrincipalSchema, left.PrincipalTable);
                var rightTableKey = TableKey(right.PrincipalSchema, right.PrincipalTable);
                if (!entityByTable.TryGetValue(leftTableKey, out var leftEntity)
                    || !entityByTable.TryGetValue(rightTableKey, out var rightEntity))
                {
                    continue;
                }

                var leftPrincipalColumns = leftGroup.Select(fk => fk.PrincipalColumn).ToArray();
                var rightPrincipalColumns = rightGroup.Select(fk => fk.PrincipalColumn).ToArray();
                var leftUsesPrimaryKey = HasPrimaryKeyColumns(primaryKeyColumnsByTable, leftTableKey, leftPrincipalColumns);
                var rightUsesPrimaryKey = HasPrimaryKeyColumns(primaryKeyColumnsByTable, rightTableKey, rightPrincipalColumns);
                if ((!leftUsesPrimaryKey && !ReferencesUniqueIndex(leftGroup, primaryKeyColumnsByTable, indexes))
                    || (!rightUsesPrimaryKey && !ReferencesUniqueIndex(rightGroup, primaryKeyColumnsByTable, indexes)))
                {
                    continue;
                }

                var leftPrincipalKeyProperties = leftPrincipalColumns
                    .Select(column => GetColumnPropertyName(columnPropertiesByTable, leftTableKey, column))
                    .ToArray();
                var rightPrincipalKeyProperties = rightPrincipalColumns
                    .Select(column => GetColumnPropertyName(columnPropertiesByTable, rightTableKey, column))
                    .ToArray();

                var isSelfJoin = string.Equals(leftTableKey, rightTableKey, StringComparison.OrdinalIgnoreCase);
                var leftCollectionBase = Pluralize(rightEntity);
                var rightCollectionBase = Pluralize(leftEntity);
                if (isSelfJoin)
                {
                    leftCollectionBase += "By" + GetColumnPropertyName(columnPropertiesByTable, joinTableKey, left.DependentColumn);
                    rightCollectionBase += "By" + GetColumnPropertyName(columnPropertiesByTable, joinTableKey, right.DependentColumn);
                }

                var existingNames = GetOrCreateMemberNames(memberNamesByTable, leftTableKey);
                var leftCollectionName = MakeUnique(leftCollectionBase, existingNames);
                var existingInverseNames = GetOrCreateMemberNames(memberNamesByTable, rightTableKey);
                var rightCollectionName = MakeUnique(rightCollectionBase, existingInverseNames);
                joins.Add(new ScaffoldManyToManyJoin(
                    joinTableKey,
                    leftTableKey,
                    rightTableKey,
                    left.DependentTable,
                    left.DependentSchema,
                    leftEntity,
                    rightEntity,
                    leftGroup.Select(fk => fk.DependentColumn).ToArray(),
                    rightGroup.Select(fk => fk.DependentColumn).ToArray(),
                    leftPrincipalKeyProperties,
                    rightPrincipalKeyProperties,
                    leftUsesPrimaryKey && rightUsesPrimaryKey,
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

        private static ScaffoldForeignKey[][] OrderManyToManyForeignKeyGroups(string joinTableName, ScaffoldForeignKey[][] foreignKeyGroups)
        {
            return foreignKeyGroups
                .OrderBy(group => PrincipalNamePosition(joinTableName, group[0].PrincipalTable))
                .ThenBy(group => PrincipalNamePosition(joinTableName, TrimIdSuffix(group[0].DependentColumn)))
                .ThenBy(group => group[0].DependentColumn, StringComparer.Ordinal)
                .ToArray();
        }

        private static int PrincipalNamePosition(string joinTableName, string principalTable)
        {
            var position = joinTableName.IndexOf(principalTable, StringComparison.OrdinalIgnoreCase);
            return position < 0 ? int.MaxValue : position;
        }

        private static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
        {
            return primaryKeyColumnsByTable.TryGetValue(tableKey, out var keyColumns)
                   && keyColumns.Count == 1
                   && keyColumns.Contains(columnName);
        }

        private static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
        {
            return primaryKeyColumnsByTable.TryGetValue(tableKey, out var keyColumns)
                   && keyColumns.Count == columnNames.Count
                   && keyColumns.SequenceEqual(columnNames, StringComparer.OrdinalIgnoreCase);
        }

        private static bool HasExactUniqueIndex(
            IReadOnlyList<ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
        {
            return indexes
                .Where(index => IsUnfilteredUniqueKeyIndex(index)
                                && string.Equals(index.TableKey, tableKey, StringComparison.OrdinalIgnoreCase))
                .GroupBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Any(group =>
                {
                    var keyColumns = group
                        .Where(index => !index.IsIncluded)
                        .OrderBy(index => index.Ordinal)
                        .Select(index => index.ColumnName)
                        .ToArray();
                    return keyColumns.Length == columnNames.Count
                           && keyColumns.All(columnNames.Contains);
                });
        }

        private static bool ReferencesPrimaryKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
        {
            var rows = foreignKeyGroup.ToArray();
            if (rows.Length == 0)
                return false;

            var principalKey = TableKey(rows[0].PrincipalSchema, rows[0].PrincipalTable);
            var principalColumns = rows.Select(row => row.PrincipalColumn).ToArray();
            return primaryKeyColumnsByTable.TryGetValue(principalKey, out var keyColumns)
                   && keyColumns.Count == rows.Length
                   && keyColumns.SequenceEqual(principalColumns, StringComparer.OrdinalIgnoreCase);
        }

        private static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ReferencesPrimaryKey(foreignKeyGroup, primaryKeyColumnsByTable)
               || ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes);

        private static IReadOnlyList<ScaffoldPrimaryKey> BuildCompositePrimaryKeys(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlySet<string> skippedTableKeys)
        {
            var keys = new List<ScaffoldPrimaryKey>();
            foreach (var (tableKey, keyColumns) in primaryKeyColumnsByTable.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                if (keyColumns.Count <= 1
                    || skippedTableKeys.Contains(tableKey)
                    || !entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var propertyNames))
                {
                    continue;
                }

                var keyProperties = keyColumns
                    .Where(propertyNames.ContainsKey)
                    .Select(column => propertyNames[column])
                    .ToArray();
                if (keyProperties.Length == keyColumns.Count)
                    keys.Add(new ScaffoldPrimaryKey(entityName, keyProperties));
            }

            return keys;
        }

        private static bool ReferencesUniqueIndex(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ReferencesUniqueIndex(foreignKeyGroup.ToArray(), primaryKeyColumnsByTable, indexes);

        private static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
        {
            if (rows.Count == 0)
                return false;

            var principalKey = TableKey(rows[0].PrincipalSchema, rows[0].PrincipalTable);
            if (!primaryKeyColumnsByTable.TryGetValue(principalKey, out var primaryKeyColumns)
                || primaryKeyColumns.Count == 0)
            {
                return false;
            }

            var principalColumns = rows.Select(row => row.PrincipalColumn).ToArray();
            return indexes
                .Where(index => IsUnfilteredUniqueKeyIndex(index) && string.Equals(index.TableKey, principalKey, StringComparison.OrdinalIgnoreCase))
                .GroupBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Any(group =>
                {
                    var keyColumns = group
                        .Where(index => !index.IsIncluded)
                        .OrderBy(index => index.Ordinal)
                        .Select(index => index.ColumnName)
                        .ToArray();
                    return keyColumns.Length == rows.Count
                           && group.All(col => col.ColumnCount == rows.Count)
                           && keyColumns.SequenceEqual(principalColumns, StringComparer.OrdinalIgnoreCase);
                });
        }

        private static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndex index)
            => index.IsUnique
               && !index.IsIncluded
               && string.IsNullOrWhiteSpace(index.FilterSql);

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var relationships = new List<ScaffoldRelationship>();
            var relationshipPairCounts = foreignKeys
                .GroupBy(fk => $"{TableKey(fk.DependentSchema, fk.DependentTable)}\u001f{TableKey(fk.PrincipalSchema, fk.PrincipalTable)}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .GroupBy(fk => TableKey(fk.DependentSchema, fk.DependentTable) + "\u001f" + TableKey(fk.PrincipalSchema, fk.PrincipalTable), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

            foreach (var foreignKeyGroup in foreignKeys
                .GroupBy(fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}", StringComparer.OrdinalIgnoreCase))
            {
                var rows = foreignKeyGroup.ToArray();
                var foreignKey = rows[0];
                var dependentKey = TableKey(foreignKey.DependentSchema, foreignKey.DependentTable);
                var principalKey = TableKey(foreignKey.PrincipalSchema, foreignKey.PrincipalTable);
                if (!entityByTable.TryGetValue(dependentKey, out var dependentEntity)
                    || !entityByTable.TryGetValue(principalKey, out var principalEntity))
                {
                    continue;
                }

                if (!ReferencesScaffoldablePrincipalKey(foreignKeyGroup, primaryKeyColumnsByTable, indexes))
                    continue;

                var foreignKeyProperties = rows.Select(row => GetColumnPropertyName(columnPropertiesByTable, dependentKey, row.DependentColumn)).ToArray();
                var principalKeyProperties = rows.Select(row => GetColumnPropertyName(columnPropertiesByTable, principalKey, row.PrincipalColumn)).ToArray();
                var foreignKeyProperty = foreignKeyProperties[0];
                var principalKeyProperty = principalKeyProperties[0];
                var hasMultipleRelationshipsToSamePrincipal = relationshipPairCounts.TryGetValue(
                    dependentKey + "\u001f" + principalKey,
                    out var relationshipPairCount)
                    && relationshipPairCount > 1;
                var isSelfRelationship = string.Equals(dependentKey, principalKey, StringComparison.OrdinalIgnoreCase);

                var dependentMemberNames = GetOrCreateMemberNames(memberNamesByTable, dependentKey);
                var referenceBase = principalEntity;
                if (isSelfRelationship
                    || hasMultipleRelationshipsToSamePrincipal
                    || dependentMemberNames.Contains(referenceBase))
                {
                    referenceBase = TrimIdSuffix(foreignKeyProperty);
                    if (string.IsNullOrWhiteSpace(referenceBase))
                        referenceBase = principalEntity + "Navigation";
                }

                var referenceName = MakeUnique(referenceBase, dependentMemberNames);

                var principalMemberNames = GetOrCreateMemberNames(memberNamesByTable, principalKey);
                var collectionBase = Pluralize(dependentEntity);
                if (isSelfRelationship
                    || hasMultipleRelationshipsToSamePrincipal
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
                    string.Equals(foreignKey.OnDelete, "CASCADE", StringComparison.OrdinalIgnoreCase),
                    NormalizeReferentialAction(foreignKey.OnDelete),
                    NormalizeReferentialAction(foreignKey.OnUpdate))
                {
                    ForeignKeyPropertyNames = foreignKeyProperties,
                    PrincipalKeyPropertyNames = principalKeyProperties
                });
            }

            return relationships;
        }

        private static IReadOnlyDictionary<string, string> BuildEntityNameMap(IReadOnlyList<ScaffoldTable> tables)
        {
            var names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var existingNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var duplicateTableNames = tables
                .GroupBy(table => table.Name, StringComparer.OrdinalIgnoreCase)
                .Where(group => group.Count() > 1)
                .Select(group => group.Key)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            foreach (var table in tables)
            {
                var sourceName = duplicateTableNames.Contains(table.Name)
                    ? string.IsNullOrWhiteSpace(table.Schema)
                        ? "Default_" + table.Name
                        : table.Schema + "_" + table.Name
                    : table.Name;
                var baseName = EscapeCSharpIdentifier(ToPascalCase(sourceName));
                names[TableKey(table.Schema, table.Name)] = MakeUnique(baseName, existingNames);
            }

            return names;
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetNonNullableColumnNamesAsync(
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
                var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (DataRow row in schema.Rows)
                {
                    var columnName = row["ColumnName"]!.ToString()!;
                    var allowNull = row["AllowDBNull"] is bool b && b;
                    if (!allowNull)
                        columns.Add(columnName);
                }

                result[TableKey(table.Schema, table.Name)] = columns;
            }

            return result;
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

        private static HashSet<string> CreateReservedContextMemberNameSet()
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var member in typeof(DbContext).GetMembers())
                names.Add(member.Name);
            foreach (var member in typeof(object).GetMembers())
                names.Add(member.Name);
            names.Add("ConfigureOptions");
            return names;
        }

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildScaffoldDefaultValueMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "Default", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.ContainsKey(feature.Name)
                    || !TryNormalizeScaffoldDefaultSql(feature.Detail, out var defaultValueSql))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = defaultValueSql;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
        {
            var result = new List<ScaffoldDefaultValueConfiguration>();
            foreach (var (tableKey, defaults) in defaultValuesByTable)
            {
                if (!entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var properties))
                    continue;

                foreach (var (columnName, defaultValueSql) in defaults)
                {
                    if (properties.TryGetValue(columnName, out var propertyName))
                        result.Add(new ScaffoldDefaultValueConfiguration(tableKey, entityName, columnName, propertyName, defaultValueSql));
                }
            }

            return result;
        }

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new List<ScaffoldIdentityOptionConfiguration>();
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "IdentityStrategy", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !TryParseIdentityOptions(feature.Detail, out var seed, out var increment)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                result.Add(new ScaffoldIdentityOptionConfiguration(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    seed,
                    increment));
            }

            return result;
        }

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new List<ScaffoldCheckConstraintConfiguration>();
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName))
                {
                    continue;
                }

                var sql = NormalizeScaffoldCheckSql(feature.Detail);
                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                result.Add(new ScaffoldCheckConstraintConfiguration(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    sql));
            }

            return result;
        }

        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new List<ScaffoldExpressionIndexConfiguration>();
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "ExpressionIndex", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName))
                {
                    continue;
                }

                var expressionSql = ExtractCreateIndexExpressionSql(feature.Detail) ?? feature.Detail.Trim();
                if (expressionSql.EndsWith("expression index", StringComparison.OrdinalIgnoreCase))
                    continue;

                result.Add(new ScaffoldExpressionIndexConfiguration(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    expressionSql,
                    IsUnique: feature.Detail.Contains("CREATE UNIQUE", StringComparison.OrdinalIgnoreCase),
                    FilterSql: ExtractWhereClause(feature.Detail)));
            }

            return result;
        }

        private static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new List<ScaffoldCollationConfiguration>();
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "Collation", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                result.Add(new ScaffoldCollationConfiguration(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    feature.Detail.Trim()));
            }

            return result;
        }

        private static string? ExtractWhereClause(string sql)
        {
            var where = sql.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            return where < 0 ? null : sql[(where + 7)..].Trim();
        }

        private static string? ExtractCreateIndexExpressionSql(string? createIndexSql)
        {
            if (string.IsNullOrWhiteSpace(createIndexSql))
                return null;

            var onIndex = createIndexSql.IndexOf(" ON ", StringComparison.OrdinalIgnoreCase);
            if (onIndex < 0)
                return null;

            var openIndex = createIndexSql.IndexOf('(', onIndex);
            if (openIndex < 0)
                return null;

            var closeIndex = FindMatchingParenthesis(createIndexSql, openIndex);
            if (closeIndex <= openIndex)
                return null;

            return createIndexSql.Substring(openIndex + 1, closeIndex - openIndex - 1).Trim();
        }

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new List<ScaffoldComputedColumnConfiguration>();
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "Computed", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                var (sql, stored) = NormalizeScaffoldComputedSql(feature.Detail);
                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                result.Add(new ScaffoldComputedColumnConfiguration(
                    feature.TableKey,
                    entityName,
                    feature.Name,
                    propertyName,
                    sql,
                    stored));
            }

            return result;
        }

        private static (string Sql, bool Stored) NormalizeScaffoldComputedSql(string raw)
        {
            var candidate = raw.Trim();
            if (candidate.EndsWith("generated column", StringComparison.OrdinalIgnoreCase))
                return (string.Empty, false);
            var stored = candidate.Contains("PERSISTED", StringComparison.OrdinalIgnoreCase)
                || candidate.Contains(" STORED", StringComparison.OrdinalIgnoreCase);

            if (candidate.StartsWith("GENERATED", StringComparison.OrdinalIgnoreCase))
            {
                var open = candidate.IndexOf('(');
                var close = candidate.LastIndexOf(')');
                if (open >= 0 && close > open)
                    candidate = candidate.Substring(open + 1, close - open - 1).Trim();
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            candidate = TrimTrailingComputedStorageToken(candidate, "VIRTUAL");
            candidate = TrimTrailingComputedStorageToken(candidate, "STORED");
            candidate = TrimTrailingComputedStorageToken(candidate, "PERSISTED");

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            return (candidate, stored);
        }

        private static string TrimTrailingComputedStorageToken(string sql, string token)
        {
            var trimmed = sql.TrimEnd();
            if (!trimmed.EndsWith(token, StringComparison.OrdinalIgnoreCase))
                return sql;

            var tokenStart = trimmed.Length - token.Length;
            if (tokenStart > 0 && (char.IsLetterOrDigit(trimmed[tokenStart - 1]) || trimmed[tokenStart - 1] == '_'))
                return sql;

            return trimmed[..tokenStart].TrimEnd();
        }

        private static string NormalizeScaffoldCheckSql(string raw)
        {
            var candidate = raw.Trim();
            if (candidate.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase))
            {
                var open = candidate.IndexOf('(');
                var close = candidate.LastIndexOf(')');
                if (open >= 0 && close > open)
                    candidate = candidate.Substring(open + 1, close - open - 1).Trim();
            }

            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            return candidate;
        }

        private static bool TryNormalizeScaffoldDefaultSql(string? raw, out string defaultValueSql)
        {
            defaultValueSql = string.Empty;
            if (string.IsNullOrWhiteSpace(raw))
                return false;

            var candidate = raw.Trim();
            while (candidate.Length >= 2 && candidate[0] == '(' && candidate[^1] == ')' && HasBalancedOuterParentheses(candidate))
                candidate = candidate[1..^1].Trim();

            try
            {
                var validated = DefaultValueValidator.Validate(candidate);
                if (string.IsNullOrWhiteSpace(validated))
                    return false;

                defaultValueSql = validated;
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        private static bool HasBalancedOuterParentheses(string value)
        {
            var depth = 0;
            var inString = false;
            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                if (ch == '\'')
                {
                    if (inString && i + 1 < value.Length && value[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inString = !inString;
                    continue;
                }

                if (inString)
                    continue;

                if (ch == '(')
                    depth++;
                else if (ch == ')')
                    depth--;

                if (depth == 0 && i < value.Length - 1)
                    return false;
                if (depth < 0)
                    return false;
            }

            return depth == 0 && !inString;
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

        private static HashSet<string> BuildProviderNativeTemporalHistoryTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var feature in features)
            {
                if (string.Equals(feature.Kind, "TemporalTable", StringComparison.OrdinalIgnoreCase)
                    && feature.Detail.Contains("history table", StringComparison.OrdinalIgnoreCase))
                {
                    result.Add(feature.TableKey);
                }
            }

            return result;
        }

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
        {
            var kindSet = kinds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var feature in features)
            {
                if (!kindSet.Contains(feature.Kind)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = feature.Detail;
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static bool IsLegacyCascadeShape(ScaffoldRelationship relationship)
            => (string.Equals(relationship.OnDelete, "CASCADE", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(relationship.OnDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
               && string.Equals(relationship.OnUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase);

        private static string FormatReferentialAction(string action)
        {
            if (!TryParseReferentialAction(action, out var referentialAction))
                referentialAction = ReferentialAction.NoAction;

            return referentialAction switch
            {
                ReferentialAction.Cascade => "ReferentialAction.Cascade",
                ReferentialAction.SetNull => "ReferentialAction.SetNull",
                ReferentialAction.Restrict => "ReferentialAction.Restrict",
                ReferentialAction.SetDefault => "ReferentialAction.SetDefault",
                _ => "ReferentialAction.NoAction"
            };
        }

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var result = new Dictionary<string, Dictionary<string, ScaffoldDecimalPrecision>>(StringComparer.OrdinalIgnoreCase);
            foreach (var feature in features)
            {
                if (!string.Equals(feature.Kind, "PrecisionScale", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !TryParseDecimalPrecision(feature.Detail, out var precision, out var scale))
                {
                    continue;
                }

                if (!result.TryGetValue(feature.TableKey, out var table))
                {
                    table = new Dictionary<string, ScaffoldDecimalPrecision>(StringComparer.OrdinalIgnoreCase);
                    result[feature.TableKey] = table;
                }

                table[feature.Name] = new ScaffoldDecimalPrecision(precision, scale);
            }

            return result.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyDictionary<string, ScaffoldDecimalPrecision>)pair.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        private static bool TryParseDecimalPrecision(string? typeName, out int precision, out int scale)
        {
            precision = 0;
            scale = 0;
            if (string.IsNullOrWhiteSpace(typeName))
                return false;

            var open = typeName.IndexOf('(');
            var comma = typeName.IndexOf(',', open + 1);
            var close = typeName.IndexOf(')', comma + 1);
            if (open < 0 || comma < 0 || close < 0)
                return false;

            var baseName = typeName[..open].Trim();
            if (!baseName.EndsWith("decimal", StringComparison.OrdinalIgnoreCase)
                && !baseName.EndsWith("numeric", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return int.TryParse(typeName.AsSpan(open + 1, comma - open - 1), NumberStyles.None, CultureInfo.InvariantCulture, out precision)
                && int.TryParse(typeName.AsSpan(comma + 1, close - comma - 1), NumberStyles.None, CultureInfo.InvariantCulture, out scale)
                && precision > 0
                && scale >= 0
                && scale <= precision;
        }

        private static bool TryParseIdentityOptions(string? detail, out long seed, out long increment)
        {
            seed = 0;
            increment = 0;
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var open = detail.IndexOf('(');
            var comma = detail.IndexOf(',', open + 1);
            var close = detail.IndexOf(')', comma + 1);
            if (open < 0 || comma < 0 || close < 0)
                return false;

            return long.TryParse(detail.AsSpan(open + 1, comma - open - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out seed)
                && long.TryParse(detail.AsSpan(comma + 1, close - comma - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out increment)
                && increment != 0;
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

        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (provider is SqliteProvider)
            {
                var sqliteResult = new Dictionary<string, List<(int Ordinal, string Column)>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var keyColumns = new List<(int Ordinal, string Column)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        if (!ReaderHasColumn(reader, "name")
                            || !ReaderHasColumn(reader, "pk"))
                        {
                            continue;
                        }

                        var ordinal = Convert.ToInt32(reader["pk"], System.Globalization.CultureInfo.InvariantCulture);
                        if (ordinal <= 0)
                        {
                            continue;
                        }

                        var name = Convert.ToString(reader["name"]);
                        if (!string.IsNullOrWhiteSpace(name))
                            keyColumns.Add((ordinal, name));
                    }

                    sqliteResult[TableKey(table.Schema, table.Name)] = keyColumns;
                }

                return ToOrderedColumnDictionary(sqliteResult);
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName, ic.key_ordinal AS Ordinal
                    FROM sys.tables t
                    INNER JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
                    INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE t.is_ms_shipped = 0
                    ORDER BY SCHEMA_NAME(t.schema_id), t.name, ic.key_ordinal
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT ns.nspname AS TableSchema, cls.relname AS TableName, att.attname AS ColumnName, keys.ordinality AS Ordinal
                    FROM pg_constraint con
                    INNER JOIN pg_class cls ON cls.oid = con.conrelid
                    INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
                    INNER JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = keys.attnum
                    WHERE con.contype = 'p'
                      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY ns.nspname, cls.relname, keys.ordinality
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryOrderedColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName, ordinal_position AS Ordinal
                    FROM information_schema.key_column_usage
                    WHERE table_schema = DATABASE()
                      AND constraint_name = 'PRIMARY'
                    ORDER BY table_name, ordinal_position
                    """).ConfigureAwait(false);
            }

            var result = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in tables)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {EscapeQualified(provider, table.Schema, table.Name)} WHERE 1=0";
                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo).ConfigureAwait(false);
                var schema = reader.GetSchemaTable()!;
                var keyColumns = new List<string>();
                foreach (DataRow row in schema.Rows)
                {
                    if (row.Table.Columns.Contains("IsKey") && row["IsKey"] is bool isKey && isKey)
                        keyColumns.Add(row["ColumnName"]!.ToString()!);
                }

                result[TableKey(table.Schema, table.Name)] = keyColumns;
            }

            return result;
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> QueryOrderedColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = tableKeys.ToDictionary(
                key => key,
                _ => new List<(int Ordinal, string Column)>(),
                StringComparer.OrdinalIgnoreCase);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                var ordinal = ReaderHasColumn(reader, "Ordinal")
                    ? Convert.ToInt32(reader["Ordinal"], System.Globalization.CultureInfo.InvariantCulture)
                    : result[tableKey].Count + 1;
                result[tableKey].Add((ordinal, columnName));
            }

            return ToOrderedColumnDictionary(result);
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> ToOrderedColumnDictionary(
            Dictionary<string, List<(int Ordinal, string Column)>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlyList<string>)pair.Value
                    .OrderBy(item => item.Ordinal)
                    .Select(item => item.Column)
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var providerName = provider.GetType().Name;
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (provider is SqliteProvider)
            {
                var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var table in tables)
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = SqlitePragma(provider, table.Schema, "table_xinfo", table.Name);
                    await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                    var rows = new List<(string Name, string Type, int PrimaryKeyOrdinal)>();
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        rows.Add((
                            Convert.ToString(reader["name"]) ?? string.Empty,
                            Convert.ToString(reader["type"]) ?? string.Empty,
                            ReaderHasColumn(reader, "pk")
                                ? Convert.ToInt32(reader["pk"], System.Globalization.CultureInfo.InvariantCulture)
                                : 0));
                    }

                    var primaryKeyColumns = rows.Where(row => row.PrimaryKeyOrdinal > 0).ToArray();
                    if (primaryKeyColumns.Length != 1)
                        continue;

                    var key = primaryKeyColumns[0];
                    if (key.Type.Contains("INT", StringComparison.OrdinalIgnoreCase))
                    {
                        var tableKey = TableKey(table.Schema, table.Name);
                        result[tableKey] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { key.Name };
                    }
                }

                return ToReadOnlySetDictionary(result);
            }

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT SCHEMA_NAME(t.schema_id) AS TableSchema, t.name AS TableName, c.name AS ColumnName
                    FROM sys.identity_columns ic
                    INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    INNER JOIN sys.tables t ON t.object_id = ic.object_id
                    WHERE t.is_ms_shipped = 0
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT table_schema AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND (
                          is_identity = 'YES'
                          OR column_default LIKE 'nextval(%'
                      )
                    """).ConfigureAwait(false);
            }

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
            {
                return await QueryColumnNameMapAsync(connection, tableKeys, """
                    SELECT NULL AS TableSchema, table_name AS TableName, column_name AS ColumnName
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND LOWER(extra) LIKE '%auto_increment%'
                    """).ConfigureAwait(false);
            }

            return new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        private static async Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> QueryColumnNameMapAsync(
            DbConnection connection,
            HashSet<string> tableKeys,
            string sql)
        {
            var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var tableKey = TableKey(NullIfWhiteSpace(Convert.ToString(reader["TableSchema"])), Convert.ToString(reader["TableName"]) ?? string.Empty);
                if (!tableKeys.Contains(tableKey))
                    continue;

                var columnName = Convert.ToString(reader["ColumnName"]);
                if (string.IsNullOrWhiteSpace(columnName))
                    continue;

                if (!result.TryGetValue(tableKey, out var columns))
                {
                    columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[tableKey] = columns;
                }

                columns.Add(columnName);
            }

            return ToReadOnlySetDictionary(result);
        }

        private static IReadOnlyDictionary<string, IReadOnlySet<string>> ToReadOnlySetDictionary(
            Dictionary<string, HashSet<string>> source)
            => source.ToDictionary(
                pair => pair.Key,
                pair => (IReadOnlySet<string>)pair.Value,
                StringComparer.OrdinalIgnoreCase);

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

            return tables
                .OrderBy(t => t.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(t => t.Name, StringComparer.Ordinal)
                .ToArray();
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

            return tables
                .OrderBy(t => t.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(t => t.Name, StringComparer.Ordinal)
                .ToArray();
        }

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
            => ScaffoldContextWithRelationships(namespaceName, contextName, entities, Array.Empty<ScaffoldRelationship>(), Array.Empty<ScaffoldManyToManyJoin>());

        private static string ScaffoldContextWithRelationships(
            string namespaceName,
            string contextName,
            IEnumerable<string> entities,
            IReadOnlyList<ScaffoldRelationship> relationships,
            IReadOnlyList<ScaffoldManyToManyJoin> manyToManyJoins,
            IReadOnlyList<ScaffoldSkippedObject>? routineStubs = null,
            IReadOnlyList<ScaffoldPrimaryKey>? compositePrimaryKeys = null,
            IReadOnlyList<ScaffoldDefaultValueConfiguration>? defaultValueConfigurations = null,
            IReadOnlyList<ScaffoldCheckConstraintConfiguration>? checkConstraintConfigurations = null,
            IReadOnlyList<ScaffoldComputedColumnConfiguration>? computedColumnConfigurations = null,
            IReadOnlyList<ScaffoldExpressionIndexConfiguration>? expressionIndexConfigurations = null,
            IReadOnlyList<ScaffoldCollationConfiguration>? collationConfigurations = null,
            IReadOnlyList<ScaffoldSkippedObject>? sequenceStubs = null,
            IReadOnlyList<ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null)
        {
            compositePrimaryKeys ??= Array.Empty<ScaffoldPrimaryKey>();
            defaultValueConfigurations ??= Array.Empty<ScaffoldDefaultValueConfiguration>();
            checkConstraintConfigurations ??= Array.Empty<ScaffoldCheckConstraintConfiguration>();
            computedColumnConfigurations ??= Array.Empty<ScaffoldComputedColumnConfiguration>();
            expressionIndexConfigurations ??= Array.Empty<ScaffoldExpressionIndexConfiguration>();
            collationConfigurations ??= Array.Empty<ScaffoldCollationConfiguration>();
            sequenceStubs ??= Array.Empty<ScaffoldSkippedObject>();
            identityOptionConfigurations ??= Array.Empty<ScaffoldIdentityOptionConfiguration>();
            var sb = _stringBuilderPool.Get();
            try
            {
                sb.AppendLine("// <auto-generated/>");
                sb.AppendLine("#nullable enable");
                sb.AppendLine("using System.Data.Common;");
                if ((routineStubs?.Count > 0) || sequenceStubs.Count > 0)
                {
                    sb.AppendLine("using System.Collections.Generic;");
                    sb.AppendLine("using System.Threading;");
                    sb.AppendLine("using System.Threading.Tasks;");
                }
                sb.AppendLine("using System.Linq;");
                if (relationships.Count > 0 || manyToManyJoins.Count > 0 || compositePrimaryKeys.Count > 0)
                    sb.AppendLine("using System;");
                sb.AppendLine("using nORM.Core;");
                sb.AppendLine("using nORM.Configuration;");
                sb.AppendLine("using nORM.Providers;");
                sb.AppendLine();
                sb.AppendLine($"namespace {namespaceName};");
                sb.AppendLine();
                sb.AppendLine($"public class {EscapeCSharpIdentifier(contextName)} : DbContext");
                sb.AppendLine("{");
                if (relationships.Count == 0 && manyToManyJoins.Count == 0 && compositePrimaryKeys.Count == 0 && defaultValueConfigurations.Count == 0 && checkConstraintConfigurations.Count == 0 && computedColumnConfigurations.Count == 0 && expressionIndexConfigurations.Count == 0 && collationConfigurations.Count == 0 && identityOptionConfigurations.Count == 0)
                {
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(contextName)}(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null) : base(cn, provider, options) {{ }}");
                }
                else
                {
                    sb.AppendLine($"    public {EscapeCSharpIdentifier(contextName)}(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null) : base(cn, provider, ConfigureOptions(options)) {{ }}");
                }
                sb.AppendLine();
                var queryPropertyNames = CreateReservedContextMemberNameSet();
                foreach (var entity in entities.OrderBy(e => e))
                {
                    var safeEntity = EscapeCSharpIdentifier(entity);
                    var queryProperty = MakeUnique(Pluralize(safeEntity), queryPropertyNames);
                    sb.AppendLine($"    public IQueryable<{safeEntity}> {queryProperty} => this.Query<{safeEntity}>();");
                }

                if (routineStubs?.Count > 0)
                {
                    AppendRoutineStubs(sb, routineStubs, queryPropertyNames);
                }

                if (sequenceStubs.Count > 0)
                {
                    AppendSequenceStubs(sb, sequenceStubs, queryPropertyNames);
                }

                if (relationships.Count > 0 || manyToManyJoins.Count > 0 || compositePrimaryKeys.Count > 0 || defaultValueConfigurations.Count > 0 || checkConstraintConfigurations.Count > 0 || computedColumnConfigurations.Count > 0 || expressionIndexConfigurations.Count > 0 || collationConfigurations.Count > 0 || identityOptionConfigurations.Count > 0)
                {
                    sb.AppendLine();
                    sb.AppendLine("    private static DbContextOptions ConfigureOptions(DbContextOptions? options)");
                    sb.AppendLine("    {");
                    sb.AppendLine("        var configuredOptions = options?.Clone() ?? new DbContextOptions();");
                    sb.AppendLine("        var configure = configuredOptions.OnModelCreating;");
                    sb.AppendLine("        configuredOptions.OnModelCreating = mb =>");
                    sb.AppendLine("        {");
                    sb.AppendLine("            configure?.Invoke(mb);");
                    foreach (var key in compositePrimaryKeys
                        .OrderBy(k => k.EntityName, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(key.EntityName);
                        sb.AppendLine($"            mb.Entity<{entity}>().HasKey({FormatScaffoldKeySelector("e", key.PropertyNames)});");
                    }
                    foreach (var defaultValue in defaultValueConfigurations
                        .OrderBy(d => d.EntityName, StringComparer.Ordinal)
                        .ThenBy(d => d.PropertyName, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(defaultValue.EntityName);
                        var property = EscapeCSharpIdentifier(defaultValue.PropertyName);
                        var sql = EscapeStringLiteral(defaultValue.DefaultValueSql);
                        sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasDefaultValueSql(\"{sql}\");");
                    }
                    foreach (var identity in identityOptionConfigurations
                        .OrderBy(i => i.EntityName, StringComparer.Ordinal)
                        .ThenBy(i => i.PropertyName, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(identity.EntityName);
                        var property = EscapeCSharpIdentifier(identity.PropertyName);
                        sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasIdentityOptions({identity.Seed.ToString(CultureInfo.InvariantCulture)}, {identity.Increment.ToString(CultureInfo.InvariantCulture)});");
                    }
                    foreach (var check in checkConstraintConfigurations
                        .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                        .ThenBy(c => c.Name, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(check.EntityName);
                        var name = EscapeStringLiteral(check.Name);
                        var sql = EscapeStringLiteral(check.Sql);
                        sb.AppendLine($"            mb.Entity<{entity}>().HasCheckConstraint(\"{name}\", \"{sql}\");");
                    }
                    foreach (var computed in computedColumnConfigurations
                        .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                        .ThenBy(c => c.PropertyName, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(computed.EntityName);
                        var property = EscapeCSharpIdentifier(computed.PropertyName);
                        var sql = EscapeStringLiteral(computed.Sql);
                        var storedSuffix = computed.Stored ? ", stored: true" : string.Empty;
                        sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasComputedColumnSql(\"{sql}\"{storedSuffix});");
                    }
                    foreach (var collation in collationConfigurations
                        .OrderBy(c => c.EntityName, StringComparer.Ordinal)
                        .ThenBy(c => c.PropertyName, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(collation.EntityName);
                        var property = EscapeCSharpIdentifier(collation.PropertyName);
                        var value = EscapeStringLiteral(collation.Collation);
                        sb.AppendLine($"            mb.Entity<{entity}>().Property(e => e.{property}).HasCollation(\"{value}\");");
                    }
                    foreach (var expressionIndex in expressionIndexConfigurations
                        .OrderBy(i => i.EntityName, StringComparer.Ordinal)
                        .ThenBy(i => i.Name, StringComparer.Ordinal))
                    {
                        var entity = EscapeCSharpIdentifier(expressionIndex.EntityName);
                        var name = EscapeStringLiteral(expressionIndex.Name);
                        var expressionSql = EscapeStringLiteral(expressionIndex.ExpressionSql);
                        var uniqueSuffix = expressionIndex.IsUnique ? ", isUnique: true" : string.Empty;
                        var filterSuffix = string.IsNullOrWhiteSpace(expressionIndex.FilterSql) ? string.Empty : $", filterSql: \"{EscapeStringLiteral(expressionIndex.FilterSql)}\"";
                        sb.AppendLine($"            mb.Entity<{entity}>().HasExpressionIndex(\"{name}\", \"{expressionSql}\"{uniqueSuffix}{filterSuffix});");
                    }
                    foreach (var relationship in relationships
                        .OrderBy(r => r.PrincipalEntityName, StringComparer.Ordinal)
                        .ThenBy(r => r.DependentEntityName, StringComparer.Ordinal)
                        .ThenBy(r => r.CollectionNavigationName, StringComparer.Ordinal)
                        .ThenBy(r => r.ReferenceNavigationName, StringComparer.Ordinal)
                        .ThenBy(r => r.ForeignKeyPropertyName, StringComparer.Ordinal))
                    {
                        var principal = EscapeCSharpIdentifier(relationship.PrincipalEntityName);
                        var dependent = EscapeCSharpIdentifier(relationship.DependentEntityName);
                        var collection = EscapeCSharpIdentifier(relationship.CollectionNavigationName);
                        var reference = EscapeCSharpIdentifier(relationship.ReferenceNavigationName);
                        var foreignKey = FormatScaffoldKeySelector("d", relationship.ForeignKeyPropertyNames);
                        var principalKey = FormatScaffoldKeySelector("p", relationship.PrincipalKeyPropertyNames);
                        sb.AppendLine($"            mb.Entity<{principal}>()");
                        sb.AppendLine($"                .HasMany(p => p.{collection})");
                        sb.AppendLine($"                .WithOne(d => d.{reference})");
                        if (IsLegacyCascadeShape(relationship))
                        {
                            var cascadeSuffix = relationship.CascadeDelete ? string.Empty : ", cascadeDelete: false";
                            sb.AppendLine($"                .HasForeignKey({foreignKey}, {principalKey}{cascadeSuffix});");
                        }
                        else
                        {
                            sb.AppendLine($"                .HasForeignKey({foreignKey}, {principalKey}, {FormatReferentialAction(relationship.OnDelete)}, {FormatReferentialAction(relationship.OnUpdate)});");
                        }
                    }
                    foreach (var join in manyToManyJoins
                        .OrderBy(j => j.LeftEntityName, StringComparer.Ordinal)
                        .ThenBy(j => j.RightEntityName, StringComparer.Ordinal)
                        .ThenBy(j => j.LeftCollectionNavigationName, StringComparer.Ordinal)
                        .ThenBy(j => j.RightCollectionNavigationName, StringComparer.Ordinal)
                        .ThenBy(j => j.JoinTableKey, StringComparer.Ordinal))
                    {
                        var left = EscapeCSharpIdentifier(join.LeftEntityName);
                        var right = EscapeCSharpIdentifier(join.RightEntityName);
                        var collection = EscapeCSharpIdentifier(join.LeftCollectionNavigationName);
                        var inverseCollection = EscapeCSharpIdentifier(join.RightCollectionNavigationName);
                        var joinTable = EscapeStringLiteral(join.JoinTableName);
                        var joinSchema = join.JoinTableSchema is null ? null : EscapeStringLiteral(join.JoinTableSchema);
                        var leftKey = FormatScaffoldKeySelector("p", join.LeftPrincipalKeyProperties);
                        var rightKey = FormatScaffoldKeySelector("p", join.RightPrincipalKeyProperties);
                        sb.AppendLine($"            mb.Entity<{left}>()");
                        sb.AppendLine($"                .HasMany<{right}>(p => p.{collection})");
                        sb.AppendLine($"                .WithMany(p => p.{inverseCollection})");
                        if (joinSchema is null && !join.IsComposite && join.UsesPrimaryKeys)
                        {
                            var leftFk = EscapeStringLiteral(join.LeftForeignKeyColumn);
                            var rightFk = EscapeStringLiteral(join.RightForeignKeyColumn);
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", \"{leftFk}\", \"{rightFk}\");");
                        }
                        else if (joinSchema is null && join.UsesPrimaryKeys)
                        {
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)});");
                        }
                        else if (joinSchema is null && !join.IsComposite)
                        {
                            var leftFk = EscapeStringLiteral(join.LeftForeignKeyColumn);
                            var rightFk = EscapeStringLiteral(join.RightForeignKeyColumn);
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", \"{leftFk}\", \"{rightFk}\", {leftKey}, {rightKey});");
                        }
                        else if (joinSchema is null)
                        {
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey});");
                        }
                        else if (!join.IsComposite && join.UsesPrimaryKeys)
                        {
                            var leftFk = EscapeStringLiteral(join.LeftForeignKeyColumn);
                            var rightFk = EscapeStringLiteral(join.RightForeignKeyColumn);
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", \"{leftFk}\", \"{rightFk}\", schema: \"{joinSchema}\");");
                        }
                        else if (join.UsesPrimaryKeys)
                        {
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, schema: \"{joinSchema}\");");
                        }
                        else if (!join.IsComposite)
                        {
                            var leftFk = EscapeStringLiteral(join.LeftForeignKeyColumn);
                            var rightFk = EscapeStringLiteral(join.RightForeignKeyColumn);
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", \"{leftFk}\", \"{rightFk}\", {leftKey}, {rightKey}, schema: \"{joinSchema}\");");
                        }
                        else
                        {
                            sb.AppendLine($"                .UsingTable(\"{joinTable}\", {FormatStringArrayLiteral(join.LeftForeignKeyColumns)}, {FormatStringArrayLiteral(join.RightForeignKeyColumns)}, {leftKey}, {rightKey}, schema: \"{joinSchema}\");");
                        }
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

        private static void AppendRoutineStubs(StringBuilder sb, IReadOnlyList<ScaffoldSkippedObject> routineStubs, HashSet<string> memberNames)
        {
            foreach (var routine in routineStubs
                .OrderBy(r => r.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(r => r.Name, StringComparer.Ordinal))
            {
                var metadata = BuildSkippedObjectMetadata(routine);
                var routineType = Convert.ToString(metadata.TryGetValue("routineType", out var type) ? type : null) ?? "routine";
                var callShape = Convert.ToString(metadata.TryGetValue("callShape", out var shape) ? shape : null);
                var outputParameterCount = metadata.TryGetValue("outputParameterCount", out var outputCountValue) && outputCountValue is int outputCount
                    ? outputCount
                    : 0;
                var inputParameters = GetRoutineInputParameters(metadata);
                var outputParameters = GetRoutineOutputParameters(metadata);
                var discoveredInputParameterCount = GetRoutineInputParameterCount(metadata);
                var methodBase = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "Async", memberNames);
                var parameterType = inputParameters.Count > 0
                    ? MakeUnique(EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "Parameters", memberNames)
                    : null;
                var outputFactory = outputParameters.Count > 0
                    ? MakeUnique("Create" + EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "OutputParameters", memberNames)
                    : null;
                var routineNameExpression = FormatProviderEscapedRoutineName(routine);
                var parameterSummary = FormatRoutineParameterSummary(metadata);
                var isFunctionCallShape = IsFunctionCallShape(callShape);
                var isScalarFunction = string.Equals(callShape, "scalar-function", StringComparison.OrdinalIgnoreCase);

                if (parameterType != null)
                {
                    sb.AppendLine();
                    sb.AppendLine($"    public sealed class {parameterType}");
                    sb.AppendLine("    {");
                    foreach (var parameter in inputParameters)
                        sb.AppendLine($"        public {parameter.TypeName} {parameter.Name} {{ get; init; }}");
                    sb.AppendLine("    }");
                }

                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}`.</summary>");
                if (!string.IsNullOrWhiteSpace(parameterSummary))
                    sb.AppendLine($"    /// <remarks>Parameters discovered at scaffold time: {EscapeXmlDocumentation(parameterSummary)}. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                else
                    sb.AppendLine("    /// <remarks>Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                var requiresPositionalFunctionArguments = isFunctionCallShape
                    && discoveredInputParameterCount > 0
                    && inputParameters.Count == 0;
                var requiresDictionaryRoutineArguments = !isFunctionCallShape
                    && discoveredInputParameterCount > 0
                    && inputParameters.Count == 0;
                var parameterSignature = requiresPositionalFunctionArguments
                    ? "object?[]? arguments = null"
                    : requiresDictionaryRoutineArguments ? "IReadOnlyDictionary<string, object?>? parameters = null"
                    : parameterType == null ? "object? parameters = null" : $"{parameterType}? parameters = null";
                if (isFunctionCallShape)
                {
                    var streamMethod = isScalarFunction
                        ? null
                        : MakeUnique("Stream" + EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "Async", memberNames);
                    var scalarValueMethod = isScalarFunction
                        ? MakeUnique(EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "ValueAsync", memberNames)
                        : null;
                    var scalarValueType = isScalarFunction
                        ? MakeUnique(EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "ValueResult", memberNames)
                        : null;
                    AppendFunctionRoutineStub(
                        sb,
                        methodBase,
                        streamMethod,
                        scalarValueMethod,
                        scalarValueType,
                        routine,
                        parameterSignature,
                        parameterType,
                        inputParameters,
                        scalar: isScalarFunction,
                        usePositionalArguments: requiresPositionalFunctionArguments,
                        expectedArgumentCount: discoveredInputParameterCount);
                }
                else
                {
                    var storedProcedureParameters = FormatStoredProcedureParameterArgument(
                        routine,
                        discoveredInputParameterCount);
                    sb.AppendLine($"    public Task<List<TResult>> {methodBase}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
                    sb.AppendLine($"        => ExecuteStoredProcedureAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters});");

                    var streamMethod = MakeUnique("Stream" + EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "Async", memberNames);
                    sb.AppendLine();
                    sb.AppendLine($"    /// <summary>Streams provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows without buffering the full result set.</summary>");
                    sb.AppendLine("    /// <remarks>Use the buffered wrapper when output parameters are required. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                    sb.AppendLine($"    public IAsyncEnumerable<TResult> {streamMethod}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
                    sb.AppendLine($"        => ExecuteStoredProcedureAsAsyncEnumerable<TResult>({routineNameExpression}, ct, {storedProcedureParameters});");
                }

                if (outputParameterCount > 0 && !isFunctionCallShape)
                {
                    var outputMethod = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(routine.Name)) + "WithOutputAsync", memberNames);
                    var storedProcedureParameters = FormatStoredProcedureParameterArgument(
                        routine,
                        discoveredInputParameterCount);
                    sb.AppendLine();
                    sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with output parameters.</summary>");
                    sb.AppendLine("    /// <remarks>Pass explicit <see cref=\"OutputParameter\"/> definitions for provider output values. Routine bodies are provider-owned and are not translated by nORM.</remarks>");
                    sb.AppendLine($"    public Task<StoredProcedureResult<TResult>> {outputMethod}<TResult>({parameterSignature}, CancellationToken ct = default, params OutputParameter[] outputParameters) where TResult : class, new()");
                    sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters}, outputParameters);");

                    if (outputFactory != null)
                    {
                        sb.AppendLine();
                        sb.AppendLine($"    /// <summary>Executes provider-bound {EscapeXmlDocumentation(routineType)} `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` with output parameters discovered at scaffold time.</summary>");
                        sb.AppendLine("    /// <remarks>Use this overload when the scaffolded output parameter metadata still matches the database routine. Pass explicit output parameters to the overload with <c>params OutputParameter[]</c> after routine signature changes.</remarks>");
                        sb.AppendLine($"    public Task<StoredProcedureResult<TResult>> {outputMethod}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
                        sb.AppendLine($"        => ExecuteStoredProcedureWithOutputAsync<TResult>({routineNameExpression}, ct, {storedProcedureParameters}, {outputFactory}());");

                        sb.AppendLine();
                        sb.AppendLine($"    /// <summary>Creates output parameter definitions discovered for `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` at scaffold time.</summary>");
                        sb.AppendLine($"    public static OutputParameter[] {outputFactory}()");
                        sb.AppendLine("        => new[]");
                        sb.AppendLine("        {");
                        foreach (var parameter in outputParameters)
                        {
                            sb.AppendLine($"            {FormatRoutineOutputParameterCreation(parameter)},");
                        }
                        sb.AppendLine("        };");
                    }
                }
            }

            sb.AppendLine();
            sb.AppendLine("    private static object? RequireScaffoldedRoutineParameters(object? parameters, int expectedInputCount, string routineName)");
            sb.AppendLine("    {");
            sb.AppendLine("        if (expectedInputCount <= 0)");
            sb.AppendLine("            return parameters;");
            sb.AppendLine();
            sb.AppendLine("        if (parameters is null)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass a parameter object containing the scaffolded inputs.\");");
            sb.AppendLine();
            sb.AppendLine("        if (parameters is IReadOnlyDictionary<string, object?> dictionary && dictionary.Count != expectedInputCount)");
            sb.AppendLine("            throw new NormConfigurationException($\"Routine `{routineName}` was scaffolded with {expectedInputCount} input parameters; pass exactly {expectedInputCount} dictionary entries using the provider parameter names.\");");
            sb.AppendLine();
            sb.AppendLine("        return parameters;");
            sb.AppendLine("    }");
        }

        private static bool IsFunctionCallShape(string? callShape)
            => string.Equals(callShape, "table-valued-function", StringComparison.OrdinalIgnoreCase)
               || string.Equals(callShape, "scalar-function", StringComparison.OrdinalIgnoreCase);

        private static void AppendSequenceStubs(StringBuilder sb, IReadOnlyList<ScaffoldSkippedObject> sequenceStubs, HashSet<string> memberNames)
        {
            foreach (var sequence in sequenceStubs
                .OrderBy(s => s.Schema ?? string.Empty, StringComparer.Ordinal)
                .ThenBy(s => s.Name, StringComparer.Ordinal))
            {
                var provider = ParseSequenceProvider(sequence.Detail);
                if (provider is not ("sqlserver" or "postgres"))
                    continue;

                var valueType = MapSequenceValueType(sequence.Detail);
                var valueTypeName = GetTypeName(valueType, allowNull: false);
                var methodBase = MakeUnique("Next" + EscapeCSharpIdentifier(ToPascalCase(sequence.Name)) + "ValueAsync", memberNames);
                var resultType = MakeUnique(EscapeCSharpIdentifier(ToPascalCase(sequence.Name)) + "SequenceValue", memberNames);

                sb.AppendLine();
                sb.AppendLine($"    private sealed class {resultType}");
                sb.AppendLine("    {");
                sb.AppendLine($"        public {valueTypeName} Value {{ get; set; }}");
                sb.AppendLine("    }");
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Gets the next provider-bound value from sequence `{EscapeXmlDocumentation(QualifiedRoutineName(sequence))}`.</summary>");
                sb.AppendLine("    /// <remarks>Sequence DDL and allocation semantics remain provider-owned and are not translated by nORM.</remarks>");
                sb.AppendLine($"    public async Task<{valueTypeName}> {methodBase}(CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine($"        var rows = await QueryUnchangedAsync<{resultType}>({FormatSequenceSqlExpression(sequence, provider)}, ct).ConfigureAwait(false);");
                sb.AppendLine("        if (rows.Count == 0)");
                sb.AppendLine($"            throw new NormConfigurationException(\"Sequence `{EscapeStringLiteral(QualifiedRoutineName(sequence))}` did not return a value.\");");
                sb.AppendLine("        return rows[0].Value;");
                sb.AppendLine("    }");
            }
        }

        private static string ParseSequenceProvider(string detail)
        {
            if (detail.StartsWith("SQL Server", StringComparison.OrdinalIgnoreCase))
                return "sqlserver";
            if (detail.StartsWith("PostgreSQL", StringComparison.OrdinalIgnoreCase))
                return "postgres";
            return string.Empty;
        }

        private static Type MapSequenceValueType(string detail)
        {
            var dataType = ParseSemicolonValue(detail, "dataType");
            var normalized = dataType.Split('(', 2)[0].Trim().ToLowerInvariant();
            return normalized switch
            {
                "tinyint" => typeof(byte),
                "smallint" => typeof(short),
                "int" or "integer" => typeof(int),
                "bigint" => typeof(long),
                "decimal" or "numeric" => typeof(decimal),
                _ => typeof(long)
            };
        }

        private static string FormatSequenceSqlExpression(ScaffoldSkippedObject sequence, string provider)
        {
            var escapedValueAlias = " + Provider.Escape(\"Value\")";
            var sequenceName = FormatProviderEscapedRoutineName(sequence);
            if (provider == "sqlserver")
                return "\"SELECT NEXT VALUE FOR \" + " + sequenceName + " + \" AS \"" + escapedValueAlias;

            return "\"SELECT nextval('\" + (" + sequenceName + ").Replace(\"'\", \"''\") + \"'::regclass) AS \"" + escapedValueAlias;
        }

        private static void AppendFunctionRoutineStub(
            StringBuilder sb,
            string methodBase,
            string? streamMethod,
            string? scalarValueMethod,
            string? scalarValueType,
            ScaffoldSkippedObject routine,
            string parameterSignature,
            string? parameterType,
            IReadOnlyList<RoutineStubParameter> inputParameters,
            bool scalar,
            bool usePositionalArguments,
            int expectedArgumentCount)
        {
            sb.AppendLine($"    public Task<List<TResult>> {methodBase}<TResult>({parameterSignature}, CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            sb.AppendLine("        var placeholders = string.Join(\", \", System.Linq.Enumerable.Range(0, args.Length).Select(i => Provider.ParamPrefix + \"p\" + i));");
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            if (scalar)
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args);");
            else
                sb.AppendLine("        return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("    }");

            if (scalar && scalarValueType != null)
            {
                sb.AppendLine();
                sb.AppendLine($"    private sealed class {scalarValueType}<TValue>");
                sb.AppendLine("    {");
                sb.AppendLine("        public TValue? Value { get; set; }");
                sb.AppendLine("    }");
                sb.AppendLine();
                sb.AppendLine($"    /// <summary>Executes provider-bound scalar function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` and returns its single value.</summary>");
                sb.AppendLine("    /// <remarks>The routine body is provider-owned and is not translated by nORM.</remarks>");
                sb.AppendLine($"    public async Task<TValue?> {scalarValueMethod}<TValue>({parameterSignature}, CancellationToken ct = default)");
                sb.AppendLine("    {");
                sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments));
                AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
                sb.AppendLine("        var placeholders = string.Join(\", \", System.Linq.Enumerable.Range(0, args.Length).Select(i => Provider.ParamPrefix + \"p\" + i));");
                sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
                sb.AppendLine($"        var rows = await QueryUnchangedAsync<{scalarValueType}<TValue>>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\"), ct, args).ConfigureAwait(false);");
                sb.AppendLine("        return rows.Count == 0 ? default : rows[0].Value;");
                sb.AppendLine("    }");
            }

            if (streamMethod is null)
                return;

            sb.AppendLine();
            sb.AppendLine($"    /// <summary>Streams provider-bound table-valued function `{EscapeXmlDocumentation(QualifiedRoutineName(routine))}` rows without buffering the full result set.</summary>");
            sb.AppendLine("    /// <remarks>Routine bodies are provider-owned and are not translated by nORM.</remarks>");
            sb.AppendLine($"    public async IAsyncEnumerable<TResult> {streamMethod}<TResult>({parameterSignature}, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default) where TResult : class, new()");
            sb.AppendLine("    {");
            sb.AppendLine(FormatRoutineArgumentArray(parameterType, inputParameters, usePositionalArguments));
            AppendFunctionArgumentCountGuard(sb, routine, expectedArgumentCount);
            sb.AppendLine("        var placeholders = string.Join(\", \", System.Linq.Enumerable.Range(0, args.Length).Select(i => Provider.ParamPrefix + \"p\" + i));");
            sb.AppendLine($"        var invocation = {FormatProviderEscapedRoutineName(routine)} + \"(\" + placeholders + \")\";");
            sb.AppendLine("        var rows = QueryUnchangedStreamAsync<TResult>(\"SELECT * FROM \" + invocation, ct, args);");
            sb.AppendLine("        await foreach (var row in rows.ConfigureAwait(false))");
            sb.AppendLine("            yield return row;");
            sb.AppendLine("    }");
        }

        private static void AppendFunctionArgumentCountGuard(
            StringBuilder sb,
            ScaffoldSkippedObject routine,
            int expectedArgumentCount)
        {
            if (expectedArgumentCount <= 0)
                return;

            sb.AppendLine($"        if (args.Length != {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)})");
            sb.AppendLine($"            throw new NormConfigurationException(\"Function `{EscapeStringLiteral(QualifiedRoutineName(routine))}` was scaffolded with {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)} input parameters; pass exactly {expectedArgumentCount.ToString(CultureInfo.InvariantCulture)} arguments in scaffolded order.\");");
        }

        private static string FormatStoredProcedureParameterArgument(ScaffoldSkippedObject routine, int expectedInputParameterCount)
        {
            if (expectedInputParameterCount <= 0)
                return "parameters";

            return $"RequireScaffoldedRoutineParameters(parameters, {expectedInputParameterCount.ToString(CultureInfo.InvariantCulture)}, {FormatProviderEscapedRoutineName(routine)})";
        }

        private static string FormatRoutineArgumentArray(string? parameterType, IReadOnlyList<RoutineStubParameter> inputParameters, bool usePositionalArguments = false)
        {
            if (usePositionalArguments)
                return "        var args = arguments is null ? System.Array.Empty<object>() : System.Array.ConvertAll(arguments, value => (object)(value ?? System.DBNull.Value));";

            if (parameterType is null || inputParameters.Count == 0)
                return "        var args = System.Array.Empty<object>();";

            var values = inputParameters
                .Select(parameter => $"(object?)parameters.{parameter.Name} ?? System.DBNull.Value")
                .ToArray();
            return "        var args = parameters is null ? System.Array.Empty<object>() : new object[] { " + string.Join(", ", values) + " };";
        }

        private static string FormatProviderEscapedRoutineName(ScaffoldSkippedObject routine)
        {
            var name = EscapeStringLiteral(routine.Name);
            if (string.IsNullOrWhiteSpace(routine.Schema))
                return $"Provider.Escape(\"{name}\")";

            var schema = EscapeStringLiteral(routine.Schema!);
            return $"Provider.Escape(\"{schema}\") + \".\" + Provider.Escape(\"{name}\")";
        }

        private static string QualifiedRoutineName(ScaffoldSkippedObject routine)
            => string.IsNullOrWhiteSpace(routine.Schema) ? routine.Name : routine.Schema + "." + routine.Name;

        private static string ParseSemicolonValue(string detail, string key)
        {
            var segments = detail.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            foreach (var segment in segments)
            {
                var pair = segment.Split('=', 2);
                if (pair.Length == 2 && string.Equals(pair[0].Trim(), key, StringComparison.OrdinalIgnoreCase))
                    return pair[1].Trim();
            }

            return string.Empty;
        }

        private static string FormatRoutineParameterSummary(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return string.Empty;
            }

            return string.Join(", ", parameters.Select(parameter =>
            {
                var name = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null) ?? "parameter";
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                return string.Join(" ", new[] { name, mode, dataType }.Where(part => !string.IsNullOrWhiteSpace(part)));
            }));
        }

        private static IReadOnlyList<RoutineStubParameter> GetRoutineInputParameters(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return Array.Empty<RoutineStubParameter>();
            }

            var names = new List<RoutineStubParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var rawName = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null);
                var normalized = NormalizeRoutineParameterName(rawName);
                if (string.IsNullOrWhiteSpace(normalized))
                    return Array.Empty<RoutineStubParameter>();

                var escaped = EscapeCSharpIdentifier(normalized);
                if (!string.Equals(escaped.TrimStart('@'), normalized, StringComparison.Ordinal))
                    return Array.Empty<RoutineStubParameter>();

                if (!usedNames.Add(escaped))
                    return Array.Empty<RoutineStubParameter>();

                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                names.Add(new RoutineStubParameter(escaped, GetRoutineParameterTypeName(dataType)));
            }

            return names.ToArray();
        }

        private static int GetRoutineInputParameterCount(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return 0;
            }

            var count = 0;
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (!string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    count++;
                }
            }

            return count;
        }

        private static IReadOnlyList<RoutineOutputParameter> GetRoutineOutputParameters(IReadOnlyDictionary<string, object?> metadata)
        {
            if (!metadata.TryGetValue("parameters", out var parametersValue)
                || parametersValue is not IReadOnlyList<IReadOnlyDictionary<string, object?>> parameters
                || parameters.Count == 0)
            {
                return Array.Empty<RoutineOutputParameter>();
            }

            var names = new List<RoutineOutputParameter>();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var parameter in parameters)
            {
                var mode = Convert.ToString(parameter.TryGetValue("mode", out var m) ? m : null);
                if (!string.Equals(mode, "OUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "INOUT", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(mode, "RETURN", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var rawName = Convert.ToString(parameter.TryGetValue("name", out var n) ? n : null);
                var normalized = NormalizeRoutineParameterName(rawName);
                if (string.IsNullOrWhiteSpace(normalized))
                    return Array.Empty<RoutineOutputParameter>();

                var escaped = EscapeCSharpIdentifier(normalized);
                if (!string.Equals(escaped.TrimStart('@'), normalized, StringComparison.Ordinal))
                    return Array.Empty<RoutineOutputParameter>();

                if (!usedNames.Add(normalized))
                    return Array.Empty<RoutineOutputParameter>();

                var dataType = Convert.ToString(parameter.TryGetValue("dataType", out var d) ? d : null);
                names.Add(new RoutineOutputParameter(
                    normalized,
                    GetRoutineParameterDbTypeName(dataType),
                    GetRoutineParameterSize(dataType),
                    GetRoutineParameterDirection(mode)));
            }

            return names.ToArray();
        }

        private static string FormatRoutineOutputParameterCreation(RoutineOutputParameter parameter)
        {
            var baseCall = $"new OutputParameter(\"{EscapeStringLiteral(parameter.Name)}\", System.Data.DbType.{parameter.DbType}";
            var hasNonDefaultDirection = !string.Equals(parameter.Direction, nameof(ParameterDirection.Output), StringComparison.Ordinal);
            if (!parameter.Size.HasValue && !hasNonDefaultDirection)
                return baseCall + ")";

            var sizeArgument = parameter.Size.HasValue
                ? parameter.Size.Value.ToString(CultureInfo.InvariantCulture)
                : "null";
            if (!hasNonDefaultDirection)
                return baseCall + ", " + sizeArgument + ")";

            return baseCall + ", " + sizeArgument + ", System.Data.ParameterDirection." + parameter.Direction + ")";
        }

        private static string GetRoutineParameterDirection(string? mode)
            => mode?.Trim().ToUpperInvariant() switch
            {
                "INOUT" => nameof(ParameterDirection.InputOutput),
                "RETURN" => nameof(ParameterDirection.ReturnValue),
                _ => nameof(ParameterDirection.Output)
            };

        private static int? GetRoutineParameterSize(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return null;

            var trimmed = dataType.Trim();
            var open = trimmed.IndexOf('(');
            if (open < 0)
                return null;

            var close = trimmed.IndexOf(')', open + 1);
            if (close < 0)
                return null;

            var normalized = trimmed[..open].Trim().ToLowerInvariant();
            if (normalized is "character varying" or "varying character")
                normalized = "varchar";
            else if (normalized is "national character varying")
                normalized = "nvarchar";
            else if (normalized is "character")
                normalized = "char";

            if (normalized is not ("char" or "varchar" or "nchar" or "nvarchar" or "binary" or "varbinary"))
                return null;

            var value = trimmed.Substring(open + 1, close - open - 1).Trim();
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var size) && size > 0
                ? size
                : null;
        }

        private static string GetRoutineParameterTypeName(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return "object?";

            var normalized = NormalizeRoutineDataType(dataType);
            if (TryMapPostgresArrayRoutineType(normalized, out var arrayTypeName))
                return arrayTypeName + "?";

            normalized = normalized switch
            {
                "character varying" or "varying character" => "varchar",
                "national character varying" => "nvarchar",
                "character" => "char",
                "double precision" => "double",
                "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                "time without time zone" or "time with time zone" => "time",
                _ => normalized
            };

            return normalized switch
            {
                "int" or "integer" or "int4" or "mediumint" => "int?",
                "int unsigned" or "mediumint unsigned" => "uint?",
                "bigint" or "int8" => "long?",
                "bigint unsigned" => "ulong?",
                "smallint" or "int2" => "short?",
                "smallint unsigned" => "ushort?",
                "tinyint" => "byte?",
                "tinyint unsigned" => "byte?",
                "bit" or "bool" or "boolean" => "bool?",
                "decimal" or "numeric" or "money" or "smallmoney" => "decimal?",
                "float" or "float8" or "double" => "double?",
                "real" or "float4" => "float?",
                "date" => "DateOnly?",
                "time" => "TimeOnly?",
                "interval" => "TimeSpan?",
                "datetime" or "datetime2" or "smalldatetime" or "timestamp" => "DateTime?",
                "datetimeoffset" or "timestamptz" => "DateTimeOffset?",
                "uniqueidentifier" or "uuid" => "Guid?",
                "table type" => "DbParameter?",
                "sysname" => "string?",
                "bpchar" => "string?",
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "citext" or "xml" or "json" or "jsonb" or "enum" or "set" => "string?",
                "binary" or "varbinary" or "image" or "bytea" or "blob" or "longblob" or "mediumblob" or "tinyblob" => "byte[]?",
                _ => "object?"
            };
        }

        private static string GetRoutineParameterDbTypeName(string? dataType)
        {
            if (string.IsNullOrWhiteSpace(dataType))
                return nameof(DbType.Object);

            var normalized = NormalizeRoutineDataType(dataType);
            if (TryMapPostgresArrayRoutineType(normalized, out _))
                return nameof(DbType.Object);

            normalized = normalized switch
            {
                "character varying" or "varying character" => "varchar",
                "national character varying" => "nvarchar",
                "character" => "char",
                "double precision" => "double",
                "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                "time without time zone" or "time with time zone" => "time",
                _ => normalized
            };

            return normalized switch
            {
                "int" or "integer" or "int4" or "mediumint" => nameof(DbType.Int32),
                "int unsigned" or "mediumint unsigned" => nameof(DbType.UInt32),
                "bigint" or "int8" => nameof(DbType.Int64),
                "bigint unsigned" => nameof(DbType.UInt64),
                "smallint" or "int2" => nameof(DbType.Int16),
                "smallint unsigned" => nameof(DbType.UInt16),
                "tinyint" => nameof(DbType.Byte),
                "tinyint unsigned" => nameof(DbType.Byte),
                "bit" or "bool" or "boolean" => nameof(DbType.Boolean),
                "decimal" or "numeric" or "money" or "smallmoney" => nameof(DbType.Decimal),
                "float" or "float8" or "double" => nameof(DbType.Double),
                "real" or "float4" => nameof(DbType.Single),
                "date" => nameof(DbType.Date),
                "time" or "interval" => nameof(DbType.Time),
                "datetime" or "datetime2" or "smalldatetime" or "timestamp" => nameof(DbType.DateTime),
                "datetimeoffset" or "timestamptz" => nameof(DbType.DateTimeOffset),
                "uniqueidentifier" or "uuid" => nameof(DbType.Guid),
                "sysname" => nameof(DbType.String),
                "bpchar" => nameof(DbType.String),
                "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" or "citext" or "xml" or "json" or "jsonb" or "enum" or "set" => nameof(DbType.String),
                "binary" or "varbinary" or "image" or "bytea" or "blob" or "longblob" or "mediumblob" or "tinyblob" => nameof(DbType.Binary),
                _ => nameof(DbType.Object)
            };
        }

        private static string NormalizeRoutineDataType(string dataType)
        {
            var normalized = dataType.Trim().ToLowerInvariant();
            var isUnsigned = normalized.Contains(" unsigned", StringComparison.Ordinal);
            var paren = normalized.IndexOf('(');
            var baseType = paren >= 0 ? normalized[..paren].Trim() : normalized;

            if (paren >= 0 && (baseType == "array" || baseType == "user-defined" || baseType == "table type"))
            {
                var close = normalized.IndexOf(')', paren + 1);
                if (close > paren)
                {
                    var inner = normalized.Substring(paren + 1, close - paren - 1).Trim();
                    if (!string.IsNullOrWhiteSpace(inner))
                    {
                        if (baseType == "array")
                            return "array (" + inner + ")";

                        if (baseType == "table type")
                            return "table type";

                        var userDefined = inner.TrimStart('_');
                        if (userDefined is "citext" or "json" or "jsonb" or "xml" or "uuid")
                            return userDefined;
                    }
                }
            }

            normalized = isUnsigned && !baseType.EndsWith(" unsigned", StringComparison.Ordinal)
                ? baseType + " unsigned"
                : baseType;

            return normalized switch
            {
                "integer unsigned" => "int unsigned",
                "character varying" or "varying character" => "varchar",
                "national character varying" => "nvarchar",
                "character" => "char",
                "double precision" => "double",
                "timestamp without time zone" => "timestamp",
                "timestamp with time zone" => "timestamptz",
                "time without time zone" or "time with time zone" => "time",
                _ => normalized
            };
        }

        private static bool TryMapPostgresArrayRoutineType(string normalized, out string typeName)
        {
            typeName = string.Empty;
            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            var close = normalized.IndexOf(')', open + 1);
            if (open < 0 || close <= open)
                return false;

            var element = normalized.Substring(open + 1, close - open - 1).Trim().TrimStart('_');
            typeName = element switch
            {
                "int2" or "smallint" => "short[]",
                "int4" or "integer" => "int[]",
                "int8" or "bigint" => "long[]",
                "float4" or "real" => "float[]",
                "float8" or "double precision" => "double[]",
                "numeric" or "decimal" => "decimal[]",
                "bool" or "boolean" => "bool[]",
                "uuid" => "Guid[]",
                "text" or "varchar" or "character varying" or "bpchar" or "char" or "character" or "citext" => "string[]",
                "bytea" => "byte[][]",
                "date" => "DateOnly[]",
                "time" or "time without time zone" or "time with time zone" => "TimeOnly[]",
                "interval" => "TimeSpan[]",
                "timestamp" or "timestamp without time zone" => "DateTime[]",
                "timestamptz" or "timestamp with time zone" => "DateTimeOffset[]",
                _ => string.Empty
            };

            return typeName.Length > 0;
        }

        private static string NormalizeRoutineParameterName(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return string.Empty;

            return name.Trim().TrimStart('@', ':', '?');
        }

        private static string FormatScaffoldKeySelector(string parameterName, IReadOnlyList<string> propertyNames)
        {
            if (propertyNames.Count == 1)
                return $"{parameterName} => {parameterName}.{EscapeCSharpIdentifier(propertyNames[0])}";

            return $"{parameterName} => new {{ {string.Join(", ", propertyNames.Select(name => parameterName + "." + EscapeCSharpIdentifier(name)))} }}";
        }

        private static string FormatStringArrayLiteral(IReadOnlyList<string> values)
            => "new[] { " + string.Join(", ", values.Select(value => "\"" + EscapeStringLiteral(value) + "\"")) + " }";

        /// <summary>
        /// Returns a C# type name with correct nullability for value or reference types.
        /// </summary>
        private static string GetTypeName(Type type, bool allowNull)
        {
            string name = type.IsArray && type != typeof(byte[])
                ? GetTypeName(type.GetElementType()!, allowNull: false) + "[]"
                : type == typeof(byte[]) ? "byte[]" : type switch
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

        private static int? GetScaffoldMaxLength(Type clrType, DataRow row)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            if (!row.Table.Columns.Contains("ColumnSize") || row["ColumnSize"] == DBNull.Value)
                return null;

            return int.TryParse(row["ColumnSize"]!.ToString(), out var size) && size > 0 && size < int.MaxValue
                ? size
                : null;
        }

        private static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null, string? providerSpecificColumnType = null)
        {
            if (provider is SqliteProvider && IsSqliteUuidDeclaredType(declaredType))
                return typeof(Guid);

            if (provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase)
                && TryMapPostgresArrayType(providerSpecificColumnType, out var arrayType))
            {
                return arrayType;
            }

            if (provider is SqliteProvider
                && isKey
                && isAuto
                && !allowNull
                && clrType == typeof(int))
            {
                // SQLite INTEGER PRIMARY KEY aliases the 64-bit rowid even when
                // provider schema metadata reports Int32 for small test values.
                return typeof(long);
            }

            return clrType;
        }

        private static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
        {
            arrayType = typeof(object[]);
            if (string.IsNullOrWhiteSpace(detail))
                return false;

            var normalized = detail.Trim().ToLowerInvariant();
            if (!normalized.StartsWith("array", StringComparison.Ordinal))
                return false;

            var open = normalized.IndexOf('(');
            var close = normalized.IndexOf(')', open + 1);
            var element = open >= 0 && close > open
                ? normalized.Substring(open + 1, close - open - 1).Trim()
                : string.Empty;
            element = element.TrimStart('_');

            var elementType = element switch
            {
                "int2" or "smallint" => typeof(short),
                "int4" or "integer" => typeof(int),
                "int8" or "bigint" => typeof(long),
                "float4" or "real" => typeof(float),
                "float8" or "double precision" => typeof(double),
                "numeric" or "decimal" => typeof(decimal),
                "bool" or "boolean" => typeof(bool),
                "uuid" => typeof(Guid),
                "text" or "varchar" or "character varying" or "bpchar" or "char" or "character" or "citext" => typeof(string),
                "bytea" => typeof(byte[]),
                "date" => typeof(DateOnly),
                "time" or "time without time zone" or "time with time zone" => typeof(TimeOnly),
                "interval" => typeof(TimeSpan),
                "timestamp" or "timestamp without time zone" => typeof(DateTime),
                "timestamptz" or "timestamp with time zone" => typeof(DateTimeOffset),
                _ => null
            };

            if (elementType is null)
                return false;

            arrayType = elementType.MakeArrayType();
            return true;
        }

        private static bool IsSqliteUuidDeclaredType(string? declaredType)
            => !string.IsNullOrWhiteSpace(declaredType)
               && declaredType.Trim().ToUpperInvariant().Contains("UUID", StringComparison.Ordinal);

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

            if (identifier[0] == '@' && IsValidEscapedCSharpIdentifier(identifier))
                return identifier;

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

        private static bool IsValidEscapedCSharpIdentifier(string identifier)
        {
            if (identifier.Length == 1)
                return false;

            var first = identifier[1];
            if (!(char.IsLetter(first) || first == '_'))
                return false;

            for (var i = 2; i < identifier.Length; i++)
            {
                var ch = identifier[i];
                if (!(char.IsLetterOrDigit(ch) || ch == '_'))
                    return false;
            }

            return true;
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

        private readonly record struct RoutineStubParameter(
            string Name,
            string TypeName);

        private readonly record struct RoutineOutputParameter(
            string Name,
            string DbType,
            int? Size,
            string Direction);

        private readonly record struct ScaffoldPrimaryKey(
            string EntityName,
            string[] PropertyNames);

        private readonly record struct ScaffoldDefaultValueConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string DefaultValueSql);

        private readonly record struct ScaffoldIdentityOptionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            long Seed,
            long Increment);

        private readonly record struct ScaffoldCheckConstraintConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string Sql);

        private readonly record struct ScaffoldComputedColumnConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Sql,
            bool Stored);

        private readonly record struct ScaffoldExpressionIndexConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string ExpressionSql,
            bool IsUnique,
            string? FilterSql);

        private readonly record struct ScaffoldCollationConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Collation);

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
            int Ordinal,
            bool IsDescending,
            bool IsIncluded,
            string? FilterSql);

        private readonly record struct ScaffoldRelationship(
            string DependentTableKey,
            string PrincipalTableKey,
            string DependentEntityName,
            string PrincipalEntityName,
            string ForeignKeyPropertyName,
            string PrincipalKeyPropertyName,
            string ReferenceNavigationName,
            string CollectionNavigationName,
            bool CascadeDelete,
            string OnDelete,
            string OnUpdate)
        {
            public IReadOnlyList<string> ForeignKeyPropertyNames { get; init; } = new[] { ForeignKeyPropertyName };

            public IReadOnlyList<string> PrincipalKeyPropertyNames { get; init; } = new[] { PrincipalKeyPropertyName };

            public bool IsComposite => ForeignKeyPropertyNames.Count > 1 || PrincipalKeyPropertyNames.Count > 1;
        }

        private readonly record struct ScaffoldManyToManyJoin(
            string JoinTableKey,
            string LeftTableKey,
            string RightTableKey,
            string JoinTableName,
            string? JoinTableSchema,
            string LeftEntityName,
            string RightEntityName,
            string[] LeftForeignKeyColumns,
            string[] RightForeignKeyColumns,
            string[] LeftPrincipalKeyProperties,
            string[] RightPrincipalKeyProperties,
            bool UsesPrimaryKeys,
            string LeftCollectionNavigationName,
            string RightCollectionNavigationName)
        {
            public string LeftForeignKeyColumn => LeftForeignKeyColumns[0];

            public string RightForeignKeyColumn => RightForeignKeyColumns[0];

            public bool IsComposite => LeftForeignKeyColumns.Length > 1 || RightForeignKeyColumns.Length > 1;
        }

        private readonly record struct ScaffoldPossibleJoinTableDiagnostic(
            string TableKey,
            string[] PrincipalTables,
            string[] ConstraintNames,
            string[] Reasons);

        private readonly record struct ScaffoldManyToManyNavigation(
            string TargetEntityName,
            string CollectionNavigationName);

        private readonly record struct ScaffoldDecimalPrecision(
            int Precision,
            int Scale);

        private readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail);
    }
}
