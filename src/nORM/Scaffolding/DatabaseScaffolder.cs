#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
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
            var contextNamespace = NormalizeContextNamespace(namespaceName, options.ContextNamespace);
            var contextOutputDirectory = ResolveContextOutputDirectory(outputDirectory, options.ContextDirectory, options.ContextOutputDirectory);
            var safeContextName = EscapeCSharpIdentifier(ToPascalCase(contextName));

            var connectionWasOpen = connection.State == ConnectionState.Open;
            if (!connectionWasOpen)
                await connection.OpenAsync().ConfigureAwait(false);

            try
            {
                if (!options.DryRun)
                {
                    Directory.CreateDirectory(outputDirectory);
                    Directory.CreateDirectory(contextOutputDirectory);
                }
                var filterCatalog = GetScaffoldFilterCatalog(connection, provider);
                var discoveredTables = await GetTablesAsync(connection, provider).ConfigureAwait(false);
                var discoveredSkippedObjects = await GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
                var (tables, skippedObjects, queryArtifactTableKeys) = BuildScaffoldObjectSelection(
                    discoveredTables,
                    discoveredSkippedObjects,
                    options,
                    provider,
                    filterCatalog);
                var entityByTable = BuildEntityNameMap(tables, options.UseDatabaseNames);
                safeContextName = MakeUniqueContextName(safeContextName, entityByTable.Values);
                var columnPropertiesByTable = await GetColumnPropertyNamesAsync(connection, provider, tables, entityByTable, options.UseDatabaseNames).ConfigureAwait(false);
                var memberNamesByTable = BuildMemberNameMap(columnPropertiesByTable, entityByTable);
                var primaryKeyColumnsByTable = await GetPrimaryKeyColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var primaryKeyConstraintNamesByTable = await GetPrimaryKeyConstraintNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var nonNullableColumnsByTable = await GetNonNullableColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var sqliteDeclaredTypesByTable = provider is SqliteProvider
                    ? await GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false)
                    : new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
                var stringBinaryFacetsByTable = await GetStringBinaryFacetsAsync(connection, provider, tables).ConfigureAwait(false);
                var commentsByTable = await GetScaffoldCommentsAsync(connection, provider, tables).ConfigureAwait(false);
                var identityColumnsByTable = await GetIdentityColumnNamesAsync(connection, provider, tables).ConfigureAwait(false);
                var scaffoldedTableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var indexes = FilterIndexesToScaffoldedTables(
                    await GetIndexesAsync(connection, provider, tables).ConfigureAwait(false),
                    scaffoldedTableKeys);
                var foreignKeys = FilterForeignKeysToScaffoldedTables(
                    await GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false),
                    scaffoldedTableKeys);
                var unsupportedFeatures = (await GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
                unsupportedFeatures.AddRange(await GetPostgresEnumColumnFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
                RemoveSupportedDescendingIndexDiagnostics(unsupportedFeatures, indexes);
                RemoveSupportedIncludedColumnIndexDiagnostics(unsupportedFeatures, indexes);
                RemoveSupportedPartialIndexDiagnostics(unsupportedFeatures, indexes);
                AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable, columnPropertiesByTable);
                AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
                AddRelationshipPrincipalKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable, indexes);
                AddRelationshipDependentKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable);
                var featureConfigurations = BuildFeatureConfigurations(
                    unsupportedFeatures,
                    entityByTable,
                    columnPropertiesByTable,
                    stringBinaryFacetsByTable);
                var computedColumnsByTable = featureConfigurations.ComputedColumnsByTable;
                var rowVersionColumnsByTable = featureConfigurations.RowVersionColumnsByTable;
                var manyToManyJoins = BuildManyToManyJoins(foreignKeys, tables, entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, identityColumnsByTable, computedColumnsByTable, indexes, nonNullableColumnsByTable, featureConfigurations.ProviderOwnedWriteBlockedTableKeys, memberNamesByTable);
                var manyToManyJoinTableKeys = manyToManyJoins.Select(j => j.JoinTableKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
                var relationships = BuildRelationships(
                    foreignKeys.Where(fk => !manyToManyJoinTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))).ToArray(),
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    indexes,
                    nonNullableColumnsByTable,
                    memberNamesByTable);
                var compositePrimaryKeys = BuildPrimaryKeyConfigurations(entityByTable, columnPropertiesByTable, primaryKeyColumnsByTable, primaryKeyConstraintNamesByTable, manyToManyJoinTableKeys);
                var defaultValueConfigurations = BuildDefaultValueConfigurations(entityByTable, columnPropertiesByTable, featureConfigurations.DefaultValuesByTable);
                RestoreGeneratedManyToManyUnsupportedFeatures(unsupportedFeatures, featureConfigurations.GeneratedModelFeatureDiagnostics, manyToManyJoinTableKeys);
                defaultValueConfigurations = defaultValueConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var checkConstraints = featureConfigurations.CheckConstraints
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var computedColumnConfigurations = featureConfigurations.ComputedColumnConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var expressionIndexConfigurations = featureConfigurations.ExpressionIndexConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var collationConfigurations = featureConfigurations.CollationConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var identityOptionConfigurations = featureConfigurations.IdentityOptionConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var precisionConfigurations = featureConfigurations.PrecisionConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var columnFacetConfigurations = featureConfigurations.ColumnFacetConfigurations
                    .Where(config => !manyToManyJoinTableKeys.Contains(config.TableKey))
                    .ToArray();
                var entityFiles = await BuildScaffoldEntityFilesAsync(
                    connection,
                    provider,
                    outputDirectory,
                    namespaceName,
                    tables,
                    entityByTable,
                    columnPropertiesByTable,
                    primaryKeyColumnsByTable,
                    nonNullableColumnsByTable,
                    sqliteDeclaredTypesByTable,
                    stringBinaryFacetsByTable,
                    commentsByTable,
                    identityColumnsByTable,
                    indexes,
                    relationships,
                    manyToManyJoins,
                    manyToManyJoinTableKeys,
                    queryArtifactTableKeys,
                    featureConfigurations,
                    options).ConfigureAwait(false);
                var generatedFiles = entityFiles.GeneratedFiles.ToList();
                var entityNames = entityFiles.EntityNames;

                var routineStubs = options.EmitRoutineStubs
                    ? skippedObjects.Where(obj => string.Equals(obj.Kind, "Routine", StringComparison.OrdinalIgnoreCase)).ToArray()
                    : Array.Empty<ScaffoldSkippedObject>();
                var sequenceStubs = options.EmitSequenceStubs
                    ? skippedObjects.Where(obj => string.Equals(obj.Kind, "Sequence", StringComparison.OrdinalIgnoreCase)).ToArray()
                    : Array.Empty<ScaffoldSkippedObject>();
                var ctxCode = ScaffoldContextWithRelationships(contextNamespace, safeContextName, entityNames, relationships, manyToManyJoins, routineStubs, compositePrimaryKeys, defaultValueConfigurations, checkConstraints, computedColumnConfigurations, expressionIndexConfigurations, collationConfigurations, sequenceStubs, identityOptionConfigurations, precisionConfigurations, columnFacetConfigurations, options.PluralizeQueryProperties, options.UseNullableReferenceTypes, namespaceName, options.UseDatabaseNames);
                generatedFiles.Add((Path.Combine(contextOutputDirectory, safeContextName + ".cs"), ctxCode));
                var diagnostics = ScaffoldDiagnostics(foreignKeys, unsupportedFeatures, skippedObjects, primaryKeyColumnsByTable, indexes, columnPropertiesByTable, nonNullableColumnsByTable, computedColumnsByTable, identityColumnsByTable, featureConfigurations.ProviderOwnedWriteBlockedTableKeys, manyToManyJoinTableKeys);
                var diagnosticsJson = string.IsNullOrWhiteSpace(diagnostics)
                    ? null
                    : ScaffoldDiagnosticsJson(foreignKeys, unsupportedFeatures, skippedObjects, primaryKeyColumnsByTable, indexes, columnPropertiesByTable, nonNullableColumnsByTable, computedColumnsByTable, identityColumnsByTable, featureConfigurations.ProviderOwnedWriteBlockedTableKeys, manyToManyJoinTableKeys);
                await EmitScaffoldOutputAsync(outputDirectory, generatedFiles, diagnostics, diagnosticsJson, options).ConfigureAwait(false);
            }
            finally
            {
                // Only close the connection if we opened it
                if (!connectionWasOpen)
                    await connection.CloseAsync().ConfigureAwait(false);
            }
        }

        private static async Task<ScaffoldEntityFileSet> BuildScaffoldEntityFilesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string outputDirectory,
            string namespaceName,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> sqliteDeclaredTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable,
            IReadOnlyDictionary<string, ScaffoldComments> commentsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyList<ScaffoldRelationship> relationships,
            IReadOnlyList<ScaffoldManyToManyJoin> manyToManyJoins,
            IReadOnlySet<string> manyToManyJoinTableKeys,
            IReadOnlySet<string> queryArtifactTableKeys,
            ScaffoldFeatureConfigurations featureConfigurations,
            ScaffoldOptions options)
        {
            var entityNames = new List<string>();
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
                featureConfigurations.ComputedColumnsByTable.TryGetValue(tableKey, out var computedColumns);
                featureConfigurations.RowVersionColumnsByTable.TryGetValue(tableKey, out var rowVersionColumns);
                identityColumnsByTable.TryGetValue(tableKey, out var identityColumns);
                nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns);
                featureConfigurations.DecimalPrecisionByTable.TryGetValue(tableKey, out var decimalPrecisions);
                stringBinaryFacetsByTable.TryGetValue(tableKey, out var columnFacets);
                commentsByTable.TryGetValue(tableKey, out var comments);
                sqliteDeclaredTypesByTable.TryGetValue(tableKey, out var sqliteDeclaredTypes);
                featureConfigurations.ProviderSpecificColumnTypesByTable.TryGetValue(tableKey, out var providerSpecificColumnTypes);
                var isReadOnlyEntity = ShouldMarkScaffoldedEntityReadOnly(
                    tableKey,
                    queryArtifactTableKeys,
                    featureConfigurations.ProviderNativeTemporalTableKeys,
                    featureConfigurations.ProviderOwnedTriggerTableKeys,
                    featureConfigurations.ProviderSpecificIdentityStrategyTableKeys,
                    featureConfigurations.ProviderSpecificDefaultTableKeys,
                    providerSpecificColumnTypes,
                    primaryKeyColumnsByTable);
                var entityCode = await ScaffoldEntityAsync(connection, provider, schemaName, tableName, entityName, namespaceName, columnPropertyNames, tableIndexes, references, collections, manyToManyCollections, computedColumns, rowVersionColumns, identityColumns, decimalPrecisions, columnFacets, comments, isReadOnlyEntity, options.UseNullableReferenceTypes, nonNullableColumns, sqliteDeclaredTypes, providerSpecificColumnTypes).ConfigureAwait(false);
                generatedFiles.Add((Path.Combine(outputDirectory, entityName + ".cs"), entityCode));
            }

            return new ScaffoldEntityFileSet(generatedFiles, entityNames);
        }

        private static async Task EmitScaffoldOutputAsync(
            string outputDirectory,
            List<(string Path, string Content)> generatedFiles,
            string diagnostics,
            string? diagnosticsJson,
            ScaffoldOptions options)
            => await ScaffoldOutputManager.EmitAsync(
                outputDirectory,
                generatedFiles,
                diagnostics,
                diagnosticsJson,
                options).ConfigureAwait(false);

        private static Task<string> ScaffoldEntityAsync(
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
            IReadOnlyDictionary<string, ScaffoldColumnFacet>? columnFacets = null,
            ScaffoldComments? comments = null,
            bool isReadOnlyEntity = false,
            bool useNullableReferenceTypes = true,
            IReadOnlySet<string>? nonNullableColumns = null,
            IReadOnlyDictionary<string, string>? sqliteDeclaredTypes = null,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes = null)
            => ScaffoldEntitySourceBuilder.BuildAsync(new ScaffoldEntitySourceInfo(
                connection,
                provider,
                schemaName,
                tableName,
                entityName,
                namespaceName,
                columnPropertyNames,
                ConvertEntityIndexInfos(indexes),
                ConvertEntityReferenceInfos(references),
                ConvertEntityCollectionInfos(collections),
                ConvertEntityManyToManyNavigationInfos(manyToManyCollections),
                computedColumns,
                rowVersionColumns,
                identityColumns,
                ConvertEntityDecimalPrecisionInfos(decimalPrecisions),
                columnFacets,
                comments,
                isReadOnlyEntity,
                useNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                providerSpecificColumnTypes));

        private static ScaffoldEntityIndexSourceInfo[] ConvertEntityIndexInfos(IReadOnlyList<ScaffoldIndex>? indexes)
            => (indexes ?? Array.Empty<ScaffoldIndex>())
                .Select(index => new ScaffoldEntityIndexSourceInfo(
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql))
                .ToArray();

        private static ScaffoldEntityReferenceInfo[] ConvertEntityReferenceInfos(IReadOnlyList<ScaffoldRelationship>? references)
            => (references ?? Array.Empty<ScaffoldRelationship>())
                .Select(reference => new ScaffoldEntityReferenceInfo(
                    reference.PrincipalEntityName,
                    reference.ReferenceNavigationName,
                    reference.ForeignKeyPropertyName,
                    reference.IsComposite,
                    reference.IsRequired))
                .ToArray();

        private static ScaffoldEntityCollectionInfo[] ConvertEntityCollectionInfos(IReadOnlyList<ScaffoldRelationship>? collections)
            => (collections ?? Array.Empty<ScaffoldRelationship>())
                .Select(collection => new ScaffoldEntityCollectionInfo(
                    collection.DependentEntityName,
                    collection.CollectionNavigationName,
                    collection.ForeignKeyPropertyName,
                    collection.IsUniqueDependentKey))
                .ToArray();

        private static ScaffoldEntityManyToManyNavigationInfo[] ConvertEntityManyToManyNavigationInfos(IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections)
            => (manyToManyCollections ?? Array.Empty<ScaffoldManyToManyNavigation>())
                .Select(collection => new ScaffoldEntityManyToManyNavigationInfo(
                    collection.TargetEntityName,
                    collection.CollectionNavigationName))
                .ToArray();

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? ConvertEntityDecimalPrecisionInfos(
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions)
            => decimalPrecisions?.ToDictionary(
                pair => pair.Key,
                pair => new ScaffoldDecimalPrecisionInfo(pair.Value.Precision, pair.Value.Scale),
                StringComparer.OrdinalIgnoreCase);

        private static string BuildSchemaProbeSql(
            DatabaseProvider provider,
            string? schemaName,
            string tableName,
            IReadOnlyDictionary<string, string>? columnPropertyNames,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlySet<string>? postgresTextCastColumns = null)
            => ScaffoldEntitySourceBuilder.BuildSchemaProbeSql(
                provider,
                schemaName,
                tableName,
                columnPropertyNames,
                providerSpecificColumnTypes,
                postgresTextCastColumns);

        private static Task<IReadOnlySet<string>> GetPostgresUserDefinedColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            string? schemaName,
            string tableName)
            => ScaffoldEntitySourceBuilder.GetPostgresUserDefinedColumnNamesAsync(connection, provider, schemaName, tableName);

        private static bool RequiresPostgresSchemaProbeCast(string detail)
            => ScaffoldProviderSpecificTypeClassifier.RequiresPostgresSchemaProbeCast(detail);

        private static bool TryGetPostgresSchemaProbeCastType(string detail, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryGetPostgresSchemaProbeCastType(detail, out castType);

        private static string GetPostgresDomainProbeCastType(string detail)
            => ScaffoldProviderSpecificTypeClassifier.GetPostgresDomainProbeCastType(detail);

        private static bool TryGetPostgresDomainBaseTypeText(string? detail, out string typeText)
            => ScaffoldProviderSpecificTypeClassifier.TryGetPostgresDomainBaseTypeText(detail, out typeText);

        private static string NormalizePostgresDomainProbeCastType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizePostgresDomainProbeCastType(typeText);

        private static bool TryNormalizeSafePostgresDomainProbeCastType(string typeText, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizeSafePostgresDomainProbeCastType(typeText, out castType);

        private static bool TryNormalizePostgresParameterizedProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryNormalizePostgresParameterizedProbeCastType(normalized, out castType);

        private static bool TryParsePostgresTypeArguments(string normalized, string typeName, out string[] args)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresTypeArguments(normalized, typeName, out args);

        private static bool TryMapPostgresArrayProbeCastType(string normalized, out string castType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayProbeCastType(normalized, out castType);

        private static string? GetScaffoldFilterCatalog(DbConnection connection, DatabaseProvider provider)
            => IsMySqlProvider(provider) ? NullIfWhiteSpace(connection.Database) : null;

        private static bool IsMySqlProvider(DatabaseProvider provider)
            => provider.GetType().Name.Contains("MySql", StringComparison.OrdinalIgnoreCase);

        private static async Task<IReadOnlyList<ScaffoldTable>> GetTablesAsync(DbConnection connection, DatabaseProvider provider)
            => (await ScaffoldTableDiscovery.GetTablesAsync(connection, provider).ConfigureAwait(false))
                .Select(ToScaffoldTable)
                .ToArray();

        private static async Task<IReadOnlyList<ScaffoldSkippedObject>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
        {
            var discoveredObjects = await ScaffoldSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            if (discoveredObjects.Count == 0)
                return Array.Empty<ScaffoldSkippedObject>();

            var objects = new ScaffoldSkippedObject[discoveredObjects.Count];
            for (var i = 0; i < discoveredObjects.Count; i++)
            {
                var obj = discoveredObjects[i];
                objects[i] = new ScaffoldSkippedObject(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);
            }

            return objects;
        }

        private static IReadOnlyList<ScaffoldSkippedObjectInfo> ConvertSkippedObjectInfos(
            IReadOnlyList<ScaffoldSkippedObject> objects)
        {
            var converted = new ScaffoldSkippedObjectInfo[objects.Count];
            for (var i = 0; i < objects.Count; i++)
            {
                var obj = objects[i];
                converted[i] = new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment)
                {
                    Metadata = BuildSkippedObjectMetadata(obj)
                };
            }

            return converted;
        }

        private static Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
            => ScaffoldSkippedObjectDiscovery.GetSqliteSchemasAsync(connection);

        private static string SqliteSchemaResult(string schema)
            => ScaffoldSkippedObjectDiscovery.SqliteSchemaResult(schema);
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

        private static Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>>> GetStringBinaryFacetsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldColumnDiscovery.GetStringBinaryFacetsAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static Task<IReadOnlyDictionary<string, ScaffoldComments>> GetScaffoldCommentsAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldCommentDiscovery.GetScaffoldCommentsAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static ScaffoldTableInfo[] ToScaffoldTableInfos(IEnumerable<ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        private static ScaffoldTable ToScaffoldTable(ScaffoldTableInfo table)
            => new(table.Name, table.Schema);

        private static ScaffoldSkippedObjectInfo[] ToSkippedObjectInfos(IEnumerable<ScaffoldSkippedObject> objects)
            => objects.Select(ToSkippedObjectInfo).ToArray();

        private static ScaffoldSkippedObjectInfo ToSkippedObjectInfo(ScaffoldSkippedObject obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        private static ScaffoldSkippedObject ToScaffoldSkippedObject(ScaffoldSkippedObjectInfo obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        private static (IReadOnlyList<ScaffoldTable> Tables, IReadOnlyList<ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys) BuildScaffoldObjectSelection(
            IReadOnlyList<ScaffoldTable> discoveredTables,
            IReadOnlyList<ScaffoldSkippedObject> discoveredSkippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            var selection = ScaffoldObjectSelectionBuilder.BuildSelection(
                discoveredTables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                ConvertSkippedObjectInfos(discoveredSkippedObjects),
                options,
                provider,
                filterCatalog);
            return (
                selection.Tables.Select(ToScaffoldTable).ToArray(),
                selection.SkippedObjects.Select(ToScaffoldSkippedObject).ToArray(),
                selection.QueryArtifactTableKeys);
        }

        private static IReadOnlyList<ScaffoldTable> FilterTables(
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
            => ScaffoldTableFilter.FilterTables(
                    ToScaffoldTableInfos(tables),
                    ToSkippedObjectInfos(skippedObjects),
                    options,
                    provider,
                    filterCatalog)
                .Select(ToScaffoldTable)
                .ToArray();

        private static void EnsureNoTableKeyCollisions(IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldTableFilter.EnsureNoTableKeyCollisions(ToScaffoldTableInfos(tables));

        private static IReadOnlyList<ScaffoldSkippedObject> FilterSkippedObjects(
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog,
            IReadOnlyList<ScaffoldSkippedObject>? emittedQueryArtifacts = null)
            => ScaffoldTableFilter.FilterSkippedObjects(
                    ToSkippedObjectInfos(skippedObjects),
                    options,
                    provider,
                    filterCatalog,
                    emittedQueryArtifacts is null ? null : ToSkippedObjectInfos(emittedQueryArtifacts))
                .Select(ToScaffoldSkippedObject)
                .ToArray();

        private static bool IsQueryArtifactObject(ScaffoldSkippedObject obj)
            => ScaffoldTableFilter.IsQueryArtifactObject(ToSkippedObjectInfo(obj));

        private static bool ShouldEmitQueryArtifactObject(ScaffoldSkippedObject obj, ScaffoldOptions options, DatabaseProvider provider, string? filterCatalog)
            => ScaffoldTableFilter.ShouldEmitQueryArtifactObject(ToSkippedObjectInfo(obj), options, provider, filterCatalog);

        private static bool IsTableLikeSqlServerSynonym(string detail)
            => ScaffoldSkippedObjectMetadataBuilder.IsTableLikeSqlServerSynonym(detail);

        private static string[] GetRequestedTableFilters(ScaffoldOptions options)
            => ScaffoldTableFilter.GetRequestedTableFilters(options);

        private static string[] GetRequestedSchemaFilters(ScaffoldOptions options)
            => ScaffoldTableFilter.GetRequestedSchemaFilters(options);

        private static string NormalizeContextNamespace(string entityNamespaceName, string? contextNamespaceName)
            => ScaffoldOutputManager.NormalizeContextNamespace(entityNamespaceName, contextNamespaceName);

        private static string ResolveContextOutputDirectory(string outputDirectory, string? contextDirectory, string? contextOutputDirectory)
            => ScaffoldOutputManager.ResolveContextOutputDirectory(outputDirectory, contextDirectory, contextOutputDirectory);

        private static void EnsureNoOutputFileConflicts(IEnumerable<string> paths, ScaffoldOptions options)
            => ScaffoldOutputManager.EnsureNoOutputFileConflicts(paths, options);

        private static void EnsureNoStaleScaffoldWarningReports(string outputDirectory, ScaffoldOptions options)
            => ScaffoldOutputManager.EnsureNoStaleScaffoldWarningReports(outputDirectory, options);

        private static async Task WriteGeneratedFileAsync(string path, string content)
            => await ScaffoldOutputManager.WriteGeneratedFileAsync(path, content).ConfigureAwait(false);

        private static async Task<IReadOnlyList<ScaffoldIndex>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var indexes = await ScaffoldIndexDiscovery.GetIndexesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            return ConvertIndexes(indexes);
        }

        private static IReadOnlyList<ScaffoldIndex> ConvertIndexes(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var converted = new ScaffoldIndex[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new ScaffoldIndex(
                    index.TableKey,
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql,
                    index.IsSyntheticName);
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldIndex> FilterIndexesToScaffoldedTables(
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> scaffoldedTableKeys)
            => indexes
                .Where(index => scaffoldedTableKeys.Contains(index.TableKey))
                .ToArray();

        private static async Task<IReadOnlyList<ScaffoldForeignKey>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var foreignKeys = await ScaffoldForeignKeyDiscovery.GetForeignKeysAsync(connection, provider, tableInfos).ConfigureAwait(false);
            return ConvertForeignKeys(foreignKeys);
        }

        private static IReadOnlyList<ScaffoldForeignKey> ConvertForeignKeys(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var converted = new ScaffoldForeignKey[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new ScaffoldForeignKey(
                    foreignKey.DependentSchema,
                    foreignKey.DependentTable,
                    foreignKey.DependentColumn,
                    foreignKey.PrincipalSchema,
                    foreignKey.PrincipalTable,
                    foreignKey.PrincipalColumn,
                    foreignKey.ConstraintName,
                    foreignKey.ColumnCount,
                    foreignKey.OnDelete,
                    foreignKey.OnUpdate,
                    foreignKey.IsSyntheticConstraintName);
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

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
            var providerName = provider.GetType().Name;

            if (provider is SqliteProvider)
                return await GetSqliteUnsupportedSchemaFeaturesAsync(connection, provider, tables, tableKeys).ConfigureAwait(false);

            if (providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase))
                return await GetSqlServerUnsupportedSchemaFeaturesAsync(connection, tableKeys).ConfigureAwait(false);

            if (providerName.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return await GetPostgresUnsupportedSchemaFeaturesAsync(connection, tableKeys).ConfigureAwait(false);

            if (providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase))
                return await GetMySqlUnsupportedSchemaFeaturesAsync(connection, provider, tables, tableKeys).ConfigureAwait(false);

            return Array.Empty<ScaffoldUnsupportedFeature>();
        }

        private static IReadOnlyList<ScaffoldUnsupportedFeature> ConvertUnsupportedFeatures(
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> features)
        {
            var converted = new ScaffoldUnsupportedFeature[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                converted[i] = new ScaffoldUnsupportedFeature(
                    feature.TableKey,
                    feature.Kind,
                    feature.Name,
                    feature.Detail)
                {
                    Metadata = feature.Metadata
                };
            }

            return converted;
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetSqliteUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            HashSet<string> tableKeys)
        {
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var features = await ScaffoldSqliteUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, tableInfos, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetSqlServerUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldSqlServerUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetPostgresUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldPostgresUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetMySqlUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            HashSet<string> tableKeys)
        {
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var features = await ScaffoldMySqlUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, tableInfos, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }



        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetPostgresEnumColumnFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return Array.Empty<ScaffoldUnsupportedFeature>();

            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var features = await ScaffoldPostgresUnsupportedFeatureDiscovery.GetEnumColumnFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static void AddMissingPrimaryKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            foreach (var table in tables)
            {
                var tableKey = TableKey(table.Schema, table.Name);
                if (primaryKeyColumnsByTable.TryGetValue(tableKey, out var keys) && keys.Count > 0)
                    continue;

                columnPropertiesByTable.TryGetValue(tableKey, out var properties);
                var columnNames = properties?.Keys.ToArray() ?? Array.Empty<string>();
                var propertyNames = properties?.Values.ToArray() ?? Array.Empty<string>();
                features.Add(new ScaffoldUnsupportedFeature(
                    tableKey,
                    "MissingPrimaryKey",
                    table.Name,
                    "Table has no primary key; generated entity is a query/bootstrap artifact until a key is configured.")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["table"] = tableKey,
                        ["columns"] = columnNames,
                        ["properties"] = propertyNames,
                        ["columnCount"] = columnNames.Length,
                        ["reason"] = "missing-primary-key"
                    }
                });
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

                var rows = group.ToArray();
                var dependentKey = TableKey(fk.DependentSchema, fk.DependentTable);
                var principalKey = TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
                var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
                features.Add(new ScaffoldUnsupportedFeature(
                    dependentKey,
                    "ReferentialAction",
                    fk.ConstraintName,
                    $"ON DELETE {onDelete}; ON UPDATE {onUpdate}")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["dependentTable"] = dependentKey,
                        ["dependentColumns"] = dependentColumns,
                        ["principalTable"] = principalKey,
                        ["principalColumns"] = principalColumns,
                        ["columnCount"] = rows.Length,
                        ["onDelete"] = onDelete,
                        ["onUpdate"] = onUpdate
                    }
                });
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
            => features.AddRange(ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildPrincipalKeyDiagnostics(
                    ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable,
                    ConvertIndexInfos(indexes))));

        private static void AddRelationshipDependentKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => features.AddRange(ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildDependentKeyDiagnostics(
                    ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable)));

        private static IReadOnlyList<(string Name, string Sql)> ExtractSqliteCheckConstraints(string tableName, string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractCheckConstraints(tableName, createTableSql);

        private static IReadOnlyDictionary<string, (string Sql, bool Stored)> ExtractSqliteGeneratedColumns(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractGeneratedColumns(createTableSql);


        private static int FindMatchingParenthesis(string sql, int openIndex)
            => ScaffoldSqliteDdlParser.FindMatchingParenthesis(sql, openIndex);

        private static IReadOnlyDictionary<string, string> ExtractSqliteColumnCollations(string? createTableSql)
            => ScaffoldSqliteDdlParser.ExtractColumnCollations(createTableSql);

        private static IReadOnlyList<string> SplitTopLevelCommaSeparated(string sql)
            => ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(sql);

        private static bool IsSqliteProviderSpecificDeclaredType(string? declaredType)
            => ScaffoldSqliteDdlParser.IsProviderSpecificDeclaredType(declaredType);

        private static bool IsUnsafeSqliteProviderSpecificDeclaredType(string normalizedDeclaredType)
            => ScaffoldSqliteDdlParser.IsUnsafeProviderSpecificDeclaredType(normalizedDeclaredType);

        private static bool ContainsSqliteDeclaredTypeToken(string normalizedDeclaredType, string token)
            => ScaffoldSqliteDdlParser.ContainsDeclaredTypeToken(normalizedDeclaredType, token);

        private static ScaffoldFeatureConfigurations BuildFeatureConfigurations(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> stringBinaryFacetsByTable)
        {
            var featureInputs = ConvertFeatureInputs(unsupportedFeatures);
            var configurations = ScaffoldFeatureConfigurationBuilder.BuildFeatureConfigurations(
                featureInputs,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);
            var generatedFeatureIndexes = configurations.GeneratedFeatureIndexes.ToArray();
            var generatedFeatureIndexSet = generatedFeatureIndexes.ToHashSet();
            var generatedModelFeatureDiagnostics = generatedFeatureIndexes
                .Select(index => unsupportedFeatures[index])
                .ToArray();
            for (var i = unsupportedFeatures.Count - 1; i >= 0; i--)
            {
                if (generatedFeatureIndexSet.Contains(i))
                    unsupportedFeatures.RemoveAt(i);
            }

            return ConvertFeatureConfigurations(configurations, generatedModelFeatureDiagnostics);
        }

        private static IReadOnlyList<ScaffoldFeatureInput> ConvertFeatureInputs(IReadOnlyList<ScaffoldUnsupportedFeature> features)
        {
            var converted = new ScaffoldFeatureInput[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                converted[i] = new ScaffoldFeatureInput(i, ConvertUnsupportedFeatureInputInfo(features[i]));
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldFeatureInput> ConvertFeatureInputs(IEnumerable<ScaffoldUnsupportedFeature> features)
        {
            var featureList = features as IReadOnlyList<ScaffoldUnsupportedFeature> ?? features.ToArray();
            return ConvertFeatureInputs(featureList);
        }

        private static ScaffoldUnsupportedFeatureInfo ConvertUnsupportedFeatureInputInfo(ScaffoldUnsupportedFeature feature)
            => new(feature.TableKey, feature.Kind, feature.Name, feature.Detail)
            {
                Metadata = feature.Metadata
            };

        private static ScaffoldFeatureInput ConvertFeatureInput(ScaffoldUnsupportedFeature feature)
            => new(0, ConvertUnsupportedFeatureInputInfo(feature));

        private static ScaffoldFeatureConfigurations ConvertFeatureConfigurations(
            ScaffoldFeatureConfigurationsInfo configurations,
            IReadOnlyList<ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics)
            => new(
                generatedModelFeatureDiagnostics,
                configurations.ProviderSpecificColumnTypesByTable,
                configurations.DefaultValuesByTable,
                configurations.ProviderSpecificDefaultTableKeys,
                ConvertCheckConstraintConfigurations(configurations.CheckConstraints),
                ConvertExpressionIndexConfigurations(configurations.ExpressionIndexConfigurations),
                ConvertCollationConfigurations(configurations.CollationConfigurations),
                ConvertComputedColumnConfigurations(configurations.ComputedColumnConfigurations),
                configurations.ComputedColumnsByTable,
                ConvertDecimalPrecisionMap(configurations.DecimalPrecisionByTable),
                ConvertPrecisionConfigurations(configurations.PrecisionConfigurations),
                ConvertColumnFacetConfigurations(configurations.ColumnFacetConfigurations),
                configurations.RowVersionColumnsByTable,
                configurations.ProviderNativeTemporalTableKeys,
                configurations.ProviderOwnedTriggerTableKeys,
                ConvertIdentityOptionConfigurations(configurations.IdentityOptionConfigurations),
                configurations.ProviderSpecificIdentityStrategyTableKeys,
                configurations.ProviderOwnedWriteBlockedTableKeys);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> ConvertCheckConstraintConfigurations(
            IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> checks)
            => checks
                .Select(static check => new ScaffoldCheckConstraintConfiguration(check.TableKey, check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldCheckConstraintConfigurationInfo ConvertCheckConstraintConfiguration(
            ScaffoldCheckConstraintConfiguration check)
            => new(check.TableKey, check.EntityName, check.Name, check.Sql);

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> ConvertDefaultValueConfigurations(
            IReadOnlyList<ScaffoldDefaultValueConfigurationInfo> defaultValues)
            => defaultValues
                .Select(static value => new ScaffoldDefaultValueConfiguration(value.TableKey, value.EntityName, value.ColumnName, value.PropertyName, value.DefaultValueSql))
                .ToArray();

        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> ConvertExpressionIndexConfigurations(
            IReadOnlyList<ScaffoldExpressionIndexConfigurationInfo> expressionIndexes)
            => expressionIndexes
                .Select(static index => new ScaffoldExpressionIndexConfiguration(index.TableKey, index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql))
                .ToArray();

        private static IReadOnlyList<ScaffoldCollationConfiguration> ConvertCollationConfigurations(
            IReadOnlyList<ScaffoldCollationConfigurationInfo> collations)
            => collations
                .Select(static collation => new ScaffoldCollationConfiguration(collation.TableKey, collation.EntityName, collation.ColumnName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> ConvertComputedColumnConfigurations(
            IReadOnlyList<ScaffoldComputedColumnConfigurationInfo> computedColumns)
            => computedColumns
                .Select(static computed => new ScaffoldComputedColumnConfiguration(computed.TableKey, computed.EntityName, computed.ColumnName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> ConvertDecimalPrecisionMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecision>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecision(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>> ConvertDecimalPrecisionInfoMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => decimalPrecisionByTable.ToDictionary(
                table => table.Key,
                table => (IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>)table.Value.ToDictionary(
                    column => column.Key,
                    column => new ScaffoldDecimalPrecisionInfo(column.Value.Precision, column.Value.Scale),
                    StringComparer.OrdinalIgnoreCase),
                StringComparer.OrdinalIgnoreCase);

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> ConvertPrecisionConfigurations(
            IReadOnlyList<ScaffoldPrecisionConfigurationInfo> precisionConfigurations)
            => precisionConfigurations
                .Select(static precision => new ScaffoldPrecisionConfiguration(precision.TableKey, precision.EntityName, precision.ColumnName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> ConvertColumnFacetConfigurations(
            IReadOnlyList<ScaffoldColumnFacetConfigurationInfo> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(static facet => new ScaffoldColumnFacetConfiguration(facet.TableKey, facet.EntityName, facet.ColumnName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> ConvertIdentityOptionConfigurations(
            IReadOnlyList<ScaffoldIdentityOptionConfigurationInfo> identityOptions)
            => identityOptions
                .Select(static identity => new ScaffoldIdentityOptionConfiguration(identity.TableKey, identity.EntityName, identity.ColumnName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();

        private static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
            IReadOnlySet<string> manyToManyJoinTableKeys)
        {
            foreach (var feature in generatedModelFeatureDiagnostics)
            {
                if (!ShouldRestoreGeneratedManyToManyUnsupportedFeature(feature.Kind)
                    || !manyToManyJoinTableKeys.Contains(feature.TableKey)
                    || unsupportedFeatures.Contains(feature))
                {
                    continue;
                }

                unsupportedFeatures.Add(feature);
            }
        }

        private static bool ShouldRestoreGeneratedManyToManyUnsupportedFeature(string kind)
            => !string.Equals(kind, "Computed", StringComparison.OrdinalIgnoreCase);

        private static bool IsScaffoldableProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsScaffoldableProviderSpecificColumnType(detail);

        private static bool IsSafePostgresUserDefinedScalarColumnType(string normalized)
            => ScaffoldProviderSpecificTypeClassifier.IsSafePostgresUserDefinedScalarColumnType(normalized);

        private static bool HasWriteBlockingProviderSpecificColumnTypes(IReadOnlyDictionary<string, string>? providerSpecificColumnTypes)
            => ScaffoldProviderSpecificTypeClassifier.HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes);

        private static bool ShouldMarkScaffoldedEntityReadOnly(
            string tableKey,
            IReadOnlySet<string> queryArtifactTableKeys,
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, string>? providerSpecificColumnTypes,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => queryArtifactTableKeys.Contains(tableKey)
               || providerNativeTemporalTableKeys.Contains(tableKey)
               || providerOwnedTriggerTableKeys.Contains(tableKey)
               || providerSpecificIdentityStrategyTableKeys.Contains(tableKey)
               || providerSpecificDefaultTableKeys.Contains(tableKey)
               || HasWriteBlockingProviderSpecificColumnTypes(providerSpecificColumnTypes)
               || !primaryKeyColumnsByTable.TryGetValue(tableKey, out var primaryKeyColumns)
               || primaryKeyColumns.Count == 0;

        private static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsWriteBlockingProviderSpecificColumnType(detail);

        private static bool IsSafeMySqlUnsignedDecimalType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeMySqlUnsignedDecimalType(detail);

        private static bool IsSafeSqlServerAliasColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeSqlServerAliasColumnType(detail);

        private static bool TryGetSqlServerAliasBaseTypeText(string? detail, out string typeText)
            => ScaffoldProviderSpecificTypeClassifier.TryGetSqlServerAliasBaseTypeText(detail, out typeText);

        private static bool IsSafeSqlServerAliasBaseType(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.IsSafeSqlServerAliasBaseType(typeText);

        private static bool IsSafePostgresDomainColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsSafePostgresDomainColumnType(detail);

        private static string ScaffoldDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldDiagnosticReportBuilder.WriteMarkdown(
                ConvertForeignKeyInfos(foreignKeys),
                ConvertUnsupportedFeatureInfos(unsupportedFeatures),
                ConvertSkippedObjectInfos(skippedObjects),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys,
                _stringBuilderPool);

        private static ScaffoldCompositeForeignKeyDiagnosticInfo[] BuildCompositeForeignKeyDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyDiagnostics(
                ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                nonNullableColumnsByTable);

        private static ScaffoldPossibleJoinTableDiagnosticInfo[] BuildPossibleJoinTableDiagnostics(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableDiagnostics(
                ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

        private static IReadOnlyDictionary<string, object?> BuildPossibleJoinTableMetadata(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableMetadata(
                tableKey,
                ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys);

        private static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldJoinTableDiagnosticBuilder.BuildCompositeForeignKeyMetadata(
                ConvertForeignKeyInfos(rows),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                nonNullableColumnsByTable);

        private static string[] BuildPossibleJoinTableReasons(
            string tableKey,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys)
            => ScaffoldJoinTableDiagnosticBuilder.BuildPossibleJoinTableReasons(
                tableKey,
                ConvertForeignKeyInfos(foreignKeys),
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                ConvertIndexInfos(indexes),
                providerOwnedWriteBlockedTableKeys);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));

        private static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(ScaffoldUnsupportedFeature feature)
            => ScaffoldUnsupportedFeatureMetadataBuilder.BuildMetadata(
                new ScaffoldUnsupportedFeatureInfo(feature.TableKey, feature.Kind, feature.Name, feature.Detail)
                {
                    Metadata = feature.Metadata
                });

        private static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldUnsupportedFeatureMetadataBuilder.TryParseMetadataBoolean(value, out parsed);

        private static string ScaffoldDiagnosticsJson(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            IReadOnlySet<string>? emittedManyToManyJoinTableKeys = null)
            => ScaffoldDiagnosticReportBuilder.WriteJson(
                ConvertForeignKeyInfos(foreignKeys),
                ConvertUnsupportedFeatureInfos(unsupportedFeatures),
                ConvertSkippedObjectInfos(skippedObjects),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                emittedManyToManyJoinTableKeys);

        private static IReadOnlyList<ScaffoldManyToManyJoin> BuildManyToManyJoins(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> identityColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlySet<string>> databaseGeneratedColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            IReadOnlySet<string> providerOwnedWriteBlockedTableKeys,
            Dictionary<string, HashSet<string>> memberNamesByTable)
        {
            var foreignKeyInfos = ConvertForeignKeyInfos(foreignKeys);
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var indexInfos = ConvertIndexInfos(indexes);
            var joins = ScaffoldManyToManyJoinDiscovery.BuildManyToManyJoins(
                foreignKeyInfos,
                tableInfos,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                identityColumnsByTable,
                databaseGeneratedColumnsByTable,
                indexInfos,
                nonNullableColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                memberNamesByTable);
            return ConvertManyToManyJoins(joins);
        }

        private static IReadOnlyList<ScaffoldForeignKeyInfo> ConvertForeignKeyInfos(IReadOnlyList<ScaffoldForeignKey> foreignKeys)
        {
            var converted = new ScaffoldForeignKeyInfo[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new ScaffoldForeignKeyInfo(
                    foreignKey.DependentSchema,
                    foreignKey.DependentTable,
                    foreignKey.DependentColumn,
                    foreignKey.PrincipalSchema,
                    foreignKey.PrincipalTable,
                    foreignKey.PrincipalColumn,
                    foreignKey.ConstraintName,
                    foreignKey.ColumnCount,
                    foreignKey.OnDelete,
                    foreignKey.OnUpdate,
                    foreignKey.IsSyntheticConstraintName);
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> ConvertUnsupportedFeatureInfos(
            IReadOnlyList<ScaffoldUnsupportedFeature> features)
        {
            var converted = new ScaffoldUnsupportedFeatureInfo[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                converted[i] = new ScaffoldUnsupportedFeatureInfo(
                    feature.TableKey,
                    feature.Kind,
                    feature.Name,
                    feature.Detail)
                {
                    Metadata = BuildUnsupportedFeatureMetadata(feature)
                };
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldIndexInfo> ConvertIndexInfos(IReadOnlyList<ScaffoldIndex> indexes)
        {
            var converted = new ScaffoldIndexInfo[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new ScaffoldIndexInfo(
                    index.TableKey,
                    index.ColumnName,
                    index.IndexName,
                    index.IsUnique,
                    index.ColumnCount,
                    index.Ordinal,
                    index.IsDescending,
                    index.IsIncluded,
                    index.NullSortOrder,
                    index.NullsNotDistinct,
                    index.FilterSql,
                    index.IsSyntheticName);
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldManyToManyJoin> ConvertManyToManyJoins(IReadOnlyList<ScaffoldManyToManyJoinInfo> joins)
        {
            var converted = new ScaffoldManyToManyJoin[joins.Count];
            for (var i = 0; i < joins.Count; i++)
            {
                var join = joins[i];
                converted[i] = new ScaffoldManyToManyJoin(
                    join.JoinTableKey,
                    join.LeftTableKey,
                    join.RightTableKey,
                    join.JoinTableName,
                    join.JoinTableSchema,
                    join.LeftEntityName,
                    join.RightEntityName,
                    join.LeftForeignKeyColumns,
                    join.RightForeignKeyColumns,
                    join.LeftPrincipalKeyProperties,
                    join.RightPrincipalKeyProperties,
                    join.LeftOnDelete,
                    join.LeftOnUpdate,
                    join.RightOnDelete,
                    join.RightOnUpdate,
                    join.UsesPrimaryKeys,
                    join.LeftCollectionNavigationName,
                    join.RightCollectionNavigationName);
            }

            return converted;
        }

        private static IReadOnlyList<ScaffoldRelationship> ConvertRelationships(IReadOnlyList<ScaffoldRelationshipInfo> relationships)
        {
            var converted = new ScaffoldRelationship[relationships.Count];
            for (var i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                converted[i] = new ScaffoldRelationship(
                    relationship.DependentTableKey,
                    relationship.PrincipalTableKey,
                    relationship.DependentEntityName,
                    relationship.PrincipalEntityName,
                    relationship.ForeignKeyPropertyName,
                    relationship.PrincipalKeyPropertyName,
                    relationship.ReferenceNavigationName,
                    relationship.CollectionNavigationName,
                    relationship.IsUniqueDependentKey,
                    relationship.CascadeDelete,
                    relationship.OnDelete,
                    relationship.OnUpdate,
                    relationship.ConstraintName)
                {
                    IsRequired = relationship.IsRequired,
                    ForeignKeyPropertyNames = relationship.ForeignKeyPropertyNames,
                    PrincipalKeyPropertyNames = relationship.PrincipalKeyPropertyNames
                };
            }

            return converted;
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

        private static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
            => ScaffoldForeignKeyShape.HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, tableKey, columnName);

        private static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldForeignKeyShape.HasPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey, columnNames);

        private static bool HasOnlyScaffoldableReferentialActions(IEnumerable<ScaffoldForeignKey> foreignKeys)
            => ScaffoldForeignKeyShape.HasOnlyScaffoldableReferentialActions(ConvertForeignKeyInfos(foreignKeys.ToArray()));

        private static bool HasExactUniqueIndex(
            IReadOnlyList<ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => ScaffoldForeignKeyShape.HasExactUniqueIndex(ConvertIndexInfos(indexes), tableKey, columnNames);

        private static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKey>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.AllForeignKeyGroupsAreUniqueDependentKeys(
                dependentTableKey,
                foreignKeyGroups.Select(ConvertForeignKeyInfos).ToArray(),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        private static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => nonNullableColumnsByTable.TryGetValue(tableKey, out var nonNullableColumns)
               && columnNames.All(nonNullableColumns.Contains);

        private static bool ReferencesPrimaryKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldForeignKeyShape.ReferencesPrimaryKey(ConvertForeignKeyInfos(foreignKeyGroup.ToArray()), primaryKeyColumnsByTable);

        private static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesScaffoldablePrincipalKey(
                ConvertForeignKeyInfos(foreignKeyGroup.ToArray()),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        private static IReadOnlyList<ScaffoldPrimaryKey> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys)
            => ConvertPrimaryKeyConfigurations(ScaffoldPrimaryKeyConfigurationBuilder.BuildPrimaryKeyConfigurations(
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                skippedTableKeys));

        private static IReadOnlyList<ScaffoldPrimaryKey> ConvertPrimaryKeyConfigurations(
            IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> primaryKeys)
            => primaryKeys
                .Select(static key => new ScaffoldPrimaryKey(key.EntityName, key.PropertyNames.ToArray(), key.ConstraintName))
                .ToArray();

        private static bool ReferencesUniqueIndex(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ReferencesUniqueIndex(foreignKeyGroup.ToArray(), primaryKeyColumnsByTable, indexes);

        private static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldForeignKeyShape.ReferencesUniqueIndex(
                ConvertForeignKeyInfos(rows),
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes));

        private static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndex index)
            => ScaffoldForeignKeyShape.IsUnfilteredUniqueKeyIndex(ConvertIndexInfos(new[] { index })[0]);

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ConvertRelationships(ScaffoldRelationshipDiscovery.BuildRelationships(
                ConvertForeignKeyInfos(foreignKeys),
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                ConvertIndexInfos(indexes),
                nonNullableColumnsByTable,
                memberNamesByTable));

        private static IReadOnlyDictionary<string, string> BuildEntityNameMap(IReadOnlyList<ScaffoldTable> tables, bool useDatabaseNames)
            => ScaffoldEntityNameBuilder.BuildEntityNameMap(
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                useDatabaseNames);

        private static Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetNonNullableColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldColumnDiscovery.GetNonNullableColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetColumnPropertyNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
            => await ScaffoldColumnPropertyDiscovery.GetColumnPropertyNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                entityByTable,
                useDatabaseNames).ConfigureAwait(false);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildColumnPropertyNameMap(
            IReadOnlyDictionary<string, IReadOnlyList<string>> orderedColumns,
            IReadOnlyDictionary<string, string> entityByTable,
            bool useDatabaseNames)
            => ScaffoldColumnPropertyNameBuilder.BuildColumnPropertyNameMap(
                orderedColumns,
                entityByTable,
                useDatabaseNames);

        private static Dictionary<string, HashSet<string>> BuildMemberNameMap(
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, string> entityByTable)
            => ScaffoldColumnPropertyNameBuilder.BuildMemberNameMap(columnPropertiesByTable, entityByTable);

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
            => ScaffoldFeatureConfigurationBuilder.BuildScaffoldDefaultValueMap(
                ConvertFeatureInputs(features),
                columnPropertiesByTable);

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
            => ConvertDefaultValueConfigurations(ScaffoldFeatureConfigurationBuilder.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable));

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ConvertPrecisionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertDecimalPrecisionInfoMap(decimalPrecisionByTable)));

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ConvertColumnFacetConfigurations(ScaffoldFeatureConfigurationBuilder.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable));

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertIdentityOptionConfigurations(ScaffoldFeatureConfigurationBuilder.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertCheckConstraintConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCheckConstraintConfigurations(
                entityByTable,
                ConvertFeatureInputs(features)));

        private static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfiguration check,
            ScaffoldUnsupportedFeature feature)
            => ScaffoldFeatureConfigurationBuilder.CheckConstraintConfigurationMatchesFeature(
                ConvertCheckConstraintConfiguration(check),
                ConvertUnsupportedFeatureInputInfo(feature));

        private static string BuildGeneratedCheckConstraintName(string entityName, string sql)
            => ScaffoldFeatureConfigurationBuilder.BuildGeneratedCheckConstraintName(entityName, sql);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertCheckConstraintConfigurations(ScaffoldFeatureConfigurationBuilder.BuildEnumCheckConstraintConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        private static bool TryBuildProviderValueCheckSql(
            string columnName,
            string? detail,
            out string checkKind,
            out string sql)
            => ScaffoldFeatureConfigurationBuilder.TryBuildProviderValueCheckSql(columnName, detail, out checkKind, out sql);

        private static bool TryParseEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseEnumValues(detail, out values);

        private static bool TryParsePostgresDomainEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresDomainEnumValues(detail, out values);

        private static bool TryParseMySqlEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlEnumValues(detail, out values);

        private static bool TryParseBoundedMySqlSetValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(detail, out values);

        private static bool TryParseMySqlQuotedTypeValues(string? detail, string typeName, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseMySqlQuotedTypeValues(detail, typeName, out values);

        private static bool TryParsePostgresEnumValues(string? detail, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParsePostgresEnumValues(detail, out values);

        private static bool TryParseSqlStringLiteralList(string body, out string[] values)
            => ScaffoldProviderSpecificTypeClassifier.TryParseSqlStringLiteralList(body, out values);

        private static IReadOnlyList<ScaffoldExpressionIndexConfiguration> BuildExpressionIndexConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertExpressionIndexConfigurations(ScaffoldFeatureConfigurationBuilder.BuildExpressionIndexConfigurations(
                entityByTable,
                ConvertFeatureInputs(features)));

        private static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldFeatureConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);

        private static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertCollationConfigurations(ScaffoldFeatureConfigurationBuilder.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        private static string? ExtractCreateIndexWhereClause(string sql)
            => ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(sql);

        private static bool IsCreateIndexUnique(string? createIndexSql)
            => ScaffoldSqlMetadataParser.IsCreateIndexUnique(createIndexSql);

        private static bool TryConsumeSqlKeyword(string sql, ref int index, string keyword)
            => ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(sql, ref index, keyword);

        private static string? ExtractCreateIndexExpressionSql(string? createIndexSql)
            => ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(createIndexSql);

        private static int FindCreateIndexKeyListOpen(string sql, int startIndex)
            => ScaffoldSqlMetadataParser.FindCreateIndexKeyListOpen(sql, startIndex);

        private static int FindSqlKeywordOutsideQuotes(string sql, string keyword, int startIndex)
            => ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(sql, keyword, startIndex);

        private static IReadOnlyList<ScaffoldComputedColumnConfiguration> BuildComputedColumnConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertComputedColumnConfigurations(ScaffoldFeatureConfigurationBuilder.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                ConvertFeatureInputs(features)));

        private static (string Sql, bool Stored) NormalizeScaffoldComputedSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(raw);

        private static bool TryTrimTrailingComputedStorageToken(string sql, string token, out string trimmedSql)
            => ScaffoldSqlMetadataParser.TryTrimTrailingComputedStorageToken(sql, token, out trimmedSql);

        private static string NormalizeScaffoldCheckSql(string raw)
            => ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(raw);

        private static bool TryNormalizeScaffoldDefaultSql(string? raw, out string defaultValueSql)
            => ScaffoldSqlMetadataParser.TryNormalizeScaffoldDefaultSql(raw, out defaultValueSql);

        private static bool HasBalancedOuterParentheses(string value)
            => ScaffoldSqlMetadataParser.HasBalancedOuterParentheses(value);

        private static IReadOnlyDictionary<string, IReadOnlySet<string>> BuildFeatureNameMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureNameMap(ConvertFeatureInputs(features), kinds);

        private static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderNativeTemporalTableKeys(ConvertFeatureInputs(features));

        private static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationBuilder.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        private static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureTableKeys(ConvertFeatureInputs(features), kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationBuilder.BuildFeatureDetailMap(ConvertFeatureInputs(features), kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ConvertDecimalPrecisionMap(ScaffoldFeatureConfigurationBuilder.BuildDecimalPrecisionMap(ConvertFeatureInputs(features)));

        private static bool TryParseDecimalPrecision(string? typeName, out int precision, out int? scale)
            => ScaffoldSqlMetadataParser.TryParseDecimalPrecision(typeName, out precision, out scale);

        private static bool TryParseIdentityOptions(string? detail, out long seed, out long increment)
            => ScaffoldSqlMetadataParser.TryParseIdentityOptions(detail, out seed, out increment);

        private static HashSet<string> GetOrCreateMemberNames(
            Dictionary<string, HashSet<string>> memberNamesByTable,
            string tableKey)
            => ScaffoldColumnPropertyNameBuilder.GetOrCreateMemberNames(memberNamesByTable, tableKey);

        private static HashSet<string> CreateReservedMemberNameSet()
            => ScaffoldColumnPropertyNameBuilder.CreateReservedMemberNameSet();

        private static Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetPrimaryKeyColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldKeyDiscovery.GetPrimaryKeyColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static Task<IReadOnlyDictionary<string, string>> GetPrimaryKeyConstraintNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

        private static Task<IReadOnlyDictionary<string, IReadOnlySet<string>>> GetIdentityColumnNamesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldColumnDiscovery.GetIdentityColumnNamesAsync(
                connection,
                provider,
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray());

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

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities)
            => ScaffoldContext(namespaceName, contextName, entities, pluralizeQueryProperties: true);

        private static string ScaffoldContext(string namespaceName, string contextName, IEnumerable<string> entities, bool pluralizeQueryProperties)
            => ScaffoldContextWithRelationships(namespaceName, contextName, entities, Array.Empty<ScaffoldRelationship>(), Array.Empty<ScaffoldManyToManyJoin>(), pluralizeQueryProperties: pluralizeQueryProperties);

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
            IReadOnlyList<ScaffoldIdentityOptionConfiguration>? identityOptionConfigurations = null,
            IReadOnlyList<ScaffoldPrecisionConfiguration>? precisionConfigurations = null,
            IReadOnlyList<ScaffoldColumnFacetConfiguration>? columnFacetConfigurations = null,
            bool pluralizeQueryProperties = true,
            bool useNullableReferenceTypes = true,
            string? entityNamespaceName = null,
            bool useDatabaseNames = false)
        {
            compositePrimaryKeys ??= Array.Empty<ScaffoldPrimaryKey>();
            defaultValueConfigurations ??= Array.Empty<ScaffoldDefaultValueConfiguration>();
            checkConstraintConfigurations ??= Array.Empty<ScaffoldCheckConstraintConfiguration>();
            computedColumnConfigurations ??= Array.Empty<ScaffoldComputedColumnConfiguration>();
            expressionIndexConfigurations ??= Array.Empty<ScaffoldExpressionIndexConfiguration>();
            collationConfigurations ??= Array.Empty<ScaffoldCollationConfiguration>();
            sequenceStubs ??= Array.Empty<ScaffoldSkippedObject>();
            identityOptionConfigurations ??= Array.Empty<ScaffoldIdentityOptionConfiguration>();
            precisionConfigurations ??= Array.Empty<ScaffoldPrecisionConfiguration>();
            columnFacetConfigurations ??= Array.Empty<ScaffoldColumnFacetConfiguration>();
            routineStubs ??= Array.Empty<ScaffoldSkippedObject>();
            return ScaffoldContextWriter.Write(new ScaffoldContextInfo(
                namespaceName,
                contextName,
                entities.ToArray(),
                ConvertContextRelationshipInfos(relationships),
                ConvertManyToManyJoinInfos(manyToManyJoins),
                ConvertRoutineStubInfos(routineStubs),
                ConvertContextPrimaryKeyInfos(compositePrimaryKeys),
                ConvertContextDefaultValueInfos(defaultValueConfigurations),
                ConvertContextCheckConstraintInfos(checkConstraintConfigurations),
                ConvertContextComputedColumnInfos(computedColumnConfigurations),
                ConvertContextExpressionIndexInfos(expressionIndexConfigurations),
                ConvertContextCollationInfos(collationConfigurations),
                ConvertContextSequenceInfos(sequenceStubs),
                ConvertContextIdentityOptionInfos(identityOptionConfigurations),
                ConvertContextPrecisionInfos(precisionConfigurations),
                ConvertContextColumnFacetInfos(columnFacetConfigurations),
                pluralizeQueryProperties,
                useNullableReferenceTypes,
                entityNamespaceName,
                useDatabaseNames));
        }

        private static ScaffoldContextRelationshipInfo[] ConvertContextRelationshipInfos(IReadOnlyList<ScaffoldRelationship> relationships)
        {
            var converted = new ScaffoldContextRelationshipInfo[relationships.Count];
            for (var i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                converted[i] = new ScaffoldContextRelationshipInfo(
                    relationship.DependentEntityName,
                    relationship.PrincipalEntityName,
                    relationship.ReferenceNavigationName,
                    relationship.CollectionNavigationName,
                    relationship.IsUniqueDependentKey,
                    relationship.CascadeDelete,
                    relationship.OnDelete,
                    relationship.OnUpdate,
                    relationship.ConstraintName,
                    relationship.ForeignKeyPropertyNames,
                    relationship.PrincipalKeyPropertyNames);
            }

            return converted;
        }

        private static ScaffoldManyToManyJoinInfo[] ConvertManyToManyJoinInfos(IReadOnlyList<ScaffoldManyToManyJoin> joins)
        {
            var converted = new ScaffoldManyToManyJoinInfo[joins.Count];
            for (var i = 0; i < joins.Count; i++)
            {
                var join = joins[i];
                converted[i] = new ScaffoldManyToManyJoinInfo(
                    join.JoinTableKey,
                    join.LeftTableKey,
                    join.RightTableKey,
                    join.JoinTableName,
                    join.JoinTableSchema,
                    join.LeftEntityName,
                    join.RightEntityName,
                    join.LeftForeignKeyColumns,
                    join.RightForeignKeyColumns,
                    join.LeftPrincipalKeyProperties,
                    join.RightPrincipalKeyProperties,
                    join.LeftOnDelete,
                    join.LeftOnUpdate,
                    join.RightOnDelete,
                    join.RightOnUpdate,
                    join.UsesPrimaryKeys,
                    join.LeftCollectionNavigationName,
                    join.RightCollectionNavigationName);
            }

            return converted;
        }

        private static ScaffoldRoutineStubInfo[] ConvertRoutineStubInfos(IReadOnlyList<ScaffoldSkippedObject> routineStubs)
        {
            var converted = new ScaffoldRoutineStubInfo[routineStubs.Count];
            for (var i = 0; i < routineStubs.Count; i++)
            {
                var routine = routineStubs[i];
                converted[i] = new ScaffoldRoutineStubInfo(
                    routine.Schema,
                    routine.Name,
                    routine.Kind,
                    routine.Detail,
                    routine.Comment,
                    BuildSkippedObjectMetadata(routine));
            }

            return converted;
        }

        private static ScaffoldContextPrimaryKeyInfo[] ConvertContextPrimaryKeyInfos(IReadOnlyList<ScaffoldPrimaryKey> primaryKeys)
            => primaryKeys
                .Select(key => new ScaffoldContextPrimaryKeyInfo(key.EntityName, key.PropertyNames, key.ConstraintName))
                .ToArray();

        private static ScaffoldContextDefaultValueInfo[] ConvertContextDefaultValueInfos(IReadOnlyList<ScaffoldDefaultValueConfiguration> defaultValues)
            => defaultValues
                .Select(defaultValue => new ScaffoldContextDefaultValueInfo(defaultValue.EntityName, defaultValue.PropertyName, defaultValue.DefaultValueSql))
                .ToArray();

        private static ScaffoldContextCheckConstraintInfo[] ConvertContextCheckConstraintInfos(IReadOnlyList<ScaffoldCheckConstraintConfiguration> checks)
            => checks
                .Select(check => new ScaffoldContextCheckConstraintInfo(check.EntityName, check.Name, check.Sql))
                .ToArray();

        private static ScaffoldContextComputedColumnInfo[] ConvertContextComputedColumnInfos(IReadOnlyList<ScaffoldComputedColumnConfiguration> computedColumns)
            => computedColumns
                .Select(computed => new ScaffoldContextComputedColumnInfo(computed.EntityName, computed.PropertyName, computed.Sql, computed.Stored))
                .ToArray();

        private static ScaffoldContextExpressionIndexInfo[] ConvertContextExpressionIndexInfos(IReadOnlyList<ScaffoldExpressionIndexConfiguration> expressionIndexes)
            => expressionIndexes
                .Select(index => new ScaffoldContextExpressionIndexInfo(index.EntityName, index.Name, index.ExpressionSql, index.IsUnique, index.FilterSql))
                .ToArray();

        private static ScaffoldContextCollationInfo[] ConvertContextCollationInfos(IReadOnlyList<ScaffoldCollationConfiguration> collations)
            => collations
                .Select(collation => new ScaffoldContextCollationInfo(collation.EntityName, collation.PropertyName, collation.Collation))
                .ToArray();

        private static ScaffoldContextSequenceInfo[] ConvertContextSequenceInfos(IReadOnlyList<ScaffoldSkippedObject> sequenceStubs)
            => sequenceStubs
                .Select(sequence => new ScaffoldContextSequenceInfo(sequence.Schema, sequence.Name, sequence.Detail, sequence.Comment))
                .ToArray();

        private static ScaffoldContextIdentityOptionInfo[] ConvertContextIdentityOptionInfos(IReadOnlyList<ScaffoldIdentityOptionConfiguration> identityOptions)
            => identityOptions
                .Select(identity => new ScaffoldContextIdentityOptionInfo(identity.EntityName, identity.PropertyName, identity.Seed, identity.Increment))
                .ToArray();

        private static ScaffoldContextPrecisionInfo[] ConvertContextPrecisionInfos(IReadOnlyList<ScaffoldPrecisionConfiguration> precisionConfigurations)
            => precisionConfigurations
                .Select(precision => new ScaffoldContextPrecisionInfo(precision.EntityName, precision.PropertyName, precision.Precision, precision.Scale))
                .ToArray();

        private static ScaffoldContextColumnFacetInfo[] ConvertContextColumnFacetInfos(IReadOnlyList<ScaffoldColumnFacetConfiguration> columnFacetConfigurations)
            => columnFacetConfigurations
                .Select(facet => new ScaffoldContextColumnFacetInfo(facet.EntityName, facet.PropertyName, facet.MaxLength, facet.IsUnicode, facet.IsFixedLength))
                .ToArray();

        private static (byte? Precision, byte? Scale) GetRoutineParameterPrecisionScale(string? dataType)
            => ScaffoldRoutineTypeMapper.GetRoutineParameterPrecisionScale(dataType);

        private static void AppendNullableDirective(StringBuilder sb, bool useNullableReferenceTypes)
            => sb.AppendLine(useNullableReferenceTypes ? "#nullable enable" : "#nullable disable");

        /// <summary>
        /// Returns a C# type name with correct nullability for value or reference types.
        /// </summary>
        private static string GetTypeName(Type type, bool allowNull, bool useNullableReferenceTypes = true)
            => ScaffoldTypeNameHelper.GetTypeName(type, allowNull, useNullableReferenceTypes);

        private static int? GetScaffoldMaxLength(Type clrType, DataRow row)
            => ScaffoldEntitySourceBuilder.GetScaffoldMaxLength(clrType, row);

        private static bool IsUnboundedScaffoldMaxLength(int size)
            => ScaffoldEntitySourceBuilder.IsUnboundedScaffoldMaxLength(size);

        private static Type NormalizeScaffoldClrType(DatabaseProvider provider, Type clrType, bool allowNull, bool isKey, bool isAuto, string? declaredType = null, string? providerSpecificColumnType = null)
            => ScaffoldEntitySourceBuilder.NormalizeScaffoldClrType(
                provider,
                clrType,
                allowNull,
                isKey,
                isAuto,
                declaredType,
                providerSpecificColumnType);

        private static bool TryMapSqlServerAliasBaseClrType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapSqlServerAliasBaseClrType(detail, out type);

        private static string NormalizeSqlServerAliasBaseTypeName(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeSqlServerAliasBaseTypeName(typeText);

        private static int? GetSqlServerAliasBaseMaxLength(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLength(detail);

        private static int? GetSqlServerAliasBaseMaxLengthFromTypeText(string typeText)
            => ScaffoldProviderSpecificTypeClassifier.GetSqlServerAliasBaseMaxLengthFromTypeText(typeText);

        private static bool TryMapMySqlUnsignedType(string? detail, out Type type)
            => ScaffoldProviderSpecificTypeClassifier.TryMapMySqlUnsignedType(detail, out type);

        private static string NormalizeMySqlUnsignedTypeDetail(string detail)
            => ScaffoldProviderSpecificTypeClassifier.NormalizeMySqlUnsignedTypeDetail(detail);

        private static bool TryMapPostgresArrayType(string? detail, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayType(detail, out arrayType);

        private static bool TryMapPostgresArrayCastType(string normalized, out Type arrayType)
            => ScaffoldProviderSpecificTypeClassifier.TryMapPostgresArrayCastType(normalized, out arrayType);

        private static bool IsSqliteUuidDeclaredType(string? declaredType)
            => ScaffoldEntitySourceBuilder.IsSqliteUuidDeclaredType(declaredType);

        /// <summary>
        /// Converts a database object name to PascalCase by removing separators and capitalizing
        /// the first letter of each segment.
        /// </summary>
        private static string ToPascalCase(string name)
            => ScaffoldNameHelper.ToPascalCase(name);

        private static string ToScaffoldClrName(string databaseName, bool useDatabaseNames)
            => ScaffoldNameHelper.ToScaffoldClrName(databaseName, useDatabaseNames);

        private static string ToScaffoldClrNamePart(string databaseName, bool useDatabaseNames)
            => ScaffoldNameHelper.ToScaffoldClrNamePart(databaseName, useDatabaseNames);

        private static string ToNavigationName(string generatedName)
            => ScaffoldNameHelper.ToNavigationName(generatedName);

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
            => ScaffoldNameHelper.EscapeCSharpIdentifier(identifier);

        private static bool IsValidNamespaceName(string namespaceName)
            => ScaffoldNameHelper.IsValidNamespaceName(namespaceName);

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
            => ScaffoldNameHelper.MakeUnique(baseName, existingNames);

        private static string MakeUniqueContextName(string contextName, IEnumerable<string> entityNames)
            => ScaffoldNameHelper.MakeUniqueContextName(contextName, entityNames);

        private static string Pluralize(string name)
            => ScaffoldNameHelper.Pluralize(name);

        private sealed record ScaffoldFeatureConfigurations(
            IReadOnlyList<ScaffoldUnsupportedFeature> GeneratedModelFeatureDiagnostics,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ProviderSpecificColumnTypesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> DefaultValuesByTable,
            IReadOnlySet<string> ProviderSpecificDefaultTableKeys,
            IReadOnlyList<ScaffoldCheckConstraintConfiguration> CheckConstraints,
            IReadOnlyList<ScaffoldExpressionIndexConfiguration> ExpressionIndexConfigurations,
            IReadOnlyList<ScaffoldCollationConfiguration> CollationConfigurations,
            IReadOnlyList<ScaffoldComputedColumnConfiguration> ComputedColumnConfigurations,
            IReadOnlyDictionary<string, IReadOnlySet<string>> ComputedColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> DecimalPrecisionByTable,
            IReadOnlyList<ScaffoldPrecisionConfiguration> PrecisionConfigurations,
            IReadOnlyList<ScaffoldColumnFacetConfiguration> ColumnFacetConfigurations,
            IReadOnlyDictionary<string, IReadOnlySet<string>> RowVersionColumnsByTable,
            IReadOnlySet<string> ProviderNativeTemporalTableKeys,
            IReadOnlySet<string> ProviderOwnedTriggerTableKeys,
            IReadOnlyList<ScaffoldIdentityOptionConfiguration> IdentityOptionConfigurations,
            IReadOnlySet<string> ProviderSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> ProviderOwnedWriteBlockedTableKeys);

        private sealed record ScaffoldEntityFileSet(
            IReadOnlyList<(string Path, string Content)> GeneratedFiles,
            IReadOnlyList<string> EntityNames);

        private readonly record struct ScaffoldTable(string Name, string? Schema);

        private readonly record struct ScaffoldSkippedObject(
            string? Schema,
            string Name,
            string Kind,
            string Detail,
            string? Comment);

        private readonly record struct ScaffoldPrimaryKey(
            string EntityName,
            string[] PropertyNames,
            string? ConstraintName);

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

        private readonly record struct ScaffoldPrecisionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int Precision,
            int? Scale);

        private readonly record struct ScaffoldColumnFacetConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int? MaxLength,
            bool? IsUnicode,
            bool IsFixedLength);

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
            string OnUpdate = "NO ACTION",
            bool IsSyntheticConstraintName = false);

        private readonly record struct ScaffoldIndex(
            string TableKey,
            string ColumnName,
            string IndexName,
            bool IsUnique,
            int ColumnCount,
            int Ordinal,
            bool IsDescending,
            bool IsIncluded,
            IndexNullSortOrder NullSortOrder,
            bool NullsNotDistinct,
            string? FilterSql,
            bool IsSyntheticName = false);

        private readonly record struct ScaffoldRelationship(
            string DependentTableKey,
            string PrincipalTableKey,
            string DependentEntityName,
            string PrincipalEntityName,
            string ForeignKeyPropertyName,
            string PrincipalKeyPropertyName,
            string ReferenceNavigationName,
            string CollectionNavigationName,
            bool IsUniqueDependentKey,
            bool CascadeDelete,
            string OnDelete,
            string OnUpdate,
            string? ConstraintName)
        {
            public IReadOnlyList<string> ForeignKeyPropertyNames { get; init; } = new[] { ForeignKeyPropertyName };

            public IReadOnlyList<string> PrincipalKeyPropertyNames { get; init; } = new[] { PrincipalKeyPropertyName };

            public bool IsRequired { get; init; }

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
            string LeftOnDelete,
            string LeftOnUpdate,
            string RightOnDelete,
            string RightOnUpdate,
            bool UsesPrimaryKeys,
            string LeftCollectionNavigationName,
            string RightCollectionNavigationName)
        {
            public string LeftForeignKeyColumn => LeftForeignKeyColumns[0];

            public string RightForeignKeyColumn => RightForeignKeyColumns[0];

            public bool IsComposite => LeftForeignKeyColumns.Length > 1 || RightForeignKeyColumns.Length > 1;
        }

        private readonly record struct ScaffoldManyToManyNavigation(
            string TargetEntityName,
            string CollectionNavigationName);

        private readonly record struct ScaffoldDecimalPrecision(
            int Precision,
            int? Scale);

        private readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail)
        {
            public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
        }
    }
}
