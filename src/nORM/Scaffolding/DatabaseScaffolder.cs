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
                var discovery = await ScaffoldModelDiscovery.BuildAsync(connection, provider, options).ConfigureAwait(false);
                var tables = discovery.Tables;
                var skippedObjects = discovery.SkippedObjects;
                var queryArtifactTableKeys = discovery.QueryArtifactTableKeys;
                var entityByTable = discovery.EntityByTable;
                var columnPropertiesByTable = discovery.ColumnPropertiesByTable;
                var primaryKeyColumnsByTable = discovery.PrimaryKeyColumnsByTable;
                var nonNullableColumnsByTable = discovery.NonNullableColumnsByTable;
                var sqliteDeclaredTypesByTable = discovery.SqliteDeclaredTypesByTable;
                var stringBinaryFacetsByTable = discovery.StringBinaryFacetsByTable;
                var commentsByTable = discovery.CommentsByTable;
                var identityColumnsByTable = discovery.IdentityColumnsByTable;
                var indexes = discovery.Indexes;
                var foreignKeys = discovery.ForeignKeys;
                var unsupportedFeatures = discovery.UnsupportedFeatures;
                var featureConfigurations = discovery.FeatureConfigurations;
                safeContextName = MakeUniqueContextName(safeContextName, entityByTable.Values);
                var computedColumnsByTable = featureConfigurations.ComputedColumnsByTable;
                var composition = ScaffoldModelCompositionBuilder.Build(discovery);
                var manyToManyJoins = composition.ManyToManyJoins;
                var manyToManyJoinTableKeys = composition.ManyToManyJoinTableKeys;
                var relationships = composition.Relationships;
                var compositePrimaryKeys = composition.CompositePrimaryKeys;
                var defaultValueConfigurations = composition.DefaultValueConfigurations;
                var checkConstraints = composition.CheckConstraints;
                var computedColumnConfigurations = composition.ComputedColumnConfigurations;
                var expressionIndexConfigurations = composition.ExpressionIndexConfigurations;
                var collationConfigurations = composition.CollationConfigurations;
                var identityOptionConfigurations = composition.IdentityOptionConfigurations;
                var precisionConfigurations = composition.PrecisionConfigurations;
                var columnFacetConfigurations = composition.ColumnFacetConfigurations;
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
            => await ScaffoldEntityFileAdapter.BuildScaffoldEntityFilesAsync(
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
            => ScaffoldEntityFileAdapter.ScaffoldEntityAsync(
                connection,
                provider,
                schemaName,
                tableName,
                entityName,
                namespaceName,
                columnPropertyNames,
                indexes,
                references,
                collections,
                manyToManyCollections,
                computedColumns,
                rowVersionColumns,
                identityColumns,
                decimalPrecisions,
                columnFacets,
                comments,
                isReadOnlyEntity,
                useNullableReferenceTypes,
                nonNullableColumns,
                sqliteDeclaredTypes,
                providerSpecificColumnTypes);

        private static ScaffoldEntityIndexSourceInfo[] ConvertEntityIndexInfos(IReadOnlyList<ScaffoldIndex>? indexes)
            => ScaffoldEntityFileAdapter.ConvertEntityIndexInfos(indexes);

        private static ScaffoldEntityReferenceInfo[] ConvertEntityReferenceInfos(IReadOnlyList<ScaffoldRelationship>? references)
            => ScaffoldEntityFileAdapter.ConvertEntityReferenceInfos(references);

        private static ScaffoldEntityCollectionInfo[] ConvertEntityCollectionInfos(IReadOnlyList<ScaffoldRelationship>? collections)
            => ScaffoldEntityFileAdapter.ConvertEntityCollectionInfos(collections);

        private static ScaffoldEntityManyToManyNavigationInfo[] ConvertEntityManyToManyNavigationInfos(IReadOnlyList<ScaffoldManyToManyNavigation>? manyToManyCollections)
            => ScaffoldEntityFileAdapter.ConvertEntityManyToManyNavigationInfos(manyToManyCollections);

        private static IReadOnlyDictionary<string, ScaffoldDecimalPrecisionInfo>? ConvertEntityDecimalPrecisionInfos(
            IReadOnlyDictionary<string, ScaffoldDecimalPrecision>? decimalPrecisions)
            => ScaffoldEntityFileAdapter.ConvertEntityDecimalPrecisionInfos(decimalPrecisions);

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
            => await ScaffoldSchemaDiscoveryAdapter.GetTablesAsync(connection, provider).ConfigureAwait(false);

        private static async Task<IReadOnlyList<ScaffoldSkippedObject>> GetSkippedObjectsAsync(DbConnection connection, DatabaseProvider provider)
            => await ScaffoldSchemaDiscoveryAdapter.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);

        private static IReadOnlyList<ScaffoldSkippedObjectInfo> ConvertSkippedObjectInfos(
            IReadOnlyList<ScaffoldSkippedObject> objects)
            => ScaffoldSchemaDiscoveryAdapter.ConvertSkippedObjectInfos(objects);

        private static Task<IReadOnlyList<string>> GetSqliteSchemasAsync(DbConnection connection)
            => ScaffoldSkippedObjectDiscovery.GetSqliteSchemasAsync(connection);

        private static string SqliteSchemaResult(string schema)
            => ScaffoldSkippedObjectDiscovery.SqliteSchemaResult(schema);
        private static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
            => ScaffoldModelDiscovery.SqlitePragma(provider, schema, pragmaName, argument);

        private static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetSqliteDeclaredColumnTypesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => await ScaffoldModelDiscovery.GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false);

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
            => ScaffoldSchemaDiscoveryAdapter.ToScaffoldTableInfos(tables);

        private static ScaffoldTable ToScaffoldTable(ScaffoldTableInfo table)
            => ScaffoldSchemaDiscoveryAdapter.ToScaffoldTable(table);

        private static ScaffoldSkippedObjectInfo[] ToSkippedObjectInfos(IEnumerable<ScaffoldSkippedObject> objects)
            => ScaffoldSchemaDiscoveryAdapter.ToSkippedObjectInfos(objects);

        private static ScaffoldSkippedObjectInfo ToSkippedObjectInfo(ScaffoldSkippedObject obj)
            => ScaffoldSchemaDiscoveryAdapter.ToSkippedObjectInfo(obj);

        private static ScaffoldSkippedObject ToScaffoldSkippedObject(ScaffoldSkippedObjectInfo obj)
            => ScaffoldSchemaDiscoveryAdapter.ToScaffoldSkippedObject(obj);

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
            => await ScaffoldSchemaDiscoveryAdapter.GetIndexesAsync(connection, provider, tables).ConfigureAwait(false);

        private static IReadOnlyList<ScaffoldIndex> ConvertIndexes(IReadOnlyList<ScaffoldIndexInfo> indexes)
            => ScaffoldSchemaDiscoveryAdapter.ConvertIndexes(indexes);

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
            => await ScaffoldSchemaDiscoveryAdapter.GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false);

        private static IReadOnlyList<ScaffoldForeignKey> ConvertForeignKeys(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
            => ScaffoldSchemaDiscoveryAdapter.ConvertForeignKeys(foreignKeys);

        private static IReadOnlyList<ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

        private static string NormalizeReferentialAction(string? action)
            => ScaffoldUnsupportedDiagnosticAdapter.NormalizeReferentialAction(action);

        private static bool TryParseReferentialAction(string? action, out ReferentialAction referentialAction)
            => ScaffoldUnsupportedDiagnosticAdapter.TryParseReferentialAction(action, out referentialAction);

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => await ScaffoldSchemaDiscoveryAdapter.GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false);

        private static IReadOnlyList<ScaffoldUnsupportedFeature> ConvertUnsupportedFeatures(
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> features)
            => ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(features);



        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetPostgresEnumColumnFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
            => await ScaffoldSchemaDiscoveryAdapter.GetPostgresEnumColumnFeaturesAsync(connection, provider, tables).ConfigureAwait(false);

        private static void AddMissingPrimaryKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
            => ScaffoldUnsupportedDiagnosticAdapter.AddMissingPrimaryKeyDiagnostics(
                features,
                tables,
                primaryKeyColumnsByTable,
                columnPropertiesByTable);

        private static void AddReferentialActionDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldForeignKey> foreignKeys)
            => ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(features, foreignKeys);

        private static void RemoveSupportedDescendingIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedDescendingIndexDiagnostics(features, indexes);

        private static void RemoveSupportedIncludedColumnIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedIncludedColumnIndexDiagnostics(features, indexes);

        private static void RemoveSupportedPartialIndexDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedPartialIndexDiagnostics(features, indexes);

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
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureConfigurations(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);

        private static void RestoreGeneratedManyToManyUnsupportedFeatures(
            List<ScaffoldUnsupportedFeature> unsupportedFeatures,
            IEnumerable<ScaffoldUnsupportedFeature> generatedModelFeatureDiagnostics,
            IReadOnlySet<string> manyToManyJoinTableKeys)
            => ScaffoldModelCompositionBuilder.RestoreGeneratedManyToManyUnsupportedFeatures(
                unsupportedFeatures,
                generatedModelFeatureDiagnostics,
                manyToManyJoinTableKeys);

        private static bool ShouldRestoreGeneratedManyToManyUnsupportedFeature(string kind)
            => ScaffoldModelCompositionBuilder.ShouldRestoreGeneratedManyToManyUnsupportedFeature(kind);

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
            => ScaffoldEntityFileAdapter.ShouldMarkScaffoldedEntityReadOnly(
                tableKey,
                queryArtifactTableKeys,
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypes,
                primaryKeyColumnsByTable);

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
            => ScaffoldDiagnosticsAdapter.ScaffoldDiagnostics(
                foreignKeys,
                unsupportedFeatures,
                skippedObjects,
                primaryKeyColumnsByTable,
                indexes,
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
            => ScaffoldDiagnosticsAdapter.BuildCompositeForeignKeyDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                indexes,
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
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableDiagnostics(
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
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
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableMetadata(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        private static IReadOnlyDictionary<string, object?> BuildCompositeForeignKeyMetadata(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable)
            => ScaffoldDiagnosticsAdapter.BuildCompositeForeignKeyMetadata(
                rows,
                primaryKeyColumnsByTable,
                indexes,
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
            => ScaffoldDiagnosticsAdapter.BuildPossibleJoinTableReasons(
                tableKey,
                foreignKeys,
                primaryKeyColumnsByTable,
                columnPropertiesByTable,
                nonNullableColumnsByTable,
                databaseGeneratedColumnsByTable,
                identityColumnsByTable,
                indexes,
                providerOwnedWriteBlockedTableKeys);

        private static bool ReaderHasColumn(DbDataReader reader, string name)
            => ScaffoldDataReaderHelper.HasColumn(reader, name);

        private static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(ScaffoldSkippedObject obj)
            => ScaffoldSchemaDiscoveryAdapter.BuildSkippedObjectMetadata(obj);

        private static IReadOnlyDictionary<string, object?> BuildUnsupportedFeatureMetadata(ScaffoldUnsupportedFeature feature)
            => ScaffoldDiagnosticsAdapter.BuildUnsupportedFeatureMetadata(feature);

        private static bool TryParseMetadataBoolean(string value, out bool parsed)
            => ScaffoldDiagnosticsAdapter.TryParseMetadataBoolean(value, out parsed);

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
            => ScaffoldDiagnosticsAdapter.ScaffoldDiagnosticsJson(
                foreignKeys,
                unsupportedFeatures,
                skippedObjects,
                primaryKeyColumnsByTable,
                indexes,
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
            => ScaffoldRelationshipAdapter.BuildManyToManyJoins(
                foreignKeys,
                tables,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                identityColumnsByTable,
                databaseGeneratedColumnsByTable,
                indexes,
                nonNullableColumnsByTable,
                providerOwnedWriteBlockedTableKeys,
                memberNamesByTable);

        private static IReadOnlyList<ScaffoldForeignKeyInfo> ConvertForeignKeyInfos(IReadOnlyList<ScaffoldForeignKey> foreignKeys)
            => ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys);

        private static IReadOnlyList<ScaffoldUnsupportedFeatureInfo> ConvertUnsupportedFeatureInfos(
            IReadOnlyList<ScaffoldUnsupportedFeature> features)
            => ScaffoldDiagnosticsAdapter.ConvertUnsupportedFeatureInfos(features);

        private static IReadOnlyList<ScaffoldIndexInfo> ConvertIndexInfos(IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes);

        private static IReadOnlyList<ScaffoldManyToManyJoin> ConvertManyToManyJoins(IReadOnlyList<ScaffoldManyToManyJoinInfo> joins)
            => ScaffoldRelationshipAdapter.ConvertManyToManyJoins(joins);

        private static IReadOnlyList<ScaffoldRelationship> ConvertRelationships(IReadOnlyList<ScaffoldRelationshipInfo> relationships)
            => ScaffoldRelationshipAdapter.ConvertRelationships(relationships);

        private static IReadOnlyList<ScaffoldManyToManyNavigation> BuildManyToManyNavigations(
            IReadOnlyList<ScaffoldManyToManyJoin> joins,
            string tableKey)
            => ScaffoldRelationshipAdapter.BuildManyToManyNavigations(joins, tableKey);

        private static bool HasSinglePrimaryKeyColumn(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            string columnName)
            => ScaffoldRelationshipAdapter.HasSinglePrimaryKeyColumn(primaryKeyColumnsByTable, tableKey, columnName);

        private static bool HasPrimaryKeyColumns(
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldRelationshipAdapter.HasPrimaryKeyColumns(primaryKeyColumnsByTable, tableKey, columnNames);

        private static bool HasOnlyScaffoldableReferentialActions(IEnumerable<ScaffoldForeignKey> foreignKeys)
            => ScaffoldRelationshipAdapter.HasOnlyScaffoldableReferentialActions(foreignKeys);

        private static bool HasExactUniqueIndex(
            IReadOnlyList<ScaffoldIndex> indexes,
            string tableKey,
            IReadOnlySet<string> columnNames)
            => ScaffoldRelationshipAdapter.HasExactUniqueIndex(indexes, tableKey, columnNames);

        private static bool AllForeignKeyGroupsAreUniqueDependentKeys(
            string dependentTableKey,
            IEnumerable<IReadOnlyList<ScaffoldForeignKey>> foreignKeyGroups,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.AllForeignKeyGroupsAreUniqueDependentKeys(
                dependentTableKey,
                foreignKeyGroups,
                primaryKeyColumnsByTable,
                indexes);

        private static bool HasNonNullableColumns(
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            string tableKey,
            IReadOnlyList<string> columnNames)
            => ScaffoldRelationshipAdapter.HasNonNullableColumns(nonNullableColumnsByTable, tableKey, columnNames);

        private static bool ReferencesPrimaryKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => ScaffoldRelationshipAdapter.ReferencesPrimaryKey(foreignKeyGroup, primaryKeyColumnsByTable);

        private static bool ReferencesScaffoldablePrincipalKey(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesScaffoldablePrincipalKey(
                foreignKeyGroup,
                primaryKeyColumnsByTable,
                indexes);

        private static IReadOnlyList<ScaffoldPrimaryKey> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys)
            => ScaffoldRelationshipAdapter.BuildPrimaryKeyConfigurations(
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                skippedTableKeys);

        private static IReadOnlyList<ScaffoldPrimaryKey> ConvertPrimaryKeyConfigurations(
            IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> primaryKeys)
            => ScaffoldRelationshipAdapter.ConvertPrimaryKeyConfigurations(primaryKeys);

        private static bool ReferencesUniqueIndex(
            IGrouping<string, ScaffoldForeignKey> foreignKeyGroup,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesUniqueIndex(foreignKeyGroup, primaryKeyColumnsByTable, indexes);

        private static bool ReferencesUniqueIndex(
            IReadOnlyList<ScaffoldForeignKey> rows,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes)
            => ScaffoldRelationshipAdapter.ReferencesUniqueIndex(rows, primaryKeyColumnsByTable, indexes);

        private static bool IsUnfilteredUniqueKeyIndex(ScaffoldIndex index)
            => ScaffoldRelationshipAdapter.IsUnfilteredUniqueKeyIndex(index);

        private static IReadOnlyList<ScaffoldRelationship> BuildRelationships(
            IReadOnlyList<ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<ScaffoldIndex> indexes,
            IReadOnlyDictionary<string, IReadOnlySet<string>> nonNullableColumnsByTable,
            Dictionary<string, HashSet<string>> memberNamesByTable)
            => ScaffoldRelationshipAdapter.BuildRelationships(
                foreignKeys,
                entityByTable,
                columnPropertiesByTable,
                primaryKeyColumnsByTable,
                indexes,
                nonNullableColumnsByTable,
                memberNamesByTable);

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
            => ScaffoldFeatureConfigurationAdapter.BuildScaffoldDefaultValueMap(features, columnPropertiesByTable);

        private static IReadOnlyList<ScaffoldDefaultValueConfiguration> BuildDefaultValueConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> defaultValuesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildDefaultValueConfigurations(
                entityByTable,
                columnPropertiesByTable,
                defaultValuesByTable);

        private static IReadOnlyList<ScaffoldPrecisionConfiguration> BuildPrecisionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> decimalPrecisionByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildPrecisionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                decimalPrecisionByTable);

        private static IReadOnlyList<ScaffoldColumnFacetConfiguration> BuildColumnFacetConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> columnFacetsByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildColumnFacetConfigurations(
                entityByTable,
                columnPropertiesByTable,
                columnFacetsByTable);

        private static IReadOnlyList<ScaffoldIdentityOptionConfiguration> BuildIdentityOptionConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildIdentityOptionConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildCheckConstraintConfigurations(entityByTable, features);

        private static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfiguration check,
            ScaffoldUnsupportedFeature feature)
            => ScaffoldFeatureConfigurationAdapter.CheckConstraintConfigurationMatchesFeature(check, feature);

        private static string BuildGeneratedCheckConstraintName(string entityName, string sql)
            => ScaffoldFeatureConfigurationBuilder.BuildGeneratedCheckConstraintName(entityName, sql);

        private static IReadOnlyList<ScaffoldCheckConstraintConfiguration> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildEnumCheckConstraintConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

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
            => ScaffoldFeatureConfigurationAdapter.BuildExpressionIndexConfigurations(entityByTable, features);

        private static bool IsProviderOwnedExpressionIndexDetail(string detail)
            => ScaffoldFeatureConfigurationBuilder.IsProviderOwnedExpressionIndexDetail(detail);

        private static IReadOnlyList<ScaffoldCollationConfiguration> BuildCollationConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildCollationConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

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
            => ScaffoldFeatureConfigurationAdapter.BuildComputedColumnConfigurations(
                entityByTable,
                columnPropertiesByTable,
                features);

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
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureNameMap(features, kinds);

        private static HashSet<string> BuildProviderNativeTemporalTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderNativeTemporalTableKeys(features);

        private static HashSet<string> BuildProviderOwnedWriteBlockedTableKeys(
            IReadOnlySet<string> providerNativeTemporalTableKeys,
            IReadOnlySet<string> providerOwnedTriggerTableKeys,
            IReadOnlySet<string> providerSpecificIdentityStrategyTableKeys,
            IReadOnlySet<string> providerSpecificDefaultTableKeys,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> providerSpecificColumnTypesByTable)
            => ScaffoldFeatureConfigurationAdapter.BuildProviderOwnedWriteBlockedTableKeys(
                providerNativeTemporalTableKeys,
                providerOwnedTriggerTableKeys,
                providerSpecificIdentityStrategyTableKeys,
                providerSpecificDefaultTableKeys,
                providerSpecificColumnTypesByTable);

        private static HashSet<string> BuildFeatureTableKeys(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureTableKeys(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> BuildFeatureDetailMap(
            IEnumerable<ScaffoldUnsupportedFeature> features,
            params string[] kinds)
            => ScaffoldFeatureConfigurationAdapter.BuildFeatureDetailMap(features, kinds);

        private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldDecimalPrecision>> BuildDecimalPrecisionMap(
            IEnumerable<ScaffoldUnsupportedFeature> features)
            => ScaffoldFeatureConfigurationAdapter.BuildDecimalPrecisionMap(features);

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
            => ScaffoldContextAdapter.Write(
                namespaceName,
                contextName,
                entities,
                relationships,
                manyToManyJoins,
                routineStubs,
                compositePrimaryKeys,
                defaultValueConfigurations,
                checkConstraintConfigurations,
                computedColumnConfigurations,
                expressionIndexConfigurations,
                collationConfigurations,
                sequenceStubs,
                identityOptionConfigurations,
                precisionConfigurations,
                columnFacetConfigurations,
                pluralizeQueryProperties,
                useNullableReferenceTypes,
                entityNamespaceName,
                useDatabaseNames);

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
            => IdentifierEscaping.EscapeTable(provider, table, schema);

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

        internal sealed record ScaffoldFeatureConfigurations(
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

        internal sealed record ScaffoldEntityFileSet(
            IReadOnlyList<(string Path, string Content)> GeneratedFiles,
            IReadOnlyList<string> EntityNames);

        internal readonly record struct ScaffoldTable(string Name, string? Schema);

        internal readonly record struct ScaffoldSkippedObject(
            string? Schema,
            string Name,
            string Kind,
            string Detail,
            string? Comment);

        internal readonly record struct ScaffoldPrimaryKey(
            string EntityName,
            string[] PropertyNames,
            string? ConstraintName);

        internal readonly record struct ScaffoldDefaultValueConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string DefaultValueSql);

        internal readonly record struct ScaffoldIdentityOptionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            long Seed,
            long Increment);

        internal readonly record struct ScaffoldPrecisionConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int Precision,
            int? Scale);

        internal readonly record struct ScaffoldColumnFacetConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            int? MaxLength,
            bool? IsUnicode,
            bool IsFixedLength);

        internal readonly record struct ScaffoldCheckConstraintConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string Sql);

        internal readonly record struct ScaffoldComputedColumnConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Sql,
            bool Stored);

        internal readonly record struct ScaffoldExpressionIndexConfiguration(
            string TableKey,
            string EntityName,
            string Name,
            string ExpressionSql,
            bool IsUnique,
            string? FilterSql);

        internal readonly record struct ScaffoldCollationConfiguration(
            string TableKey,
            string EntityName,
            string ColumnName,
            string PropertyName,
            string Collation);

        internal readonly record struct ScaffoldForeignKey(
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

        internal readonly record struct ScaffoldIndex(
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

        internal readonly record struct ScaffoldRelationship(
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

        internal readonly record struct ScaffoldManyToManyJoin(
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

        internal readonly record struct ScaffoldManyToManyNavigation(
            string TargetEntityName,
            string CollectionNavigationName);

        internal readonly record struct ScaffoldDecimalPrecision(
            int Precision,
            int? Scale);

        internal readonly record struct ScaffoldUnsupportedFeature(
            string TableKey,
            string Kind,
            string Name,
            string Detail)
        {
            public IReadOnlyDictionary<string, object?>? Metadata { get; init; }
        }
    }
}
