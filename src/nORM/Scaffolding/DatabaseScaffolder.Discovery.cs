#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
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
    }
}
