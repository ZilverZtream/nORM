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
