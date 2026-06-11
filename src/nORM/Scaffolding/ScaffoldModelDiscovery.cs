#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldModelDiscovery
    {
        public static async Task<ScaffoldModelDiscoveryResult> BuildAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldOptions options)
        {
            var filterCatalog = GetScaffoldFilterCatalog(connection, provider);
            var discoveredTables = await ScaffoldSchemaDiscoveryAdapter.GetTablesAsync(connection, provider).ConfigureAwait(false);
            var discoveredSkippedObjects = await ScaffoldSchemaDiscoveryAdapter.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            var (tables, skippedObjects, queryArtifactTableKeys) = BuildScaffoldObjectSelection(
                discoveredTables,
                discoveredSkippedObjects,
                options,
                provider,
                filterCatalog);
            var entityByTable = ScaffoldEntityNameBuilder.BuildEntityNameMap(
                tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                options.UseDatabaseNames);
            var tableInfos = tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();
            var columnPropertiesByTable = await ScaffoldColumnPropertyDiscovery.GetColumnPropertyNamesAsync(
                connection,
                provider,
                tableInfos,
                entityByTable,
                options.UseDatabaseNames).ConfigureAwait(false);
            var memberNamesByTable = ScaffoldColumnPropertyNameBuilder.BuildMemberNameMap(columnPropertiesByTable, entityByTable);
            var primaryKeyColumnsByTable = await ScaffoldKeyDiscovery.GetPrimaryKeyColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var primaryKeyConstraintNamesByTable = await ScaffoldKeyDiscovery.GetPrimaryKeyConstraintNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var nonNullableColumnsByTable = await ScaffoldColumnDiscovery.GetNonNullableColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var sqliteDeclaredTypesByTable = ScaffoldProviderKind.IsSqlite(provider)
                ? await GetSqliteDeclaredColumnTypesAsync(connection, provider, tables).ConfigureAwait(false)
                : new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            var stringBinaryFacetsByTable = await ScaffoldColumnDiscovery.GetStringBinaryFacetsAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var commentsByTable = await ScaffoldCommentDiscovery.GetScaffoldCommentsAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var identityColumnsByTable = await ScaffoldColumnDiscovery.GetIdentityColumnNamesAsync(connection, provider, tableInfos).ConfigureAwait(false);
            var scaffoldedTableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var indexes = FilterIndexesToScaffoldedTables(
                await ScaffoldSchemaDiscoveryAdapter.GetIndexesAsync(connection, provider, tables).ConfigureAwait(false),
                scaffoldedTableKeys);
            var foreignKeys = FilterForeignKeysToScaffoldedTables(
                await ScaffoldSchemaDiscoveryAdapter.GetForeignKeysAsync(connection, provider, tables).ConfigureAwait(false),
                scaffoldedTableKeys);
            var unsupportedFeatures = (await ScaffoldSchemaDiscoveryAdapter.GetUnsupportedSchemaFeaturesAsync(connection, provider, tables).ConfigureAwait(false)).ToList();
            unsupportedFeatures.AddRange(await ScaffoldSchemaDiscoveryAdapter.GetPostgresEnumColumnFeaturesAsync(connection, provider, tables).ConfigureAwait(false));
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedDescendingIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedIncludedColumnIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.RemoveSupportedPartialIndexDiagnostics(unsupportedFeatures, indexes);
            ScaffoldUnsupportedDiagnosticAdapter.AddMissingPrimaryKeyDiagnostics(unsupportedFeatures, tables, primaryKeyColumnsByTable, columnPropertiesByTable);
            ScaffoldUnsupportedDiagnosticAdapter.AddReferentialActionDiagnostics(unsupportedFeatures, foreignKeys);
            AddRelationshipPrincipalKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable, indexes);
            AddRelationshipDependentKeyDiagnostics(unsupportedFeatures, foreignKeys, primaryKeyColumnsByTable);
            var featureConfigurations = ScaffoldFeatureConfigurationAdapter.BuildFeatureConfigurations(
                unsupportedFeatures,
                entityByTable,
                columnPropertiesByTable,
                stringBinaryFacetsByTable);

            return new ScaffoldModelDiscoveryResult(
                tables,
                skippedObjects,
                queryArtifactTableKeys,
                entityByTable,
                columnPropertiesByTable,
                memberNamesByTable,
                primaryKeyColumnsByTable,
                primaryKeyConstraintNamesByTable,
                nonNullableColumnsByTable,
                sqliteDeclaredTypesByTable,
                stringBinaryFacetsByTable,
                commentsByTable,
                identityColumnsByTable,
                scaffoldedTableKeys,
                indexes,
                foreignKeys,
                unsupportedFeatures,
                featureConfigurations);
        }

        public static async Task<IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>> GetSqliteDeclaredColumnTypesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
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

        public static string SqlitePragma(DatabaseProvider provider, string? schema, string pragmaName, string argument)
        {
            var prefix = string.IsNullOrWhiteSpace(schema)
                ? string.Empty
                : provider.Escape(schema!) + ".";
            return $"PRAGMA {prefix}{pragmaName}({IdentifierEscaping.EscapeSingle(provider, argument)})";
        }

        private static (IReadOnlyList<DatabaseScaffolder.ScaffoldTable> Tables, IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys) BuildScaffoldObjectSelection(
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> discoveredTables,
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> discoveredSkippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            var selection = ScaffoldObjectSelectionBuilder.BuildSelection(
                discoveredTables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                ScaffoldSchemaDiscoveryAdapter.ConvertSkippedObjectInfos(discoveredSkippedObjects),
                options,
                provider,
                filterCatalog);
            return (
                selection.Tables.Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldTable).ToArray(),
                selection.SkippedObjects.Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldSkippedObject).ToArray(),
                selection.QueryArtifactTableKeys);
        }

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> FilterIndexesToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes,
            IReadOnlySet<string> scaffoldedTableKeys)
            => indexes
                .Where(index => scaffoldedTableKeys.Contains(index.TableKey))
                .ToArray();

        private static IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> FilterForeignKeysToScaffoldedTables(
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlySet<string> scaffoldedTableKeys)
            => foreignKeys
                .Where(fk => scaffoldedTableKeys.Contains(TableKey(fk.DependentSchema, fk.DependentTable))
                             && scaffoldedTableKeys.Contains(TableKey(fk.PrincipalSchema, fk.PrincipalTable)))
                .ToArray();

        private static void AddRelationshipPrincipalKeyDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildPrincipalKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable,
                    ScaffoldRelationshipAdapter.ConvertIndexInfos(indexes))));

        private static void AddRelationshipDependentKeyDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable)
            => features.AddRange(ScaffoldSchemaDiscoveryAdapter.ConvertUnsupportedFeatures(
                ScaffoldRelationshipDiagnosticBuilder.BuildDependentKeyDiagnostics(
                    ScaffoldRelationshipAdapter.ConvertForeignKeyInfos(foreignKeys),
                    primaryKeyColumnsByTable)));

        private static string? GetScaffoldFilterCatalog(DbConnection connection, DatabaseProvider provider)
            => ScaffoldProviderKind.IsMySql(provider) ? NullIfWhiteSpace(connection.Database) : null;

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }

    internal sealed record ScaffoldModelDiscoveryResult(
        IReadOnlyList<DatabaseScaffolder.ScaffoldTable> Tables,
        IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> SkippedObjects,
        IReadOnlySet<string> QueryArtifactTableKeys,
        IReadOnlyDictionary<string, string> EntityByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> ColumnPropertiesByTable,
        Dictionary<string, HashSet<string>> MemberNamesByTable,
        IReadOnlyDictionary<string, IReadOnlyList<string>> PrimaryKeyColumnsByTable,
        IReadOnlyDictionary<string, string> PrimaryKeyConstraintNamesByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NonNullableColumnsByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> SqliteDeclaredTypesByTable,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, ScaffoldColumnFacet>> StringBinaryFacetsByTable,
        IReadOnlyDictionary<string, ScaffoldComments> CommentsByTable,
        IReadOnlyDictionary<string, IReadOnlySet<string>> IdentityColumnsByTable,
        IReadOnlySet<string> ScaffoldedTableKeys,
        IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> Indexes,
        IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> ForeignKeys,
        List<DatabaseScaffolder.ScaffoldUnsupportedFeature> UnsupportedFeatures,
        DatabaseScaffolder.ScaffoldFeatureConfigurations FeatureConfigurations);
}
