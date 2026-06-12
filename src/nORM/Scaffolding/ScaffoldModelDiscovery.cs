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
            var (tables, skippedObjects, queryArtifactTableKeys) = await BuildScaffoldObjectSelectionAsync(
                connection,
                provider,
                options).ConfigureAwait(false);
            var tableInfos = BuildTableInfos(tables);
            var entityByTable = ScaffoldEntityNameBuilder.BuildEntityNameMap(
                tableInfos,
                options.UseDatabaseNames);

            var metadata = await ScaffoldModelMetadataDiscovery.DiscoverAsync(
                connection,
                provider,
                tables,
                tableInfos,
                entityByTable,
                options.UseDatabaseNames).ConfigureAwait(false);

            return new ScaffoldModelDiscoveryResult(
                tables,
                skippedObjects,
                queryArtifactTableKeys,
                entityByTable,
                metadata.ColumnPropertiesByTable,
                metadata.MemberNamesByTable,
                metadata.PrimaryKeyColumnsByTable,
                metadata.PrimaryKeyConstraintNamesByTable,
                metadata.NonNullableColumnsByTable,
                metadata.SqliteDeclaredTypesByTable,
                metadata.StringBinaryFacetsByTable,
                metadata.CommentsByTable,
                metadata.IdentityColumnsByTable,
                metadata.ScaffoldedTableKeys,
                metadata.Indexes,
                metadata.ForeignKeys,
                metadata.UnsupportedFeatures,
                metadata.FeatureConfigurations);
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

        private static async Task<(IReadOnlyList<DatabaseScaffolder.ScaffoldTable> Tables, IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys)> BuildScaffoldObjectSelectionAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldOptions options)
        {
            var filterCatalog = GetScaffoldFilterCatalog(connection, provider);
            var discoveredTables = await ScaffoldSchemaDiscoveryAdapter.GetTablesAsync(connection, provider).ConfigureAwait(false);
            var discoveredSkippedObjects = await ScaffoldSchemaDiscoveryAdapter.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
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

        private static ScaffoldTableInfo[] BuildTableInfos(IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

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
