#nullable enable
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelDiscovery
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
                options.UseDatabaseNames,
                options.UsePluralizer);

            var metadata = await ScaffoldModelMetadataDiscovery.DiscoverAsync(
                connection,
                provider,
                tables,
                tableInfos,
                entityByTable,
                queryArtifactTableKeys,
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
                metadata.ColumnStoreTypesByTable,
                metadata.StringBinaryFacetsByTable,
                metadata.CommentsByTable,
                metadata.IdentityColumnsByTable,
                metadata.ScaffoldedTableKeys,
                metadata.Indexes,
                metadata.ForeignKeys,
                metadata.UnsupportedFeatures,
                metadata.FeatureConfigurations);
        }
    }
}
