#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSchemaDiscoveryAdapter
    {
        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (ScaffoldProviderKind.IsSqlite(provider))
                return await GetSqliteUnsupportedSchemaFeaturesAsync(connection, provider, tables, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsSqlServer(provider))
                return await GetSqlServerUnsupportedSchemaFeaturesAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsPostgres(provider))
                return await GetPostgresUnsupportedSchemaFeaturesAsync(connection, tableKeys).ConfigureAwait(false);

            if (ScaffoldProviderKind.IsMySql(provider))
                return await GetMySqlUnsupportedSchemaFeaturesAsync(connection, provider, tables, tableKeys).ConfigureAwait(false);

            return Array.Empty<ScaffoldUnsupportedFeature>();
        }

        public static IReadOnlyList<ScaffoldUnsupportedFeature> ConvertUnsupportedFeatures(
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

        public static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetPostgresEnumColumnFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            if (!ScaffoldProviderKind.IsPostgres(provider))
                return Array.Empty<ScaffoldUnsupportedFeature>();

            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var features = await ScaffoldPostgresUnsupportedFeatureDiscovery.GetEnumColumnFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<ScaffoldUnsupportedFeature>> GetSqliteUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldSqliteUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, ToScaffoldTableInfos(tables), tableKeys).ConfigureAwait(false);
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
            var features = await ScaffoldMySqlUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, ToScaffoldTableInfos(tables), tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }
    }
}
