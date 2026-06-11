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
        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
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

            return Array.Empty<DatabaseScaffolder.ScaffoldUnsupportedFeature>();
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature> ConvertUnsupportedFeatures(
            IReadOnlyList<ScaffoldUnsupportedFeatureInfo> features)
        {
            var converted = new DatabaseScaffolder.ScaffoldUnsupportedFeature[features.Count];
            for (var i = 0; i < features.Count; i++)
            {
                var feature = features[i];
                converted[i] = new DatabaseScaffolder.ScaffoldUnsupportedFeature(
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

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetPostgresEnumColumnFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
        {
            if (!provider.GetType().Name.Contains("Postgres", StringComparison.OrdinalIgnoreCase))
                return Array.Empty<DatabaseScaffolder.ScaffoldUnsupportedFeature>();

            var tableKeys = tables.Select(t => TableKey(t.Schema, t.Name)).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var features = await ScaffoldPostgresUnsupportedFeatureDiscovery.GetEnumColumnFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetSqliteUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldSqliteUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, ToScaffoldTableInfos(tables), tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetSqlServerUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldSqlServerUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetPostgresUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldPostgresUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }

        private static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldUnsupportedFeature>> GetMySqlUnsupportedSchemaFeaturesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            HashSet<string> tableKeys)
        {
            var features = await ScaffoldMySqlUnsupportedFeatureDiscovery.GetFeaturesAsync(connection, provider, ToScaffoldTableInfos(tables), tableKeys).ConfigureAwait(false);
            return ConvertUnsupportedFeatures(features);
        }
    }
}
