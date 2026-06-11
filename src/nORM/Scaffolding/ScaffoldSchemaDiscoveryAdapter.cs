#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static class ScaffoldSchemaDiscoveryAdapter
    {
        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldTable>> GetTablesAsync(
            DbConnection connection,
            DatabaseProvider provider)
            => (await ScaffoldTableDiscovery.GetTablesAsync(connection, provider).ConfigureAwait(false))
                .Select(ToScaffoldTable)
                .ToArray();

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject>> GetSkippedObjectsAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            var discoveredObjects = await ScaffoldSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            if (discoveredObjects.Count == 0)
                return Array.Empty<DatabaseScaffolder.ScaffoldSkippedObject>();

            var objects = new DatabaseScaffolder.ScaffoldSkippedObject[discoveredObjects.Count];
            for (var i = 0; i < discoveredObjects.Count; i++)
            {
                var obj = discoveredObjects[i];
                objects[i] = new DatabaseScaffolder.ScaffoldSkippedObject(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);
            }

            return objects;
        }

        public static IReadOnlyList<ScaffoldSkippedObjectInfo> ConvertSkippedObjectInfos(
            IReadOnlyList<DatabaseScaffolder.ScaffoldSkippedObject> objects)
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

        public static ScaffoldTableInfo[] ToScaffoldTableInfos(IEnumerable<DatabaseScaffolder.ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        public static DatabaseScaffolder.ScaffoldTable ToScaffoldTable(ScaffoldTableInfo table)
            => new(table.Name, table.Schema);

        public static ScaffoldSkippedObjectInfo[] ToSkippedObjectInfos(
            IEnumerable<DatabaseScaffolder.ScaffoldSkippedObject> objects)
            => objects.Select(ToSkippedObjectInfo).ToArray();

        public static ScaffoldSkippedObjectInfo ToSkippedObjectInfo(DatabaseScaffolder.ScaffoldSkippedObject obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static DatabaseScaffolder.ScaffoldSkippedObject ToScaffoldSkippedObject(ScaffoldSkippedObjectInfo obj)
            => new(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment);

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldIndex>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
        {
            var indexes = await ScaffoldIndexDiscovery.GetIndexesAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertIndexes(indexes);
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> ConvertIndexes(IReadOnlyList<ScaffoldIndexInfo> indexes)
        {
            var converted = new DatabaseScaffolder.ScaffoldIndex[indexes.Count];
            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                converted[i] = new DatabaseScaffolder.ScaffoldIndex(
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

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
        {
            var foreignKeys = await ScaffoldForeignKeyDiscovery.GetForeignKeysAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertForeignKeys(foreignKeys);
        }

        public static IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> ConvertForeignKeys(IReadOnlyList<ScaffoldForeignKeyInfo> foreignKeys)
        {
            var converted = new DatabaseScaffolder.ScaffoldForeignKey[foreignKeys.Count];
            for (var i = 0; i < foreignKeys.Count; i++)
            {
                var foreignKey = foreignKeys[i];
                converted[i] = new DatabaseScaffolder.ScaffoldForeignKey(
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

        public static IReadOnlyDictionary<string, object?> BuildSkippedObjectMetadata(
            DatabaseScaffolder.ScaffoldSkippedObject obj)
            => ScaffoldSkippedObjectMetadataBuilder.BuildMetadata(
                new ScaffoldSkippedObjectInfo(obj.Schema, obj.Name, obj.Kind, obj.Detail, obj.Comment));

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

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
