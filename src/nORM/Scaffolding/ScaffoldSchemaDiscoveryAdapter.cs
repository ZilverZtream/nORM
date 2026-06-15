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
        public static async Task<IReadOnlyList<ScaffoldTable>> GetTablesAsync(
            DbConnection connection,
            DatabaseProvider provider)
            => (await ScaffoldTableDiscovery.GetTablesAsync(connection, provider).ConfigureAwait(false))
                .Select(ToScaffoldTable)
                .ToArray();

        public static async Task<IReadOnlyList<ScaffoldSkippedObject>> GetSkippedObjectsAsync(
            DbConnection connection,
            DatabaseProvider provider)
        {
            var discoveredObjects = await ScaffoldSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            discoveredObjects = await ScaffoldSkippedObjectDiscovery.AttachCommentsAsync(connection, provider, discoveredObjects).ConfigureAwait(false);
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

        public static async Task<IReadOnlyList<ScaffoldIndex>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var indexes = await ScaffoldIndexDiscovery.GetIndexesAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertIndexes(indexes);
        }

        public static async Task<IReadOnlyList<ScaffoldForeignKey>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTable> tables)
        {
            var foreignKeys = await ScaffoldForeignKeyDiscovery.GetForeignKeysAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertForeignKeys(foreignKeys);
        }
    }
}
