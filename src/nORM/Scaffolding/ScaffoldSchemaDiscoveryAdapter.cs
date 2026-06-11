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

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldIndex>> GetIndexesAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
        {
            var indexes = await ScaffoldIndexDiscovery.GetIndexesAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertIndexes(indexes);
        }

        public static async Task<IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey>> GetForeignKeysAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables)
        {
            var foreignKeys = await ScaffoldForeignKeyDiscovery.GetForeignKeysAsync(connection, provider, ToScaffoldTableInfos(tables)).ConfigureAwait(false);
            return ConvertForeignKeys(foreignKeys);
        }
    }
}
