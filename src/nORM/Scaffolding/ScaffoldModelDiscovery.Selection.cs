#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldModelDiscovery
    {
        private static async Task<(IReadOnlyList<ScaffoldTable> Tables, IReadOnlyList<ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys)> BuildScaffoldObjectSelectionAsync(
            DbConnection connection,
            DatabaseProvider provider,
            ScaffoldOptions options)
        {
            var filterCatalog = GetScaffoldFilterCatalog(connection, provider);
            var discoveredTables = await ScaffoldSchemaDiscoveryAdapter.GetTablesAsync(connection, provider).ConfigureAwait(false);
            var discoveredSkippedObjects = await ScaffoldSkippedObjectDiscovery.GetSkippedObjectsAsync(connection, provider).ConfigureAwait(false);
            var selection = ScaffoldObjectSelectionBuilder.BuildSelection(
                discoveredTables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                discoveredSkippedObjects,
                options,
                provider,
                filterCatalog);
            // A selected table or view that cannot be read (a broken view, revoked access, an object dropped or
            // still being created by a concurrent session) must not abort the whole scaffold. Probe each and route
            // the unreadable ones to skipped objects with a loud reason so the rest of the schema still scaffolds.
            var (accessibleTables, inaccessibleObjects) =
                await PartitionByAccessibilityAsync(connection, provider, selection.Tables).ConfigureAwait(false);
            var skippedObjects = await ScaffoldSkippedObjectDiscovery.AttachCommentsAsync(connection, provider, selection.SkippedObjects).ConfigureAwait(false);
            return (
                accessibleTables.Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldTable).ToArray(),
                skippedObjects.Concat(inaccessibleObjects).Select(ScaffoldSchemaDiscoveryAdapter.ToScaffoldSkippedObject).ToArray(),
                selection.QueryArtifactTableKeys);
        }

        /// <summary>
        /// Splits the selected tables/views into those that can be read and those that cannot. A trivial
        /// <c>SELECT * ... WHERE 1=0</c> probe is enough to surface a broken view, a permission failure, or an
        /// object that vanished after enumeration; an unreadable object is returned as a skipped object rather
        /// than allowed to abort the whole run, so one bad object never costs the entire scaffold.
        /// </summary>
        private static async Task<(IReadOnlyList<ScaffoldTableInfo> Accessible, IReadOnlyList<ScaffoldSkippedObjectInfo> Inaccessible)> PartitionByAccessibilityAsync(
            DbConnection connection,
            DatabaseProvider provider,
            IReadOnlyList<ScaffoldTableInfo> tables)
        {
            var accessible = new List<ScaffoldTableInfo>(tables.Count);
            var inaccessible = new List<ScaffoldSkippedObjectInfo>();
            foreach (var table in tables)
            {
                try
                {
                    await using var cmd = connection.CreateCommand();
                    cmd.CommandText = $"SELECT * FROM {IdentifierEscaping.EscapeTable(provider, table.Name, table.Schema)} WHERE 1=0";
                    await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly).ConfigureAwait(false);
                    accessible.Add(table);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    inaccessible.Add(new ScaffoldSkippedObjectInfo(
                        table.Schema,
                        table.Name,
                        "InaccessibleTable",
                        "Table or view could not be read and was skipped so the rest of the schema still scaffolds: " + FirstLine(ex.Message),
                        null));
                }
            }

            return (accessible, inaccessible);
        }

        private static string FirstLine(string message)
        {
            var line = message.Replace('\r', '\n').Split('\n')[0].Trim();
            return line.Length > 200 ? line[..200] : line;
        }

        private static ScaffoldTableInfo[] BuildTableInfos(IReadOnlyList<ScaffoldTable> tables)
            => tables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray();

        private static string? GetScaffoldFilterCatalog(DbConnection connection, DatabaseProvider provider)
            => ScaffoldProviderKind.IsMySql(provider) ? NullIfWhiteSpace(connection.Database) : null;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
