#nullable enable
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    public static partial class DatabaseScaffolder
    {
        private static string? GetScaffoldFilterCatalog(DbConnection connection, DatabaseProvider provider)
            => ScaffoldProviderKind.IsMySql(provider) ? NullIfWhiteSpace(connection.Database) : null;

        private static (IReadOnlyList<ScaffoldTable> Tables, IReadOnlyList<ScaffoldSkippedObject> SkippedObjects, IReadOnlySet<string> QueryArtifactTableKeys) BuildScaffoldObjectSelection(
            IReadOnlyList<ScaffoldTable> discoveredTables,
            IReadOnlyList<ScaffoldSkippedObject> discoveredSkippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            var selection = ScaffoldObjectSelectionBuilder.BuildSelection(
                discoveredTables.Select(static table => new ScaffoldTableInfo(table.Name, table.Schema)).ToArray(),
                ConvertSkippedObjectInfos(discoveredSkippedObjects),
                options,
                provider,
                filterCatalog);
            return (
                selection.Tables.Select(ToScaffoldTable).ToArray(),
                selection.SkippedObjects.Select(ToScaffoldSkippedObject).ToArray(),
                selection.QueryArtifactTableKeys);
        }

        private static IReadOnlyList<ScaffoldTable> FilterTables(
            IReadOnlyList<ScaffoldTable> tables,
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
            => ScaffoldTableFilter.FilterTables(
                    ToScaffoldTableInfos(tables),
                    ToSkippedObjectInfos(skippedObjects),
                    options,
                    provider,
                    filterCatalog)
                .Select(ToScaffoldTable)
                .ToArray();

        private static void EnsureNoTableKeyCollisions(IReadOnlyList<ScaffoldTable> tables)
            => ScaffoldTableFilter.EnsureNoTableKeyCollisions(ToScaffoldTableInfos(tables));

        private static IReadOnlyList<ScaffoldSkippedObject> FilterSkippedObjects(
            IReadOnlyList<ScaffoldSkippedObject> skippedObjects,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog,
            IReadOnlyList<ScaffoldSkippedObject>? emittedQueryArtifacts = null)
            => ScaffoldTableFilter.FilterSkippedObjects(
                    ToSkippedObjectInfos(skippedObjects),
                    options,
                    provider,
                    filterCatalog,
                    emittedQueryArtifacts is null ? null : ToSkippedObjectInfos(emittedQueryArtifacts))
                .Select(ToScaffoldSkippedObject)
                .ToArray();

        private static bool IsQueryArtifactObject(ScaffoldSkippedObject obj)
            => ScaffoldTableFilter.IsQueryArtifactObject(ToSkippedObjectInfo(obj));

        private static bool ShouldEmitQueryArtifactObject(ScaffoldSkippedObject obj, ScaffoldOptions options, DatabaseProvider provider, string? filterCatalog)
            => ScaffoldTableFilter.ShouldEmitQueryArtifactObject(ToSkippedObjectInfo(obj), options, provider, filterCatalog);

        private static bool IsTableLikeSqlServerSynonym(string detail)
            => ScaffoldSkippedObjectMetadataBuilder.IsTableLikeSqlServerSynonym(detail);

        private static string[] GetRequestedTableFilters(ScaffoldOptions options)
            => ScaffoldTableFilter.GetRequestedTableFilters(options);

        private static string[] GetRequestedSchemaFilters(ScaffoldOptions options)
            => ScaffoldTableFilter.GetRequestedSchemaFilters(options);
    }
}
