#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        public static bool IsQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "VirtualTable", StringComparison.OrdinalIgnoreCase)
               || (string.Equals(obj.Kind, "Synonym", StringComparison.OrdinalIgnoreCase) && ScaffoldSkippedObjectMetadataBuilder.IsTableLikeSqlServerSynonym(obj.Detail));

        public static bool ShouldEmitQueryArtifactObject(
            ScaffoldSkippedObjectInfo obj,
            ScaffoldOptions options,
            DatabaseProvider provider,
            string? filterCatalog)
        {
            if (!IsQueryArtifactObject(obj))
                return false;

            if (options.EmitViewEntities || options.EmitQueryArtifacts)
                return true;

            var requested = GetRequestedTableFilters(options);
            var requestedSchemas = GetRequestedSchemaFilters(options);
            if (requested.Length == 0 && requestedSchemas.Length == 0)
                return IsDefaultQueryArtifactObject(obj);

            return requested.Any(request => MatchesSkippedObjectFilter(provider, obj, request, filterCatalog))
                   || requestedSchemas.Any(schema => MatchesSchemaFilter(provider, obj.Schema, schema, filterCatalog));
        }

        private static bool IsShadowOfEmittedVirtualTable(
            ScaffoldSkippedObjectInfo obj,
            IReadOnlyList<ScaffoldSkippedObjectInfo> emittedVirtualTables)
            => string.Equals(obj.Kind, "VirtualTableShadow", StringComparison.OrdinalIgnoreCase)
               && emittedVirtualTables.Any(vt =>
                   string.Equals(vt.Schema ?? string.Empty, obj.Schema ?? string.Empty, StringComparison.OrdinalIgnoreCase)
                   && obj.Name.StartsWith(vt.Name + "_", StringComparison.OrdinalIgnoreCase));

        private static bool IsDefaultQueryArtifactObject(ScaffoldSkippedObjectInfo obj)
            => string.Equals(obj.Kind, "View", StringComparison.OrdinalIgnoreCase)
               || string.Equals(obj.Kind, "MaterializedView", StringComparison.OrdinalIgnoreCase);
    }
}
