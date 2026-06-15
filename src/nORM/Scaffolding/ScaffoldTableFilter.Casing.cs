#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Providers;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldTableFilter
    {
        private static ScaffoldTableInfo ApplyRequestedTableCasing(
            DatabaseProvider provider,
            ScaffoldTableInfo table,
            IReadOnlyList<string> requested,
            string? filterCatalog)
        {
            var request = requested.FirstOrDefault(filter => MatchesTableFilter(provider, table, filter, filterCatalog));
            if (string.IsNullOrWhiteSpace(request))
                return table;

            var requestedSchema = GetSchemaNameOrNull(request);
            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && string.Equals(requestedSchema, table.Schema, StringComparison.OrdinalIgnoreCase))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), requestedSchema, table.Kind);
            }

            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && IsDefaultSqliteSchemaQualifiedFilter(provider, table.Schema, table.Name, request))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), table.Schema, table.Kind);
            }

            if (!string.IsNullOrWhiteSpace(requestedSchema)
                && IsDefaultMySqlCatalogQualifiedFilter(provider, table.Schema, table.Name, request, filterCatalog))
            {
                return new ScaffoldTableInfo(GetUnqualifiedName(request), table.Schema, table.Kind);
            }

            if (requestedSchema is null && string.Equals(request, table.Name, StringComparison.OrdinalIgnoreCase))
                return new ScaffoldTableInfo(request, table.Schema, table.Kind);

            return table;
        }

        private static string DisplayTableMatch(ScaffoldTableInfo table)
            => string.IsNullOrWhiteSpace(table.Schema)
                ? "<default>." + table.Name
                : TableKey(table.Schema, table.Name);
    }
}
