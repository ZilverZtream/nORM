#nullable enable
using System;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        private static bool TryParseRelationshipPrincipalKeyDetail(
            string detail,
            out string principalTable,
            out string[] principalColumns)
        {
            principalTable = string.Empty;
            principalColumns = Array.Empty<string>();
            const string prefix = "FK references ";
            if (!detail.StartsWith(prefix, StringComparison.Ordinal))
                return false;

            var columnsStart = detail.IndexOf(".(", prefix.Length, StringComparison.Ordinal);
            if (columnsStart < 0)
                return false;

            var columnsEnd = detail.IndexOf(')', columnsStart + 2);
            if (columnsEnd <= columnsStart + 2)
                return false;

            principalTable = detail.Substring(prefix.Length, columnsStart - prefix.Length);
            principalColumns = detail
                .Substring(columnsStart + 2, columnsEnd - columnsStart - 2)
                .Split(',')
                .Select(static column => column.Trim())
                .Where(static column => column.Length > 0)
                .ToArray();
            return !string.IsNullOrWhiteSpace(principalTable) && principalColumns.Length > 0;
        }

        private static bool TryParseReferentialActionDetail(
            string detail,
            out string onDelete,
            out string onUpdate)
        {
            onDelete = string.Empty;
            onUpdate = string.Empty;
            const string deletePrefix = "ON DELETE ";
            const string updateMarker = "; ON UPDATE ";
            if (!detail.StartsWith(deletePrefix, StringComparison.OrdinalIgnoreCase))
                return false;

            var updateIndex = detail.IndexOf(updateMarker, deletePrefix.Length, StringComparison.OrdinalIgnoreCase);
            if (updateIndex < 0)
                return false;

            onDelete = detail.Substring(deletePrefix.Length, updateIndex - deletePrefix.Length).Trim();
            onUpdate = detail[(updateIndex + updateMarker.Length)..].Trim();
            return onDelete.Length > 0 && onUpdate.Length > 0;
        }
    }
}
