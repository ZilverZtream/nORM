#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        private static bool TryAddKeyOrRelationshipFeatureMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            switch (feature.Kind)
            {
                case "MissingPrimaryKey":
                    AddMissingPrimaryKeyMetadata(metadata, feature);
                    return true;
                case "RelationshipPrincipalKey":
                    AddRelationshipPrincipalKeyMetadata(metadata, feature);
                    return true;
                case "RelationshipDependentKey":
                    AddRelationshipDependentKeyMetadata(metadata, feature);
                    return true;
                case "ReferentialAction":
                    AddReferentialActionMetadata(metadata, feature);
                    return true;
                default:
                    return false;
            }
        }

        private static void AddMissingPrimaryKeyMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["readOnlyEntity"] = true;
            metadata["generatedWritesSupported"] = false;
            metadata["generatedNavigationSupported"] = false;
            if (!metadata.ContainsKey("table"))
                metadata["table"] = feature.TableKey;
            if (!metadata.ContainsKey("reason"))
                metadata["reason"] = "missing-primary-key";
        }

        private static void AddRelationshipPrincipalKeyMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["navigationSuppressed"] = true;
            metadata["reason"] = "principal-key-not-scaffoldable";
            if (!metadata.ContainsKey("dependentTable"))
                metadata["dependentTable"] = feature.TableKey;
            if (TryParseRelationshipPrincipalKeyDetail(feature.Detail, out var principalTable, out var principalColumns))
            {
                if (!metadata.ContainsKey("principalTable"))
                    metadata["principalTable"] = principalTable;
                if (!metadata.ContainsKey("principalColumns"))
                    metadata["principalColumns"] = principalColumns;
            }
        }

        private static void AddRelationshipDependentKeyMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["navigationSuppressed"] = true;
            metadata["reason"] = "dependent-table-keyless";
            metadata["generatedNavigationSupported"] = false;
            if (!metadata.ContainsKey("dependentTable"))
                metadata["dependentTable"] = feature.TableKey;
        }

        private static void AddReferentialActionMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["navigationSuppressed"] = true;
            metadata["generatedNavigationSupported"] = false;
            metadata["reason"] = "referential-action-not-scaffoldable";
            if (TryParseReferentialActionDetail(feature.Detail, out var onDelete, out var onUpdate))
            {
                metadata["onDelete"] = onDelete;
                metadata["onUpdate"] = onUpdate;
            }
        }

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
