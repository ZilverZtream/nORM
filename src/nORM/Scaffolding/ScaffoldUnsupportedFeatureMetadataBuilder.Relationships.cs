#nullable enable
using System.Collections.Generic;

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
            metadata["generatedNavigationSupported"] = false;
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

    }
}
