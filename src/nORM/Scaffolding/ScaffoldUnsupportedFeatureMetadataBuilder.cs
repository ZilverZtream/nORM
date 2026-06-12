#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        public static IReadOnlyDictionary<string, object?> BuildMetadata(ScaffoldUnsupportedFeatureInfo feature)
        {
            var metadata = feature.Metadata is null
                ? new Dictionary<string, object?>(StringComparer.Ordinal)
                : new Dictionary<string, object?>(feature.Metadata, StringComparer.Ordinal);
            switch (feature.Kind)
            {
                case "MissingPrimaryKey":
                    metadata["readOnlyEntity"] = true;
                    metadata["generatedWritesSupported"] = false;
                    metadata["generatedNavigationSupported"] = false;
                    if (!metadata.ContainsKey("table"))
                        metadata["table"] = feature.TableKey;
                    if (!metadata.ContainsKey("reason"))
                        metadata["reason"] = "missing-primary-key";
                    break;
                case "RelationshipPrincipalKey":
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
                    break;
                case "RelationshipDependentKey":
                    metadata["navigationSuppressed"] = true;
                    metadata["reason"] = "dependent-table-keyless";
                    metadata["generatedNavigationSupported"] = false;
                    if (!metadata.ContainsKey("dependentTable"))
                        metadata["dependentTable"] = feature.TableKey;
                    break;
                case "ReferentialAction":
                    metadata["navigationSuppressed"] = true;
                    metadata["generatedNavigationSupported"] = false;
                    metadata["reason"] = "referential-action-not-scaffoldable";
                    if (TryParseReferentialActionDetail(feature.Detail, out var onDelete, out var onUpdate))
                    {
                        metadata["onDelete"] = onDelete;
                        metadata["onUpdate"] = onUpdate;
                    }
                    break;
                case "PrecisionScale":
                    if (ScaffoldSqlMetadataParser.TryParseDecimalPrecision(feature.Detail, out var precision, out var scale))
                    {
                        metadata["precision"] = precision;
                        metadata["scale"] = scale;
                    }
                    break;
                case "Computed":
                    var (sql, stored) = ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(feature.Detail);
                    metadata["computedSql"] = sql;
                    metadata["stored"] = stored;
                    break;
                case "RowVersion":
                    if (!metadata.ContainsKey("table"))
                        metadata["table"] = feature.TableKey;
                    metadata["providerType"] = feature.Detail;
                    metadata["providerOwnedDdl"] = true;
                    metadata["generatedModelConfigurationSupported"] = true;
                    metadata["concurrencyToken"] = true;
                    metadata["databaseGenerated"] = true;
                    metadata["readOnlyEntity"] = false;
                    metadata["generatedWritesSupported"] = true;
                    metadata["reason"] = "provider-managed-rowversion";
                    break;
                case "IdentityStrategy":
                    metadata["identityStrategy"] = feature.Detail;
                    metadata["readOnlyEntity"] = true;
                    metadata["generatedWritesSupported"] = false;
                    metadata["reason"] = "provider-specific-identity-strategy";
                    if (ScaffoldSqlMetadataParser.TryParseIdentityOptions(feature.Detail, out var seed, out var increment))
                    {
                        metadata["seed"] = seed;
                        metadata["increment"] = increment;
                    }
                    break;
                case "ProviderSpecificColumnType":
                    metadata["providerType"] = feature.Detail;
                    var writeBlocking = IsWriteBlockingProviderSpecificColumnType(feature.Detail);
                    metadata["readOnlyEntity"] = writeBlocking;
                    metadata["generatedWritesSupported"] = !writeBlocking;
                    metadata["reason"] = writeBlocking ? "provider-specific-column-type" : "provider-specific-ddl";
                    break;
                case "Collation":
                    metadata["collation"] = feature.Detail;
                    break;
                case "Default":
                    metadata["defaultSql"] = feature.Detail;
                    metadata["readOnlyEntity"] = true;
                    metadata["generatedWritesSupported"] = false;
                    metadata["reason"] = "provider-specific-default";
                    break;
                case "CheckConstraint":
                    metadata["checkSql"] = ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(feature.Detail);
                    break;
                case "PartialIndex":
                    metadata["filtered"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "ExpressionIndex":
                    metadata["expressionBased"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    ScaffoldIndexFeatureMetadataBuilder.AddExpressionIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "IncludedColumnIndex":
                    metadata["includedColumns"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "DescendingIndex":
                    metadata["descending"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "PrefixIndex":
                    metadata["prefixIndex"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "ProviderSpecificIndex":
                    metadata["providerSpecific"] = true;
                    ScaffoldIndexFeatureMetadataBuilder.AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "Trigger":
                    AddTriggerFeatureMetadata(metadata, feature);
                    break;
                case "TemporalTable":
                    AddTemporalTableFeatureMetadata(metadata, feature);
                    metadata["readOnlyEntity"] = true;
                    metadata["generatedWritesSupported"] = false;
                    metadata["reason"] = "provider-native-temporal";
                    break;
            }

            return metadata;
        }

        public static bool TryParseMetadataBoolean(string value, out bool parsed)
        {
            var trimmed = value.Trim();
            if (trimmed.Equals("true", StringComparison.OrdinalIgnoreCase)
                || trimmed.Equals("1", StringComparison.Ordinal))
            {
                parsed = true;
                return true;
            }

            if (trimmed.Equals("false", StringComparison.OrdinalIgnoreCase)
                || trimmed.Equals("0", StringComparison.Ordinal))
            {
                parsed = false;
                return true;
            }

            parsed = false;
            return false;
        }

        private static bool IsWriteBlockingProviderSpecificColumnType(string? detail)
            => ScaffoldProviderSpecificTypeClassifier.IsWriteBlockingProviderSpecificColumnType(detail);
    }
}
