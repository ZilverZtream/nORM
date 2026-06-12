#nullable enable
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        private static bool TryAddScalarFeatureMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            switch (feature.Kind)
            {
                case "PrecisionScale":
                    AddPrecisionScaleMetadata(metadata, feature);
                    return true;
                case "Computed":
                    AddComputedMetadata(metadata, feature);
                    return true;
                case "RowVersion":
                    AddRowVersionMetadata(metadata, feature);
                    return true;
                case "IdentityStrategy":
                    AddIdentityStrategyMetadata(metadata, feature);
                    return true;
                case "ProviderSpecificColumnType":
                    AddProviderSpecificColumnTypeMetadata(metadata, feature);
                    return true;
                case "Collation":
                    metadata["collation"] = feature.Detail;
                    return true;
                case "Default":
                    AddDefaultMetadata(metadata, feature);
                    return true;
                case "CheckConstraint":
                    metadata["checkSql"] = ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(feature.Detail);
                    return true;
                default:
                    return false;
            }
        }

        private static void AddPrecisionScaleMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            if (!ScaffoldSqlMetadataParser.TryParseDecimalPrecision(feature.Detail, out var precision, out var scale))
                return;

            metadata["precision"] = precision;
            metadata["scale"] = scale;
        }

        private static void AddComputedMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            var (sql, stored) = ScaffoldSqlMetadataParser.NormalizeScaffoldComputedSql(feature.Detail);
            metadata["computedSql"] = sql;
            metadata["stored"] = stored;
        }

        private static void AddRowVersionMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
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
        }

        private static void AddIdentityStrategyMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["identityStrategy"] = feature.Detail;
            metadata["readOnlyEntity"] = true;
            metadata["generatedWritesSupported"] = false;
            metadata["reason"] = "provider-specific-identity-strategy";
            if (ScaffoldSqlMetadataParser.TryParseIdentityOptions(feature.Detail, out var seed, out var increment))
            {
                metadata["seed"] = seed;
                metadata["increment"] = increment;
            }
        }

        private static void AddProviderSpecificColumnTypeMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["providerType"] = feature.Detail;
            var writeBlocking = IsWriteBlockingProviderSpecificColumnType(feature.Detail);
            metadata["readOnlyEntity"] = writeBlocking;
            metadata["generatedWritesSupported"] = !writeBlocking;
            metadata["reason"] = writeBlocking ? "provider-specific-column-type" : "provider-specific-ddl";
        }

        private static void AddDefaultMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            metadata["defaultSql"] = feature.Detail;
            metadata["readOnlyEntity"] = true;
            metadata["generatedWritesSupported"] = false;
            metadata["reason"] = "provider-specific-default";
        }
    }
}
