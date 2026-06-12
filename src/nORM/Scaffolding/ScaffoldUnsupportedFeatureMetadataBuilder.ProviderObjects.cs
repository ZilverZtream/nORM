#nullable enable
using System;
using System.Collections.Generic;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedFeatureMetadataBuilder
    {
        private static bool TryAddProviderObjectFeatureMetadata(
            IDictionary<string, object?> metadata,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            switch (feature.Kind)
            {
                case "Trigger":
                    AddTriggerFeatureMetadata(metadata, feature);
                    return true;
                case "TemporalTable":
                    AddTemporalTableFeatureMetadata(metadata, feature);
                    metadata["readOnlyEntity"] = true;
                    metadata["generatedWritesSupported"] = false;
                    metadata["reason"] = "provider-native-temporal";
                    return true;
                default:
                    return false;
            }
        }

        private static void AddTriggerFeatureMetadata(IDictionary<string, object?> metadata, ScaffoldUnsupportedFeatureInfo feature)
        {
            var values = ScaffoldSemicolonParser.Parse(feature.Detail, out var header);
            var provider = ParseSkippedObjectProvider(header);
            metadata["provider"] = IsKnownProviderName(provider) ? provider : ParseSkippedObjectProvider(feature.Detail);
            metadata["providerObjectKind"] = "Trigger";
            if (!metadata.ContainsKey("table"))
                metadata["table"] = feature.TableKey;
            if (!metadata.ContainsKey("triggerName"))
                metadata["triggerName"] = feature.Name;
            metadata["providerOwnedDdl"] = true;
            metadata["generatedModelConfigurationSupported"] = false;
            metadata["readOnlyEntity"] = true;
            metadata["generatedWritesSupported"] = false;
            metadata["reason"] = "provider-owned-trigger";
            AddMetadataValue(metadata, values, "timing");
            AddMetadataValue(metadata, values, "event");
            AddMetadataValue(metadata, values, "orientation");
            AddMetadataValue(metadata, values, "triggerSql");
            AddMetadataBooleanValue(metadata, values, "isDisabled");
            AddMetadataBooleanValue(metadata, values, "isInsteadOf");
            if (!metadata.ContainsKey("definitionAvailable"))
                metadata["definitionAvailable"] = metadata.ContainsKey("triggerSql");
        }

        private static void AddTemporalTableFeatureMetadata(IDictionary<string, object?> metadata, ScaffoldUnsupportedFeatureInfo feature)
        {
            var values = ScaffoldSemicolonParser.Parse(feature.Detail, out var header);
            var provider = ParseSkippedObjectProvider(header);
            metadata["provider"] = IsKnownProviderName(provider) ? provider : ParseSkippedObjectProvider(feature.Detail);
            metadata["providerObjectKind"] = "TemporalTable";
            if (!metadata.ContainsKey("table"))
                metadata["table"] = feature.TableKey;
            metadata["providerNativeTemporal"] = true;
            metadata["generatedTemporalConfigurationSupported"] = false;
            AddMetadataValue(metadata, values, "temporalType");
            AddMetadataValue(metadata, values, "historyTable");
            if (!metadata.ContainsKey("temporalType"))
            {
                metadata["temporalType"] = feature.Detail.Contains("history table", StringComparison.OrdinalIgnoreCase)
                    ? "history"
                    : "system-versioned";
            }
        }

        private static void AddMetadataBooleanValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (!values.TryGetValue(key, out var value)
                || !TryParseMetadataBoolean(value, out var parsed))
            {
                return;
            }

            metadata[key] = parsed;
        }

        private static void AddMetadataValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
                metadata[key] = value;
        }

        private static bool IsKnownProviderName(string value)
            => value.Equals("SQL Server", StringComparison.OrdinalIgnoreCase)
               || value.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("MySQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("SQLite", StringComparison.OrdinalIgnoreCase);

        private static string ParseSkippedObjectProvider(string detail)
            => ScaffoldSkippedObjectMetadataBuilder.ParseSkippedObjectProvider(detail);
    }
}
