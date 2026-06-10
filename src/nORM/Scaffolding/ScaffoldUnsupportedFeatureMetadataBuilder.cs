#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldUnsupportedFeatureMetadataBuilder
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
                    metadata["concurrencyToken"] = true;
                    metadata["databaseGenerated"] = true;
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
                    AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "ExpressionIndex":
                    metadata["expressionBased"] = true;
                    AddIndexFeatureMetadata(metadata, feature.Detail);
                    AddExpressionIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "IncludedColumnIndex":
                    metadata["includedColumns"] = true;
                    AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "DescendingIndex":
                    metadata["descending"] = true;
                    AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "PrefixIndex":
                    metadata["prefixIndex"] = true;
                    AddIndexFeatureMetadata(metadata, feature.Detail);
                    break;
                case "ProviderSpecificIndex":
                    metadata["providerSpecific"] = true;
                    AddIndexFeatureMetadata(metadata, feature.Detail);
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

        private static void AddIndexFeatureMetadata(IDictionary<string, object?> metadata, string detail)
        {
            if (string.IsNullOrWhiteSpace(detail))
                return;

            var values = ScaffoldSemicolonParser.Parse(detail, out var header);
            var provider = ParseSkippedObjectProvider(header);
            if (IsKnownProviderName(provider))
                metadata["provider"] = provider;

            AddMetadataValue(metadata, values, "indexType");
            AddMetadataValue(metadata, values, "accessMethod");
            AddMetadataValue(metadata, values, "filterSql");
            AddMetadataBooleanValue(metadata, values, "hasNullsNotDistinct");
            AddMetadataBooleanValue(metadata, values, "hasNonDefaultOperatorClass");
            AddMetadataBooleanValue(metadata, values, "hasIndexCollation");
            AddMetadataBooleanValue(metadata, values, "hasNonDefaultNullOrdering");

            if (values.TryGetValue("prefixColumns", out var prefixColumns)
                && !string.IsNullOrWhiteSpace(prefixColumns))
            {
                metadata["prefixColumns"] = ParsePrefixIndexColumns(prefixColumns);
            }

            var indexSql = values.TryGetValue("indexSql", out var explicitIndexSql)
                ? NullIfWhiteSpace(explicitIndexSql)
                : ExtractCreateIndexStatement(detail);
            if (string.IsNullOrWhiteSpace(indexSql))
                return;

            metadata["indexSql"] = indexSql;
            metadata["isUnique"] = ScaffoldSqlMetadataParser.IsCreateIndexUnique(indexSql);
            var keySql = ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(indexSql);
            if (!string.IsNullOrWhiteSpace(keySql))
                metadata["keySql"] = keySql;
            var filterSql = ScaffoldSqlMetadataParser.ExtractCreateIndexWhereClause(indexSql);
            if (!string.IsNullOrWhiteSpace(filterSql))
                metadata["filterSql"] = filterSql;
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

        private static void AddExpressionIndexFeatureMetadata(IDictionary<string, object?> metadata, string detail)
        {
            var values = ScaffoldSemicolonParser.Parse(detail, out _);
            if (values.TryGetValue("expression", out var expression)
                && !string.IsNullOrWhiteSpace(expression))
            {
                metadata["expressionSql"] = expression.Trim();
            }

            var indexSql = ExtractCreateIndexStatement(detail);
            var expressionSql = ScaffoldSqlMetadataParser.ExtractCreateIndexExpressionSql(indexSql ?? detail);
            if (!string.IsNullOrWhiteSpace(expressionSql)
                && !expressionSql.EndsWith("expression index", StringComparison.OrdinalIgnoreCase))
            {
                metadata["expressionSql"] = expressionSql;
            }
        }

        private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ParsePrefixIndexColumns(string prefixColumns)
        {
            var result = new List<IReadOnlyDictionary<string, object?>>();
            foreach (var rawPart in ScaffoldSqliteDdlParser.SplitTopLevelCommaSeparated(prefixColumns))
            {
                var part = rawPart.Trim();
                if (part.Length == 0)
                    continue;

                var separator = part.LastIndexOf(':');
                if (separator <= 0)
                    continue;

                var lengths = part[(separator + 1)..];
                var slash = lengths.IndexOf('/');
                var prefixLengthText = slash >= 0 ? lengths[..slash] : lengths;
                var declaredLengthText = slash >= 0 ? lengths[(slash + 1)..] : string.Empty;
                var column = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["name"] = part[..separator].Trim()
                };

                var prefixLength = ParseNullableInt(prefixLengthText);
                if (prefixLength.HasValue)
                    column["prefixLength"] = prefixLength.Value;

                var declaredLength = ParseNullableInt(declaredLengthText);
                if (declaredLength.HasValue)
                    column["declaredLength"] = declaredLength.Value;

                result.Add(column);
            }

            return result;
        }

        private static string? ExtractCreateIndexStatement(string detail)
        {
            var createIndex = ScaffoldSqlMetadataParser.FindSqlKeywordOutsideQuotes(detail, "CREATE", 0);
            if (createIndex < 0)
                return null;

            var candidate = detail[createIndex..].Trim();
            var index = 0;
            return ScaffoldSqlMetadataParser.TryConsumeSqlKeyword(candidate, ref index, "CREATE")
                ? candidate
                : null;
        }

        private static bool IsKnownProviderName(string value)
            => value.Equals("SQL Server", StringComparison.OrdinalIgnoreCase)
               || value.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("MySQL", StringComparison.OrdinalIgnoreCase)
               || value.Equals("SQLite", StringComparison.OrdinalIgnoreCase);

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

        private static void AddMetadataValue(
            IDictionary<string, object?> metadata,
            IReadOnlyDictionary<string, string> values,
            string key)
        {
            if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
                metadata[key] = value;
        }

        private static string ParseSkippedObjectProvider(string detail)
            => ScaffoldSkippedObjectMetadataBuilder.ParseSkippedObjectProvider(detail);

        private static int? ParseNullableInt(string? value)
            => int.TryParse(value, out var parsed) ? parsed : null;

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
