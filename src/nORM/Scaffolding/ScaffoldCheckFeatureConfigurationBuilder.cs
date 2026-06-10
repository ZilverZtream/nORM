#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace nORM.Scaffolding
{
    internal static class ScaffoldCheckFeatureConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new List<ScaffoldCheckConstraintConfigurationInfo>();
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "CheckConstraint", StringComparison.OrdinalIgnoreCase)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || string.IsNullOrWhiteSpace(feature.Detail)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName))
                {
                    continue;
                }

                var sql = ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(feature.Detail);
                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                var constraintName = HasSyntheticDatabaseName(feature)
                    ? BuildGeneratedCheckConstraintName(entityName, sql)
                    : feature.Name;
                result.Add(new ScaffoldCheckConstraintConfigurationInfo(
                    feature.TableKey,
                    entityName,
                    constraintName,
                    sql));
            }

            return result;
        }

        public static bool CheckConstraintConfigurationMatchesFeature(
            ScaffoldCheckConstraintConfigurationInfo check,
            ScaffoldUnsupportedFeatureInfo feature)
        {
            if (!string.Equals(check.TableKey, feature.TableKey, StringComparison.OrdinalIgnoreCase))
                return false;

            if (string.Equals(check.Name, feature.Name, StringComparison.OrdinalIgnoreCase))
                return true;

            return HasSyntheticDatabaseName(feature)
                   && string.Equals(check.Sql, ScaffoldSqlMetadataParser.NormalizeScaffoldCheckSql(feature.Detail), StringComparison.Ordinal);
        }

        public static string BuildGeneratedCheckConstraintName(string entityName, string sql)
        {
            var nameSegment = SanitizeConstraintNameSegment(entityName.TrimStart('@'));
            if (nameSegment.Length > 106)
                nameSegment = nameSegment[..106];

            return "CK_" + nameSegment + "_" + StableIdentifierHash(sql);
        }

        public static IReadOnlyList<ScaffoldCheckConstraintConfigurationInfo> BuildEnumCheckConstraintConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IEnumerable<ScaffoldFeatureInput> features)
        {
            var result = new List<ScaffoldCheckConstraintConfigurationInfo>();
            foreach (var input in features)
            {
                var feature = input.Feature;
                if (!string.Equals(feature.Kind, "ProviderSpecificColumnType", StringComparison.OrdinalIgnoreCase)
                    || !TryBuildProviderValueCheckSql(feature.Name, feature.Detail, out var checkKind, out var sql)
                    || string.IsNullOrWhiteSpace(feature.Name)
                    || !IsSimpleSqlIdentifier(feature.Name)
                    || !entityByTable.TryGetValue(feature.TableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(feature.TableKey, out var properties)
                    || !properties.TryGetValue(feature.Name, out var propertyName))
                {
                    continue;
                }

                var checkName = "CK_" + entityName + "_" + propertyName + "_" + checkKind;
                result.Add(new ScaffoldCheckConstraintConfigurationInfo(feature.TableKey, entityName, checkName, sql));
            }

            return result;
        }

        public static bool TryBuildProviderValueCheckSql(
            string columnName,
            string? detail,
            out string checkKind,
            out string sql)
        {
            checkKind = string.Empty;
            sql = string.Empty;

            if (ScaffoldProviderSpecificTypeClassifier.TryParseEnumValues(detail, out var enumValues))
            {
                checkKind = "Enum";
                sql = columnName + " IN (" + string.Join(", ", QuoteSqlStringLiterals(enumValues)) + ")";
                return true;
            }

            if (ScaffoldProviderSpecificTypeClassifier.TryParsePostgresDomainEnumValues(detail, out enumValues))
            {
                checkKind = "Enum";
                sql = columnName + " IN (" + string.Join(", ", QuoteSqlStringLiterals(enumValues)) + ")";
                return true;
            }

            if (ScaffoldProviderSpecificTypeClassifier.TryParseBoundedMySqlSetValues(detail, out var setValues))
            {
                checkKind = "Set";
                sql = columnName + " IN (" + string.Join(", ", QuoteSqlStringLiterals(BuildMySqlSetCombinations(setValues))) + ")";
                return true;
            }

            return false;
        }

        private static bool HasSyntheticDatabaseName(ScaffoldUnsupportedFeatureInfo feature)
            => feature.Metadata is not null
               && feature.Metadata.TryGetValue("isSyntheticName", out var value)
               && value is true;

        private static string SanitizeConstraintNameSegment(string value)
        {
            var sb = new StringBuilder(value.Length);
            foreach (var ch in value)
                sb.Append(char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_');

            return sb.Length == 0 ? "Table" : sb.ToString();
        }

        private static string StableIdentifierHash(string value)
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            var hash = offset;
            foreach (var b in Encoding.UTF8.GetBytes(value))
            {
                hash ^= b;
                hash *= prime;
            }

            return hash.ToString("X16", CultureInfo.InvariantCulture);
        }

        private static string[] BuildMySqlSetCombinations(IReadOnlyList<string> values)
        {
            var result = new List<string> { string.Empty };
            var combinationCount = 1 << values.Count;
            for (var mask = 1; mask < combinationCount; mask++)
            {
                var selected = new List<string>(values.Count);
                for (var i = 0; i < values.Count; i++)
                {
                    if ((mask & (1 << i)) != 0)
                        selected.Add(values[i]);
                }

                result.Add(string.Join(",", selected));
            }

            return result.ToArray();
        }

        private static IEnumerable<string> QuoteSqlStringLiterals(IEnumerable<string> values)
            => values.Select(static value => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'");

        private static bool IsSimpleSqlIdentifier(string value)
        {
            if (string.IsNullOrWhiteSpace(value) || !(char.IsLetter(value[0]) || value[0] == '_'))
                return false;

            for (var i = 1; i < value.Length; i++)
            {
                var ch = value[i];
                if (!char.IsLetterOrDigit(ch) && ch != '_')
                    return false;
            }

            return true;
        }
    }
}
