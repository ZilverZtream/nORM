#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCheckFeatureConfigurationBuilder
    {
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
