#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldCheckFeatureConfigurationBuilder
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
    }
}
