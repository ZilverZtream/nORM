#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static class ScaffoldPrimaryKeyConfigurationBuilder
    {
        public static IReadOnlyList<ScaffoldPrimaryKeyConfigurationInfo> BuildPrimaryKeyConfigurations(
            IReadOnlyDictionary<string, string> entityByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, string> primaryKeyConstraintNamesByTable,
            IReadOnlySet<string> skippedTableKeys)
        {
            var keys = new List<ScaffoldPrimaryKeyConfigurationInfo>();
            foreach (var (tableKey, keyColumns) in primaryKeyColumnsByTable.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                primaryKeyConstraintNamesByTable.TryGetValue(tableKey, out var constraintName);
                if ((keyColumns.Count <= 1 && string.IsNullOrWhiteSpace(constraintName))
                    || skippedTableKeys.Contains(tableKey)
                    || !entityByTable.TryGetValue(tableKey, out var entityName)
                    || !columnPropertiesByTable.TryGetValue(tableKey, out var propertyNames))
                {
                    continue;
                }

                var keyProperties = keyColumns
                    .Where(propertyNames.ContainsKey)
                    .Select(column => propertyNames[column])
                    .ToArray();
                if (keyProperties.Length == keyColumns.Count)
                {
                    keys.Add(new ScaffoldPrimaryKeyConfigurationInfo(
                        entityName,
                        keyProperties,
                        NullIfWhiteSpace(constraintName)));
                }
            }

            return keys;
        }

        private static string? NullIfWhiteSpace(string? value)
            => string.IsNullOrWhiteSpace(value) ? null : value;
    }

    internal readonly record struct ScaffoldPrimaryKeyConfigurationInfo(
        string EntityName,
        IReadOnlyList<string> PropertyNames,
        string? ConstraintName);
}
