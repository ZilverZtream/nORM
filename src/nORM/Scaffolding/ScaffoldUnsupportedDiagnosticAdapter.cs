#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

namespace nORM.Scaffolding
{
    internal static class ScaffoldUnsupportedDiagnosticAdapter
    {
        public static string NormalizeReferentialAction(string? action)
            => ScaffoldReferentialAction.Normalize(action);

        public static bool TryParseReferentialAction(string? action, out ReferentialAction referentialAction)
            => ScaffoldReferentialAction.TryParse(action, out referentialAction);

        public static void AddMissingPrimaryKeyDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldTable> tables,
            IReadOnlyDictionary<string, IReadOnlyList<string>> primaryKeyColumnsByTable,
            IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> columnPropertiesByTable)
        {
            foreach (var table in tables)
            {
                var tableKey = TableKey(table.Schema, table.Name);
                if (primaryKeyColumnsByTable.TryGetValue(tableKey, out var keys) && keys.Count > 0)
                    continue;

                columnPropertiesByTable.TryGetValue(tableKey, out var properties);
                var columnNames = properties?.Keys.ToArray() ?? Array.Empty<string>();
                var propertyNames = properties?.Values.ToArray() ?? Array.Empty<string>();
                features.Add(new DatabaseScaffolder.ScaffoldUnsupportedFeature(
                    tableKey,
                    "MissingPrimaryKey",
                    table.Name,
                    "Table has no primary key; generated entity is a query/bootstrap artifact until a key is configured.")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["table"] = tableKey,
                        ["columns"] = columnNames,
                        ["properties"] = propertyNames,
                        ["columnCount"] = columnNames.Length,
                        ["reason"] = "missing-primary-key"
                    }
                });
            }
        }

        public static void AddReferentialActionDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldForeignKey> foreignKeys)
        {
            foreach (var group in foreignKeys.GroupBy(
                fk => $"{fk.DependentSchema}\u001f{fk.DependentTable}\u001f{fk.ConstraintName}",
                StringComparer.OrdinalIgnoreCase))
            {
                var fk = group.First();
                var onDelete = NormalizeReferentialAction(fk.OnDelete);
                var onUpdate = NormalizeReferentialAction(fk.OnUpdate);
                if (TryParseReferentialAction(onDelete, out _)
                    && TryParseReferentialAction(onUpdate, out _))
                    continue;

                var rows = group.ToArray();
                var dependentKey = TableKey(fk.DependentSchema, fk.DependentTable);
                var principalKey = TableKey(fk.PrincipalSchema, fk.PrincipalTable);
                var dependentColumns = rows.Select(static row => row.DependentColumn).ToArray();
                var principalColumns = rows.Select(static row => row.PrincipalColumn).ToArray();
                features.Add(new DatabaseScaffolder.ScaffoldUnsupportedFeature(
                    dependentKey,
                    "ReferentialAction",
                    fk.ConstraintName,
                    $"ON DELETE {onDelete}; ON UPDATE {onUpdate}")
                {
                    Metadata = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["dependentTable"] = dependentKey,
                        ["dependentColumns"] = dependentColumns,
                        ["principalTable"] = principalKey,
                        ["principalColumns"] = principalColumns,
                        ["columnCount"] = rows.Length,
                        ["onDelete"] = onDelete,
                        ["onUpdate"] = onUpdate
                    }
                });
            }
        }

        public static void RemoveSupportedDescendingIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedDescending = indexes
                .Where(static index => index.IsDescending)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "DescendingIndex", StringComparison.OrdinalIgnoreCase)
                && supportedDescending.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        public static void RemoveSupportedIncludedColumnIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedIncluded = indexes
                .Where(static index => index.IsIncluded)
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "IncludedColumnIndex", StringComparison.OrdinalIgnoreCase)
                && supportedIncluded.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        public static void RemoveSupportedPartialIndexDiagnostics(
            List<DatabaseScaffolder.ScaffoldUnsupportedFeature> features,
            IReadOnlyList<DatabaseScaffolder.ScaffoldIndex> indexes)
        {
            var supportedPartial = indexes
                .Where(static index => !string.IsNullOrWhiteSpace(index.FilterSql))
                .Select(static index => index.TableKey + "\u001f" + index.IndexName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            features.RemoveAll(feature =>
                string.Equals(feature.Kind, "PartialIndex", StringComparison.OrdinalIgnoreCase)
                && supportedPartial.Contains(feature.TableKey + "\u001f" + feature.Name));
        }

        private static string TableKey(string? schema, string table)
            => ScaffoldForeignKeyShape.TableKey(schema, table);
    }
}
