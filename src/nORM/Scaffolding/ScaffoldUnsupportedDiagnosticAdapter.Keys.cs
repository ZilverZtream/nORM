#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldUnsupportedDiagnosticAdapter
    {
        public static void AddMissingPrimaryKeyDiagnostics(
            List<ScaffoldUnsupportedFeature> features,
            IReadOnlyList<ScaffoldTable> tables,
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
                features.Add(new ScaffoldUnsupportedFeature(
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
    }
}
