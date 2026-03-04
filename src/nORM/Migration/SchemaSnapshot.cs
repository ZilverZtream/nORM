using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace nORM.Migration
{
    /// <summary>
    /// Represents a snapshot of the database schema at a particular point in time.
    /// </summary>
    public class SchemaSnapshot
    {
        /// <summary>Tables captured in the snapshot.</summary>
        public List<TableSchema> Tables { get; set; } = new();
    }

    /// <summary>
    /// Describes the schema of a single table including its columns.
    /// </summary>
    public class TableSchema
    {
        /// <summary>Name of the table.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>Columns defined on the table.</summary>
        public List<ColumnSchema> Columns { get; set; } = new();
    }

    /// <summary>
    /// Describes a column within a table schema snapshot.
    /// </summary>
    public class ColumnSchema
    {
        /// <summary>Name of the column.</summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>Full CLR type name of the column.</summary>
        public string ClrType { get; set; } = string.Empty;
        /// <summary>Indicates whether the column allows <c>null</c> values.</summary>
        public bool IsNullable { get; set; }
        /// <summary>G1: True when the column is (part of) the table's primary key.</summary>
        public bool IsPrimaryKey { get; set; }
        /// <summary>G1: True when the column has a UNIQUE index.</summary>
        public bool IsUnique { get; set; }
        /// <summary>G1: Non-null means the column is covered by a named index.</summary>
        public string? IndexName { get; set; }
    }

    /// <summary>
    /// Helper responsible for creating <see cref="SchemaSnapshot"/> instances by
    /// scanning assemblies for entity types and their mapping attributes.
    /// </summary>
    public static class SchemaSnapshotBuilder
    {
        /// <summary>
        /// Builds a snapshot of the entity schema by inspecting the types in the provided assembly.
        /// </summary>
        /// <param name="assembly">The assembly containing the entity types to scan.</param>
        /// <returns>A snapshot describing the tables and columns inferred from the assembly.</returns>
        public static SchemaSnapshot Build(Assembly assembly)
        {
            var snapshot = new SchemaSnapshot();
            foreach (var type in assembly.GetTypes())
            {
                if (!type.IsClass || type.IsAbstract)
                    continue;

                var tableAttr = type.GetCustomAttribute<TableAttribute>();
                // If no Table attribute and no Key property, skip
                if (tableAttr == null && !type.GetProperties().Any(p => p.GetCustomAttribute<KeyAttribute>() != null))
                    continue;

                var table = new TableSchema
                {
                    Name = tableAttr?.Name ?? type.Name
                };

                // G1: collect PK property names for the type using [Key] or convention
                var pkNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (p.GetCustomAttribute<KeyAttribute>() != null)
                        pkNames.Add(p.Name);
                }
                // Convention fallback: if no [Key] found, treat "Id" or "{TypeName}Id" as PK
                if (pkNames.Count == 0)
                {
                    if (type.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance) != null)
                        pkNames.Add("Id");
                    var conventionName = type.Name + "Id";
                    if (type.GetProperty(conventionName, BindingFlags.Public | BindingFlags.Instance) != null)
                        pkNames.Add(conventionName);
                }

                foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (!prop.CanRead || !prop.CanWrite)
                        continue;
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;

                    var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
                    var clr = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    var isPk = pkNames.Contains(prop.Name);
                    var column = new ColumnSchema
                    {
                        Name = colAttr?.Name ?? prop.Name,
                        ClrType = clr.FullName ?? clr.Name,
                        IsNullable = !prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null,
                        // G1: populate PK / index metadata from attributes or convention
                        IsPrimaryKey = isPk,
                        IsUnique = isPk,   // PKs are implicitly unique; non-PK unique indexes require a future attribute
                        IndexName = isPk ? "PK_" + table.Name : null
                    };
                    table.Columns.Add(column);
                }

                snapshot.Tables.Add(table);
            }

            return snapshot;
        }
    }

    /// <summary>
    /// Represents the differences between two <see cref="SchemaSnapshot"/> instances.
    /// </summary>
    public class SchemaDiff
    {
        /// <summary>Tables that exist in the new snapshot but not in the old.</summary>
        public List<TableSchema> AddedTables { get; } = new();
        /// <summary>Columns that exist in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, ColumnSchema Column)> AddedColumns { get; } = new();
        /// <summary>Columns whose definition has changed between snapshots.</summary>
        public List<(TableSchema Table, ColumnSchema NewColumn, ColumnSchema OldColumn)> AlteredColumns { get; } = new();
        /// <summary>G1: Indexes that appear in the new snapshot but not in the old.</summary>
        public List<(TableSchema Table, string IndexName, bool IsUnique, string[] ColumnNames)> AddedIndexes { get; } = new();
        /// <summary>G1: Indexes that appear in the old snapshot but not in the new.</summary>
        public List<(TableSchema Table, string IndexName)> DroppedIndexes { get; } = new();

        /// <summary>Indicates whether the diff contains any schema changes.</summary>
        public bool HasChanges => AddedTables.Count > 0 || AddedColumns.Count > 0 || AlteredColumns.Count > 0
            || AddedIndexes.Count > 0 || DroppedIndexes.Count > 0;
    }

    /// <summary>
    /// Provides methods for computing differences between two schema snapshots.
    /// </summary>
    public static class SchemaDiffer
    {
        /// <summary>
        /// Computes the difference between two schema snapshots.
        /// </summary>
        /// <param name="oldSnapshot">The snapshot representing the current database schema.</param>
        /// <param name="newSnapshot">The snapshot representing the desired schema.</param>
        /// <returns>A <see cref="SchemaDiff"/> describing the operations required to transform the schema.</returns>
        public static SchemaDiff Diff(SchemaSnapshot oldSnapshot, SchemaSnapshot newSnapshot)
        {
            var diff = new SchemaDiff();
            foreach (var newTable in newSnapshot.Tables)
            {
                var oldTable = oldSnapshot.Tables.FirstOrDefault(t => string.Equals(t.Name, newTable.Name, StringComparison.OrdinalIgnoreCase));
                if (oldTable == null)
                {
                    diff.AddedTables.Add(newTable);
                    continue;
                }

                foreach (var col in newTable.Columns)
                {
                    var oldCol = oldTable.Columns.FirstOrDefault(c => string.Equals(c.Name, col.Name, StringComparison.OrdinalIgnoreCase));
                    if (oldCol == null)
                        diff.AddedColumns.Add((newTable, col));
                    else if (!string.Equals(oldCol.ClrType, col.ClrType, StringComparison.OrdinalIgnoreCase) || oldCol.IsNullable != col.IsNullable)
                        diff.AlteredColumns.Add((newTable, col, oldCol));
                }

                // G1: Detect index changes — compare named indexes between old and new
                var oldIndexes = BuildIndexMap(oldTable);
                var newIndexes = BuildIndexMap(newTable);

                foreach (var (name, (isUnique, cols)) in newIndexes)
                {
                    if (!oldIndexes.ContainsKey(name))
                        diff.AddedIndexes.Add((newTable, name, isUnique, cols));
                }

                foreach (var (name, _) in oldIndexes)
                {
                    if (!newIndexes.ContainsKey(name))
                        diff.DroppedIndexes.Add((newTable, name));
                }
            }
            return diff;
        }

        /// <summary>
        /// G1: Builds a map of index name → (IsUnique, column names[]) from the columns of a table.
        /// Only columns that carry an <see cref="ColumnSchema.IndexName"/> are included.
        /// </summary>
        private static Dictionary<string, (bool IsUnique, string[] ColumnNames)> BuildIndexMap(TableSchema table)
        {
            var map = new Dictionary<string, (bool, string[])>(StringComparer.OrdinalIgnoreCase);
            foreach (var col in table.Columns)
            {
                if (col.IndexName == null) continue;
                if (!map.TryGetValue(col.IndexName, out var entry))
                    entry = (col.IsUnique, Array.Empty<string>());
                entry = (entry.Item1 || col.IsUnique, entry.Item2.Append(col.Name).ToArray());
                map[col.IndexName] = entry;
            }
            return map;
        }
    }
}
