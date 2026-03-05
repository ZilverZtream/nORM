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
        /// <summary>SQL literal default value for ADD COLUMN NOT NULL migrations (e.g. "''" or "0").</summary>
        public string? DefaultValue { get; set; }
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
                    // MM-1: A property must be readable to be a column.
                    if (!prop.CanRead)
                        continue;
                    // MM-1: The old `!prop.CanWrite` check was wrong — it excluded init-only and
                    // read-only properties that legitimately map to DB columns. The correct
                    // exclusion criterion is the property's TYPE (navigation vs scalar), not
                    // its writability. The navigation-property filters below handle exclusion.
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;
                    // MG-1: Exclude collection/enumerable navigation properties (e.g. List<Post>, ICollection<T>)
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType)
                        && prop.PropertyType != typeof(string))
                        continue;
                    // MG-1: Exclude reference navigation properties (class types that are not mappable scalars)
                    if (!IsMappableType(prop.PropertyType))
                        continue;
                    // MM-1: Exclude computed properties with no backing column — these are get-only
                    // properties that have no setter AND no init accessor, meaning they are pure
                    // expressions (e.g., public string FullName => FirstName + " " + LastName).
                    // Properties with init-only setters DO have a SetMethod (IsInitOnly = true) and
                    // are legitimate columns, so we must NOT exclude them here.
                    if (!prop.CanWrite)
                    {
                        // Check if it's an init-only property (has a set accessor marked IsInitOnly)
                        var setter = prop.GetSetMethod(nonPublic: true);
                        bool isInitOnly = setter != null &&
                            setter.ReturnParameter.GetRequiredCustomModifiers()
                                .Contains(typeof(System.Runtime.CompilerServices.IsExternalInit));
                        // If no setter at all (not even init-only), it's a computed/expression property — exclude it
                        if (!isInitOnly && setter == null)
                            continue;
                    }

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

        /// <summary>
        /// MG-1: Returns true for types that map to database scalar columns.
        /// Reference navigation properties and collection properties return false.
        /// </summary>
        private static bool IsMappableType(Type t)
        {
            // Unwrap Nullable<T> — e.g. int?, Guid?
            var underlying = Nullable.GetUnderlyingType(t);
            if (underlying != null)
                return true; // Nullable value types are always mappable

            // Value types (int, bool, DateTime, Guid, enum, etc.) are always mappable
            if (t.IsValueType)
                return true;

            // string and byte[] are the only reference types that are scalar columns
            if (t == typeof(string) || t == typeof(byte[]))
                return true;

            // All other reference types (class, interface) are navigation properties
            return false;
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
        /// <summary>SD-8: Tables that exist in the old snapshot but not in the new (dropped tables).</summary>
        public List<TableSchema> DroppedTables { get; } = new();
        /// <summary>SD-8: Columns that exist in the old snapshot but not in the new for a given table (dropped columns).</summary>
        public List<(TableSchema Table, ColumnSchema Column)> DroppedColumns { get; } = new();

        /// <summary>Indicates whether the diff contains any schema changes.</summary>
        public bool HasChanges => AddedTables.Count > 0 || AddedColumns.Count > 0 || AlteredColumns.Count > 0
            || AddedIndexes.Count > 0 || DroppedIndexes.Count > 0
            || DroppedTables.Count > 0 || DroppedColumns.Count > 0;
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
                    else if (!string.Equals(oldCol.ClrType, col.ClrType, StringComparison.OrdinalIgnoreCase)
                        || oldCol.IsNullable != col.IsNullable
                        || oldCol.IsPrimaryKey != col.IsPrimaryKey
                        || oldCol.IsUnique != col.IsUnique
                        || !string.Equals(oldCol.IndexName, col.IndexName, StringComparison.OrdinalIgnoreCase))
                        diff.AlteredColumns.Add((newTable, col, oldCol));
                }

                // SD-8: Detect dropped columns — columns present in old but not in new
                foreach (var oldCol in oldTable.Columns)
                {
                    if (!newTable.Columns.Any(c => string.Equals(c.Name, oldCol.Name, StringComparison.OrdinalIgnoreCase)))
                        diff.DroppedColumns.Add((newTable, oldCol));
                }

                // G1: Detect index changes — compare named indexes between old and new
                var oldIndexes = BuildIndexMap(oldTable);
                var newIndexes = BuildIndexMap(newTable);

                foreach (var (name, (isUnique, cols)) in newIndexes)
                {
                    if (!oldIndexes.ContainsKey(name))
                    {
                        diff.AddedIndexes.Add((newTable, name, isUnique, cols));
                    }
                    else
                    {
                        // MIG-2: Index exists in both — check for definition changes
                        var (oldIsUnique, oldCols) = oldIndexes[name];
                        var colsChanged = !oldCols.OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                            .SequenceEqual(cols.OrderBy(x => x, StringComparer.OrdinalIgnoreCase),
                                StringComparer.OrdinalIgnoreCase);
                        if (oldIsUnique != isUnique || colsChanged)
                        {
                            diff.DroppedIndexes.Add((newTable, name));
                            diff.AddedIndexes.Add((newTable, name, isUnique, cols));
                        }
                    }
                }

                foreach (var (name, _) in oldIndexes)
                {
                    if (!newIndexes.ContainsKey(name))
                        diff.DroppedIndexes.Add((newTable, name));
                }
            }

            // SD-8: Detect dropped tables — tables present in old but not in new
            foreach (var oldTable in oldSnapshot.Tables)
            {
                if (!newSnapshot.Tables.Any(t => string.Equals(t.Name, oldTable.Name, StringComparison.OrdinalIgnoreCase)))
                    diff.DroppedTables.Add(oldTable);
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
                // Include columns that have an explicit IndexName, or are unique/PK (implicit constraint).
                // This ensures that changes to IsPrimaryKey or IsUnique flow through AddedIndexes/DroppedIndexes.
                string? indexKey = col.IndexName;
                if (indexKey == null)
                {
                    if (col.IsPrimaryKey)
                        indexKey = $"__PK__{col.Name}";
                    else if (col.IsUnique)
                        indexKey = $"__UQ__{col.Name}";
                    else
                        continue;
                }
                if (!map.TryGetValue(indexKey, out var entry))
                    entry = (col.IsUnique || col.IsPrimaryKey, Array.Empty<string>());
                entry = (entry.Item1 || col.IsUnique || col.IsPrimaryKey, entry.Item2.Append(col.Name).ToArray());
                map[indexKey] = entry;
            }
            return map;
        }
    }
}
