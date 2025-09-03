using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace nORM.Migration
{
    public class SchemaSnapshot
    {
        public List<TableSchema> Tables { get; set; } = new();
    }

    public class TableSchema
    {
        public string Name { get; set; } = string.Empty;
        public List<ColumnSchema> Columns { get; set; } = new();
    }

    public class ColumnSchema
    {
        public string Name { get; set; } = string.Empty;
        public string ClrType { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
    }

    public static class SchemaSnapshotBuilder
    {
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

                foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (!prop.CanRead || !prop.CanWrite)
                        continue;
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;

                    var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
                    var clr = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    var column = new ColumnSchema
                    {
                        Name = colAttr?.Name ?? prop.Name,
                        ClrType = clr.FullName ?? clr.Name,
                        IsNullable = !prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null
                    };
                    table.Columns.Add(column);
                }

                snapshot.Tables.Add(table);
            }

            return snapshot;
        }
    }

    public class SchemaDiff
    {
        public List<TableSchema> AddedTables { get; } = new();
        public List<(TableSchema Table, ColumnSchema Column)> AddedColumns { get; } = new();
        public List<(TableSchema Table, ColumnSchema NewColumn, ColumnSchema OldColumn)> AlteredColumns { get; } = new();

        public bool HasChanges => AddedTables.Count > 0 || AddedColumns.Count > 0 || AlteredColumns.Count > 0;
    }

    public static class SchemaDiffer
    {
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
            }
            return diff;
        }
    }
}
