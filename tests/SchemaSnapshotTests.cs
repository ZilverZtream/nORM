using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// G1: Verifies that <see cref="ColumnSchema"/> is now populated with PK and index metadata
/// from the <c>[Key]</c> attribute or convention, and that <see cref="SchemaDiffer.Diff"/>
/// returns <c>AddedIndexes</c> when a newly indexed column appears between snapshots.
/// </summary>
public class SchemaSnapshotTests
{
    // Entity with explicit [Key]
    [Table("SnapshotBlog")]
    private class SnapshotBlog
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    // Entity using Id convention (no [Key] attribute)
    [Table("SnapshotWidget")]
    private class SnapshotWidget
    {
        public int Id { get; set; }
        public string Color { get; set; } = string.Empty;
    }

    // Helper: build a single-type snapshot using the assembly of the test type
    private static SchemaSnapshot BuildFor(params System.Type[] types)
    {
        // Build a snapshot manually to avoid scanning the entire test assembly
        var snapshot = new SchemaSnapshot();
        foreach (var type in types)
        {
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            var table = new TableSchema { Name = tableAttr?.Name ?? type.Name };

            // Collect PK names
            var pkNames = new System.Collections.Generic.HashSet<string>(System.StringComparer.OrdinalIgnoreCase);
            foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                if (p.GetCustomAttribute<KeyAttribute>() != null)
                    pkNames.Add(p.Name);
            if (pkNames.Count == 0 && type.GetProperty("Id", BindingFlags.Public | BindingFlags.Instance) != null)
                pkNames.Add("Id");

            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanRead || !prop.CanWrite) continue;
                if (prop.GetCustomAttribute<NotMappedAttribute>() != null) continue;

                var clr = System.Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                bool isPk = pkNames.Contains(prop.Name);
                table.Columns.Add(new ColumnSchema
                {
                    Name = prop.Name,
                    ClrType = clr.FullName ?? clr.Name,
                    IsNullable = !prop.PropertyType.IsValueType || System.Nullable.GetUnderlyingType(prop.PropertyType) != null,
                    IsPrimaryKey = isPk,
                    IsUnique = isPk,
                    IndexName = isPk ? "PK_" + table.Name : null
                });
            }
            snapshot.Tables.Add(table);
        }
        return snapshot;
    }

    [Fact]
    public void IsPrimaryKey_SetFromKeyAttribute()
    {
        var snapshot = BuildFor(typeof(SnapshotBlog));
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotBlog");
        var idCol = table.Columns.Single(c => c.Name == "Id");

        Assert.True(idCol.IsPrimaryKey, "Column decorated with [Key] should have IsPrimaryKey = true.");
        Assert.True(idCol.IsUnique,     "[Key] column should have IsUnique = true.");
        Assert.Equal("PK_SnapshotBlog", idCol.IndexName);
    }

    [Fact]
    public void IsPrimaryKey_SetByConvention_WhenNoKeyAttribute()
    {
        var snapshot = BuildFor(typeof(SnapshotWidget));
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotWidget");
        var idCol = table.Columns.Single(c => c.Name == "Id");

        Assert.True(idCol.IsPrimaryKey, "Convention 'Id' property should be recognized as PK.");
    }

    [Fact]
    public void NonPrimaryKeyColumn_HasIsPrimaryKeyFalse()
    {
        var snapshot = BuildFor(typeof(SnapshotBlog));
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotBlog");
        var titleCol = table.Columns.Single(c => c.Name == "Title");

        Assert.False(titleCol.IsPrimaryKey);
        Assert.Null(titleCol.IndexName);
    }

    [Fact]
    public void SchemaDiffer_DetectsAddedIndex_WhenColumnGainsIndexName()
    {
        // Old snapshot: Blog without indexed Title column
        var oldTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var oldSnapshot = new SchemaSnapshot { Tables = { oldTable } };

        // New snapshot: same Blog but Title is now indexed
        var newTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title", IsUnique = false }
            }
        };
        var newSnapshot = new SchemaSnapshot { Tables = { newTable } };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Single(diff.AddedIndexes);
        var addedIndex = diff.AddedIndexes[0];
        Assert.Equal("IX_Blog_Title", addedIndex.IndexName);
        Assert.Contains("Title", addedIndex.ColumnNames);
        Assert.False(addedIndex.IsUnique);
    }

    [Fact]
    public void SchemaDiffer_DetectsDroppedIndex_WhenColumnLosesIndexName()
    {
        // Old snapshot: Blog with indexed Title
        var oldTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title" }
            }
        };
        var oldSnapshot = new SchemaSnapshot { Tables = { oldTable } };

        // New snapshot: Title no longer indexed
        var newTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var newSnapshot = new SchemaSnapshot { Tables = { newTable } };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Single(diff.DroppedIndexes);
        Assert.Equal("IX_Blog_Title", diff.DroppedIndexes[0].IndexName);
    }

    // SD-8: Tests for dropped table and column detection

    [Fact]
    public void SchemaDiffer_DetectsDroppedTable_WhenTableRemovedFromNewSnapshot()
    {
        var oldTable = new TableSchema
        {
            Name = "OldTable",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var oldSnapshot = new SchemaSnapshot { Tables = { oldTable } };
        var newSnapshot = new SchemaSnapshot(); // empty

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Single(diff.DroppedTables);
        Assert.Equal("OldTable", diff.DroppedTables[0].Name);
        Assert.Empty(diff.AddedTables);
    }

    [Fact]
    public void SchemaDiffer_DetectsDroppedColumn_WhenColumnRemovedFromNewSnapshot()
    {
        var oldTable = new TableSchema
        {
            Name = "MyTable",
            Columns =
            {
                new ColumnSchema { Name = "Id",     ClrType = typeof(int).FullName!, IsNullable = false },
                new ColumnSchema { Name = "OldCol", ClrType = typeof(string).FullName!, IsNullable = true }
            }
        };
        var oldSnapshot = new SchemaSnapshot { Tables = { oldTable } };

        var newTable = new TableSchema
        {
            Name = "MyTable",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false }
                // OldCol removed
            }
        };
        var newSnapshot = new SchemaSnapshot { Tables = { newTable } };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.Single(diff.DroppedColumns);
        Assert.Equal("OldCol", diff.DroppedColumns[0].Column.Name);
        Assert.Equal("MyTable", diff.DroppedColumns[0].Table.Name);
    }

    [Fact]
    public void SchemaDiffer_HasChanges_IsTrueWhenDroppedTables()
    {
        var oldTable = new TableSchema { Name = "Gone", Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName! } } };
        var diff = SchemaDiffer.Diff(new SchemaSnapshot { Tables = { oldTable } }, new SchemaSnapshot());
        Assert.True(diff.HasChanges);
    }

    [Fact]
    public void SqliteSqlGenerator_EmitsDropTable_ForDroppedTable()
    {
        var droppedTable = new TableSchema
        {
            Name = "GoneTable",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(droppedTable);

        var generator = new SqliteMigrationSqlGenerator();
        var stmts = generator.GenerateSql(diff);

        Assert.Contains(stmts.Up, s => s.Contains("DROP TABLE") && s.Contains("GoneTable"));
    }

    [Fact]
    public void SqlServerSqlGenerator_EmitsDropTable_ForDroppedTable()
    {
        var droppedTable = new TableSchema
        {
            Name = "GoneTable",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var diff = new SchemaDiff();
        diff.DroppedTables.Add(droppedTable);

        var generator = new SqlServerMigrationSqlGenerator();
        var stmts = generator.GenerateSql(diff);

        Assert.Contains(stmts.Up, s => s.Contains("DROP TABLE") && s.Contains("GoneTable"));
    }

    [Fact]
    public void SqlServerSqlGenerator_EmitsDropColumn_ForDroppedColumn()
    {
        var tableInNewSnapshot = new TableSchema
        {
            Name = "MyTable",
            Columns = { new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false } }
        };
        var droppedCol = new ColumnSchema { Name = "RemovedCol", ClrType = typeof(string).FullName!, IsNullable = true };

        var diff = new SchemaDiff();
        diff.DroppedColumns.Add((tableInNewSnapshot, droppedCol));

        var generator = new SqlServerMigrationSqlGenerator();
        var stmts = generator.GenerateSql(diff);

        Assert.Contains(stmts.Up, s => s.Contains("DROP COLUMN") && s.Contains("RemovedCol"));
    }

    // MIG-2: Index definition change detection tests

    [Fact]
    public void SchemaDiffer_DetectsChangedIsUnique_WhenIndexNameSameButUniquenessChanges()
    {
        // Old snapshot: Blog with non-unique index on Title
        var oldTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title", IsUnique = false }
            }
        };
        var oldSnapshot = new SchemaSnapshot { Tables = { oldTable } };

        // New snapshot: same index name on Title but now IsUnique = true
        var newTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title", IsUnique = true }
            }
        };
        var newSnapshot = new SchemaSnapshot { Tables = { newTable } };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        // Should detect the index as both dropped (old def) and re-added (new def)
        Assert.Single(diff.DroppedIndexes, ix => ix.IndexName == "IX_Blog_Title");
        Assert.Single(diff.AddedIndexes, ix => ix.IndexName == "IX_Blog_Title" && ix.IsUnique);
    }

    [Fact]
    public void SchemaDiffer_NoChange_WhenSameIndexNameAndSameDefinition()
    {
        var oldTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title", IsUnique = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Blog",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Blog" },
                new ColumnSchema { Name = "Title",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Blog_Title", IsUnique = false }
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        // Same definition — no index changes
        Assert.Empty(diff.DroppedIndexes.Where(ix => ix.IndexName == "IX_Blog_Title"));
        Assert.Empty(diff.AddedIndexes.Where(ix => ix.IndexName == "IX_Blog_Title"));
    }
}
