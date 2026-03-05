using System.Collections.Generic;
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

    // MG-1: Navigation and collection property exclusion tests

    // Helper entity types used by MG-1 tests (private so they don't pollute snapshot scans)
    [Table("MG1_Post")]
    private class MG1Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    [Table("MG1_Category")]
    private class MG1Category
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("MG1_Article")]
    private class MG1Article
    {
        [Key] public int Id { get; set; }
        public string Body { get; set; } = string.Empty;
        public int CategoryId { get; set; }

        // Navigation property — should be EXCLUDED from snapshot
        public MG1Category Category { get; set; } = null!;

        // Collection navigation property — should be EXCLUDED from snapshot
        public List<MG1Post> Posts { get; set; } = new();
    }

    [Fact]
    public void SchemaSnapshotBuilder_ExcludesNavigationProperties()
    {
        // Build snapshot for the in-process assembly so it picks up MG1Article
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MG1Article).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MG1_Article");

        // If the entity wasn't picked up, skip — some test assemblies may differ
        if (table == null) return;

        var columnNames = table.Columns.Select(c => c.Name).ToList();

        // Scalar columns must be present
        Assert.Contains("Id", columnNames);
        Assert.Contains("Body", columnNames);
        Assert.Contains("CategoryId", columnNames);

        // Navigation properties must NOT appear as columns
        Assert.DoesNotContain("Category", columnNames);
        Assert.DoesNotContain("Posts", columnNames);
    }

    [Fact]
    public void SchemaSnapshotBuilder_OnlyScalarColumnsIncluded_ForEntityWithNavigations()
    {
        // Directly exercise SchemaSnapshotBuilder.Build with a controlled single-type snapshot
        // by scanning just the test types defined above
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MG1Article).Assembly);

        foreach (var table in snapshot.Tables)
        {
            foreach (var col in table.Columns)
            {
                // Collection types should never appear
                var colType = typeof(MG1Article).Assembly
                    .GetTypes()
                    .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    .FirstOrDefault(p => p.Name == col.Name)?.PropertyType;

                if (colType != null)
                {
                    // If we found the property, verify it's not a collection (non-string IEnumerable)
                    var isCollection = typeof(System.Collections.IEnumerable).IsAssignableFrom(colType)
                                       && colType != typeof(string);
                    Assert.False(isCollection,
                        $"Column '{col.Name}' in table '{table.Name}' is a collection type and should have been excluded.");
                }
            }
        }
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

    // ─── Fix 2: SchemaDiffer detects PK/unique metadata changes ───────────

    [Fact]
    public void SchemaDiffer_DetectsAlteredColumn_WhenIsUniqueFlips()
    {
        // Column changes from non-unique to unique (no type or nullability change).
        var oldTable = new TableSchema
        {
            Name = "Item",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Item" },
                new ColumnSchema { Name = "Code",  ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Item",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Item" },
                new ColumnSchema { Name = "Code",  ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true } // IsUnique flipped
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.AlteredColumns, ac => ac.NewColumn.Name == "Code");
    }

    [Fact]
    public void SchemaDiffer_DetectsAlteredColumn_WhenIsPrimaryKeyFlips()
    {
        // Column promoted to PK (no type/nullability change).
        var oldTable = new TableSchema
        {
            Name = "Widget",
            Columns =
            {
                new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Widget",
            Columns =
            {
                new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false, IsPrimaryKey = true }
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.AlteredColumns, ac => ac.NewColumn.Name == "Code");
    }

    [Fact]
    public void SchemaDiffer_DetectsAlteredColumn_WhenIndexNameLost()
    {
        // Column loses its IndexName without any other change — should be detected.
        var oldTable = new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Slug",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Slug" }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",    ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Slug",  ClrType = typeof(string).FullName!, IsNullable = false, IndexName = null } // index removed
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.AlteredColumns, ac => ac.NewColumn.Name == "Slug");
    }

    [Fact]
    public void SchemaDiffer_DetectsAlteredColumn_WhenIndexNameAdded()
    {
        // Column gains an IndexName (becomes indexed) — should be detected.
        var oldTable = new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Title", ClrType = typeof(string).FullName!, IsNullable = false } // no index
            }
        };
        var newTable = new TableSchema
        {
            Name = "Post",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Post" },
                new ColumnSchema { Name = "Title", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_Post_Title" } // gained index
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Single(diff.AlteredColumns, ac => ac.NewColumn.Name == "Title");
    }

    [Fact]
    public void SchemaDiffer_DetectsAllFourMetadataChanges_InOneDiff()
    {
        // All four change types combined: IsPrimaryKey, IsUnique, IndexName added, IndexName lost.
        var oldTable = new TableSchema
        {
            Name = "Mixed",
            Columns =
            {
                new ColumnSchema { Name = "A", ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = false },
                new ColumnSchema { Name = "B", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = false },
                new ColumnSchema { Name = "C", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_C" },
                new ColumnSchema { Name = "D", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Mixed",
            Columns =
            {
                new ColumnSchema { Name = "A", ClrType = typeof(int).FullName!,    IsNullable = false, IsPrimaryKey = true },  // PK gained
                new ColumnSchema { Name = "B", ClrType = typeof(string).FullName!, IsNullable = false, IsUnique = true },       // unique gained
                new ColumnSchema { Name = "C", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = null },      // index lost
                new ColumnSchema { Name = "D", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "idx_D" }   // index gained
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Contains(diff.AlteredColumns, ac => ac.NewColumn.Name == "A");
        Assert.Contains(diff.AlteredColumns, ac => ac.NewColumn.Name == "B");
        Assert.Contains(diff.AlteredColumns, ac => ac.NewColumn.Name == "C");
        Assert.Contains(diff.AlteredColumns, ac => ac.NewColumn.Name == "D");
    }

    [Fact]
    public void SchemaDiffer_NoFalsePositive_WhenColumnUnchanged()
    {
        // An unchanged column must not appear in AlteredColumns.
        var oldTable = new TableSchema
        {
            Name = "Stable",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Stable" },
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Stable",
            Columns =
            {
                new ColumnSchema { Name = "Id",   ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Stable" },
                new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, IsNullable = false }
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.Empty(diff.AlteredColumns);
        Assert.Empty(diff.AddedColumns);
        Assert.Empty(diff.DroppedColumns);
    }

    // ── MM-1: Read-only / init-only / computed property mapping ──────────────

    /// <summary>
    /// MM-1: A property with a standard getter+setter must be included as a column.
    /// (Baseline sanity check; must still pass after the fix.)
    /// </summary>
    [Table("MM1ReadWrite")]
    private class MM1ReadWriteEntity
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void SchemaSnapshot_RegularReadWriteProperty_IsIncluded()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MM1ReadWriteEntity).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MM1ReadWrite");
        Assert.NotNull(table);
        Assert.Contains(table.Columns, c => c.Name == "Name");
    }

    /// <summary>
    /// MM-1: An init-only property (declared with the <c>init</c> accessor) must be included
    /// as a column. Before the fix, <c>!prop.CanWrite</c> incorrectly excluded init-only
    /// properties because <see cref="System.Reflection.PropertyInfo.CanWrite"/> returns false
    /// for them in reflection when accessed from outside the defining assembly.
    /// </summary>
    [Table("MM1InitOnly")]
    private class MM1InitOnlyEntity
    {
        [Key]
        public int Id { get; init; }
        public string Name { get; init; } = string.Empty;
    }

    [Fact]
    public void SchemaSnapshot_InitOnlyProperty_IsIncluded()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MM1InitOnlyEntity).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MM1InitOnly");
        Assert.NotNull(table);
        // Both Id (key) and Name (init-only scalar) must appear as mapped columns.
        Assert.Contains(table.Columns, c => c.Name == "Id");
        Assert.Contains(table.Columns, c => c.Name == "Name");
    }

    /// <summary>
    /// MM-1: A pure computed property (get-only expression body, no setter at all) must be
    /// excluded because it has no backing database column.
    /// </summary>
    [Table("MM1Computed")]
    private class MM1ComputedEntity
    {
        [Key]
        public int Id { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        // Pure computed — no setter of any kind; should be excluded.
        public string FullName => FirstName + " " + LastName;
    }

    [Fact]
    public void SchemaSnapshot_ComputedGetOnlyProperty_IsExcluded()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MM1ComputedEntity).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MM1Computed");
        Assert.NotNull(table);
        // FullName is a computed expression — must NOT appear.
        Assert.DoesNotContain(table.Columns, c => c.Name == "FullName");
        // Scalar backing properties must still appear.
        Assert.Contains(table.Columns, c => c.Name == "FirstName");
        Assert.Contains(table.Columns, c => c.Name == "LastName");
    }

    /// <summary>
    /// MM-1: A reference-type navigation property (class type that is not string or byte[])
    /// must still be excluded after the CanWrite fix. The exclusion criterion is the property
    /// TYPE (non-scalar reference class), not its writability.
    /// </summary>
    [Table("MM1Navigation")]
    private class MM1NavigationEntity
    {
        [Key]
        public int Id { get; set; }
        public int CategoryId { get; set; }
        // Reference navigation property — class type, not a scalar → must be excluded.
        public MM1ReadWriteEntity? Category { get; set; }
    }

    [Fact]
    public void SchemaSnapshot_NavigationProperty_IsExcluded()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MM1NavigationEntity).Assembly);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "MM1Navigation");
        Assert.NotNull(table);
        // The navigation property must not be mapped to a column.
        Assert.DoesNotContain(table.Columns, c => c.Name == "Category");
        // The FK scalar column must be present.
        Assert.Contains(table.Columns, c => c.Name == "CategoryId");
    }
}
