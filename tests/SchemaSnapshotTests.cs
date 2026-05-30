using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// G1: Verifies that <see cref="ColumnSchema"/> is now populated with PK and index metadata
/// from the <c>[Key]</c> attribute or convention, and that <see cref="SchemaDiffer.Diff"/>
/// returns <c>AddedIndexes</c> when a newly indexed column appears between snapshots.
/// </summary>
[Xunit.Trait("Category", "Fast")]
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

    [Table("SnapshotIndexedEntity")]
    private class SnapshotIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("IX_SnapshotIndexedEntity_Code", IsUnique = true)]
        public string Code { get; set; } = string.Empty;
    }

    [Table("SnapshotDescendingIndexedEntity")]
    private class SnapshotDescendingIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("IX_SnapshotDescendingIndexedEntity_Code", IsDescending = true)]
        public string Code { get; set; } = string.Empty;
    }

    [Table("SnapshotIncludedIndexedEntity")]
    private class SnapshotIncludedIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("IX_SnapshotIncludedIndexedEntity_Code")]
        public string Code { get; set; } = string.Empty;
        [Index("IX_SnapshotIncludedIndexedEntity_Code", IsIncluded = true)]
        public string DisplayName { get; set; } = string.Empty;
    }

    [Table("SnapshotCompositeIndexedEntity")]
    private class SnapshotCompositeIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("IX_SnapshotComposite_Tenant_Code", IsUnique = true, Order = 1)]
        public string Code { get; set; } = string.Empty;
        [Index("IX_SnapshotComposite_Tenant", Order = 0)]
        [Index("IX_SnapshotComposite_Tenant_Code", IsUnique = true, Order = 0)]
        public int TenantId { get; set; }
    }

    [Table("SnapshotSchemaEntity", Schema = "tenant")]
    private class SnapshotSchemaEntity
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("SnapshotPrecisionEntity")]
    private class SnapshotPrecisionEntity
    {
        [Key] public int Id { get; set; }
        [Column(TypeName = "decimal(28,6)")]
        public decimal Amount { get; set; }
    }

    [Table("SnapshotDefaultEntity")]
    private class SnapshotDefaultEntity
    {
        [Key] public int Id { get; set; }
        public string Status { get; set; } = string.Empty;
    }

    [Table("SnapshotFkParent")]
    private class SnapshotFkParent
    {
        [Key] public int Id { get; set; }
        public List<SnapshotFkChild> Children { get; set; } = new();
    }

    [Table("SnapshotFkChild")]
    private class SnapshotFkChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

 // Helper: build a single-type snapshot using the assembly of the test type
    private static SchemaSnapshot BuildFor(params System.Type[] types)
    {
 // Build a snapshot manually to avoid scanning the entire test assembly
        var snapshot = new SchemaSnapshot();
        foreach (var type in types)
        {
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            var tableName = tableAttr is null
                ? type.Name
                : string.IsNullOrWhiteSpace(tableAttr.Schema)
                    ? tableAttr.Name
                    : tableAttr.Schema + "." + tableAttr.Name;
            var table = new TableSchema { Name = tableName };

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
    public void BuildFromContext_IncludesFluentDefaultValueSql()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotDefaultEntity>()
                    .Property(e => e.Status)
                    .HasDefaultValueSql("'new'")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDefaultEntity");
        var status = table.Columns.Single(c => c.Name == "Status");
        Assert.Equal("'new'", status.DefaultValue);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentCheckConstraints()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotDefaultEntity>()
                    .HasCheckConstraint("CK_SnapshotDefaultEntity_Status", "length(Status) > 0")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDefaultEntity");
        var check = Assert.Single(table.CheckConstraints);
        Assert.Equal("CK_SnapshotDefaultEntity_Status", check.ConstraintName);
        Assert.Equal("length(Status) > 0", check.Sql);
    }

    [Fact]
    public void BuildFromContext_IncludesExplicitReferentialActions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotFkParent>()
                    .HasMany(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id, ReferentialAction.SetNull, ReferentialAction.Restrict)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var child = snapshot.Tables.Single(t => t.Name == "SnapshotFkChild");
        var fk = Assert.Single(child.ForeignKeys);
        Assert.Equal("SET NULL", fk.OnDelete);
        Assert.Equal("RESTRICT", fk.OnUpdate);
    }

    [Fact]
    public void SchemaSnapshotBuilder_ReadsIndexAttribute()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotIndexedEntity");
        var code = table.Columns.Single(c => c.Name == nameof(SnapshotIndexedEntity.Code));

        Assert.Equal("IX_SnapshotIndexedEntity_Code", code.IndexName);
        Assert.True(code.IsUnique);
    }

    [Fact]
    public void SchemaSnapshotBuilder_ReadsDescendingIndexAttribute()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotDescendingIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDescendingIndexedEntity");
        var code = table.Columns.Single(c => c.Name == "Code");
        var index = Assert.Single(code.Indexes);
        Assert.True(index.IsDescending);
    }

    [Fact]
    public void SchemaSnapshotBuilder_ReadsIncludedIndexAttribute()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotIncludedIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotIncludedIndexedEntity");
        var code = table.Columns.Single(c => c.Name == "Code");
        var displayName = table.Columns.Single(c => c.Name == "DisplayName");

        Assert.False(Assert.Single(code.Indexes).IsIncluded);
        Assert.True(Assert.Single(displayName.Indexes).IsIncluded);
    }

    [Fact]
    public void SchemaDiffer_UsesIndexAttributeOrderForCompositeIndexes()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotCompositeIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotCompositeIndexedEntity");
        var tenant = table.Columns.Single(c => c.Name == "TenantId");
        Assert.False(tenant.IsUnique);
        Assert.Equal(2, tenant.Indexes.Count);
        Assert.Contains(tenant.Indexes, i => i.Name == "IX_SnapshotComposite_Tenant");
        Assert.Contains(tenant.Indexes, i => i.Name == "IX_SnapshotComposite_Tenant_Code" && i.IsUnique && i.Order == 0);
        var oldSnapshot = new SchemaSnapshot
        {
            Tables =
            {
                new TableSchema
                {
                    Name = table.Name,
                    Columns =
                    {
                        new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_" + table.Name },
                        new ColumnSchema { Name = "TenantId", ClrType = typeof(int).FullName!, IsNullable = false },
                        new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false }
                    }
                }
            }
        };

        var diff = SchemaDiffer.Diff(oldSnapshot, snapshot);
        var added = Assert.Single(diff.AddedIndexes, x => x.IndexName == "IX_SnapshotComposite_Tenant_Code");
        Assert.True(added.IsUnique);
        Assert.Equal(new[] { "TenantId", "Code" }, added.ColumnNames);
    }

    [Fact]
    public void TableAttributeSchema_IncludedInSnapshotTableName()
    {
        var snapshot = BuildFor(typeof(SnapshotSchemaEntity));
        var table = Assert.Single(snapshot.Tables);

        Assert.Equal("tenant.SnapshotSchemaEntity", table.Name);
        Assert.Contains(table.Columns, c => c.Name == "Id" && c.IndexName == "PK_tenant.SnapshotSchemaEntity");
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

 // Tests for dropped table and column detection

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
    public void SchemaDiff_destructive_warnings_identify_column_rename_candidate()
    {
        var oldTable = new TableSchema
        {
            Name = "Orders",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "TotalCost", ClrType = typeof(decimal).FullName!, IsNullable = false }
            }
        };
        var newTable = new TableSchema
        {
            Name = "Orders",
            Columns =
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                new ColumnSchema { Name = "TotalAmount", ClrType = typeof(decimal).FullName!, IsNullable = false }
            }
        };

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { oldTable } },
            new SchemaSnapshot { Tables = { newTable } });

        Assert.True(diff.HasDestructiveChanges);
        var warning = Assert.Single(diff.GetDestructiveChangeWarnings());
        Assert.Contains("Orders.TotalCost", warning);
        Assert.Contains("Possible rename candidate", warning);
        Assert.Contains("Orders.TotalAmount", warning);
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

 // Index definition change detection tests

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

 // Navigation and collection property exclusion tests

 // Helper entity types used by tests (private so they don't pollute snapshot scans)
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
        var snapshot = SchemaSnapshotBuilder.Build(typeof(MG1Article).Assembly);

        // Resolve each TableSchema to its source entity type via [Table]
        // attribute or class name, then look up the property on THAT type.
        // The prior global FirstOrDefault scan could pick up an unrelated
        // entity's same-named property (e.g. two test entities both declare
        // `[Table("WsccItem")]` -- one with scalar `Tags`, one with collection
        // `Tags` -- and the scan matched the collection one first, failing
        // the assertion for the scalar table's column).
        var allEntityTypes = typeof(MG1Article).Assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract)
            .ToList();

        foreach (var table in snapshot.Tables)
        {
            var entityType = allEntityTypes.FirstOrDefault(t =>
                (t.GetCustomAttribute<TableAttribute>()?.Name ?? t.Name) == table.Name);
            if (entityType == null) continue;

            foreach (var col in table.Columns)
            {
                var prop = entityType.GetProperty(col.Name, BindingFlags.Public | BindingFlags.Instance);
                if (prop == null) continue;
                var colType = prop.PropertyType;
                var isCollection = typeof(System.Collections.IEnumerable).IsAssignableFrom(colType)
                                   && colType != typeof(string);
                Assert.False(isCollection,
                    $"Column '{col.Name}' in table '{table.Name}' (entity {entityType.FullName}) is a collection type and should have been excluded.");
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

    [Fact]
    public void SchemaSnapshotBuilder_ReadsDecimalPrecisionFromColumnTypeName()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotPrecisionEntity).Assembly);
        var table = Assert.Single(snapshot.Tables.Where(t => t.Name == "SnapshotPrecisionEntity"));
        var amount = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotPrecisionEntity.Amount)));

        Assert.Equal(28, amount.Precision);
        Assert.Equal(6, amount.Scale);
    }

    [Fact]
    public void SchemaDiffer_DetectsDecimalPrecisionChange()
    {
        var oldSnapshot = new SchemaSnapshot
        {
            Tables =
            {
                new TableSchema
                {
                    Name = "Invoice",
                    Columns =
                    {
                        new ColumnSchema { Name = "Amount", ClrType = typeof(decimal).FullName!, Precision = 18, Scale = 2, IsNullable = false }
                    }
                }
            }
        };
        var newSnapshot = new SchemaSnapshot
        {
            Tables =
            {
                new TableSchema
                {
                    Name = "Invoice",
                    Columns =
                    {
                        new ColumnSchema { Name = "Amount", ClrType = typeof(decimal).FullName!, Precision = 28, Scale = 6, IsNullable = false }
                    }
                }
            }
        };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);
        var altered = Assert.Single(diff.AlteredColumns);
        Assert.Equal("Amount", altered.NewColumn.Name);
    }

 // ── Read-only / init-only / computed property mapping ──────────────

 /// <summary>
 /// A property with a standard getter+setter must be included as a column.
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
 /// An init-only property (declared with the <c>init</c> accessor) must be included
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
 /// A pure computed property (get-only expression body, no setter at all) must be
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
 /// A reference-type navigation property (class type that is not string or byte[])
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
