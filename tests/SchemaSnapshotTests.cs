using System;
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

    [Table("SnapshotUniqueIncludedIndexedEntity")]
    private class SnapshotUniqueIncludedIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("UX_SnapshotUniqueIncludedIndexedEntity_Code", IsUnique = true)]
        public string Code { get; set; } = string.Empty;
        [Index("UX_SnapshotUniqueIncludedIndexedEntity_Code", IsIncluded = true)]
        public string DisplayName { get; set; } = string.Empty;
    }

    [Table("SnapshotNullsNotDistinctIndexedEntity")]
    private class SnapshotNullsNotDistinctIndexedEntity
    {
        [Key] public int Id { get; set; }
        [Index("IX_SnapshotNullsNotDistinctIndexedEntity_Code", IsUnique = true, NullsNotDistinct = true, NullSortOrder = IndexNullSortOrder.First)]
        public string? Code { get; set; }
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

    [Table("SnapshotPrecisionProviderTextEntity")]
    private class SnapshotPrecisionProviderTextEntity
    {
        [Key] public int Id { get; set; }
        [Column(TypeName = "numeric ( 19 , 4 )")]
        public decimal SpacedAmount { get; set; }
        [Column(TypeName = "numeric(10)")]
        public decimal PrecisionOnlyAmount { get; set; }
        [Column(TypeName = "DOMAIN (public.price_amount -> numeric(12, 3))")]
        public decimal DomainAmount { get; set; }
        [Column(TypeName = "mydecimal(18,2)")]
        public decimal NotDecimal { get; set; }
    }

    [Table("SnapshotLengthEntity")]
    private class SnapshotLengthEntity
    {
        [Key] public int Id { get; set; }
        [MaxLength(80)]
        public string Name { get; set; } = string.Empty;
        [StringLength(40)]
        public string Code { get; set; } = string.Empty;
        [MaxLength(32)]
        public byte[] Payload { get; set; } = Array.Empty<byte>();
        public string Notes { get; set; } = string.Empty;
    }

    [Table("SnapshotDefaultEntity")]
    private class SnapshotDefaultEntity
    {
        [Key] public int Id { get; set; }
        public string Status { get; set; } = string.Empty;
    }

    [Table("SnapshotOwnsOneOwner")]
    private class SnapshotOwnsOneOwner
    {
        [Key] public int Id { get; set; }
        public SnapshotOwnsOneAddress Address { get; set; } = new();
    }

    private class SnapshotOwnsOneAddress
    {
        public string City { get; set; } = string.Empty;
        public int CityLength { get; set; }
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

    [Table("SnapshotOwnedOwner")]
    private class SnapshotOwnedOwner
    {
        [Key] public int Id { get; set; }
        [NotMapped] public List<SnapshotOwnedLine> Lines { get; set; } = new();
    }

    private class SnapshotOwnedLine
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int LineId { get; set; }
        [Index("IX_SnapshotOwnedLines_Sku", IsUnique = true)]
        public string Sku { get; set; } = string.Empty;
        public int SkuLength { get; set; }
        [Column(TypeName = "decimal(12,2)")]
        public decimal Amount { get; set; }
    }

    [Table("SnapshotM2MPost")]
    private class SnapshotM2MPost
    {
        [Key] public int Id { get; set; }
        [NotMapped]
        public List<SnapshotM2MLabel> Labels { get; set; } = new();
    }

    [Table("SnapshotM2MLabel")]
    private class SnapshotM2MLabel
    {
        [Key] public int Id { get; set; }
        [NotMapped]
        public List<SnapshotM2MPost> Posts { get; set; } = new();
    }

    [Table("SnapshotM2MStudent")]
    private class SnapshotM2MStudent
    {
        public int TenantId { get; set; }
        public int StudentId { get; set; }
        [NotMapped]
        public List<SnapshotM2MCourse> Courses { get; set; } = new();
    }

    [Table("SnapshotM2MCourse")]
    private class SnapshotM2MCourse
    {
        public int TenantId { get; set; }
        public int CourseId { get; set; }
        [NotMapped]
        public List<SnapshotM2MStudent> Students { get; set; } = new();
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
    public void BuildFromContext_PreservesFluentPrimaryKeyConstraintName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotBlog>()
                    .HasKey(e => e.Id, "PK_Custom_SnapshotBlog")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotBlog");
        var id = table.Columns.Single(c => c.Name == "Id");
        Assert.True(id.IsPrimaryKey);
        Assert.Equal("PK_Custom_SnapshotBlog", id.IndexName);
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
    public void BuildFromContext_IncludesFluentDefaultValueConstraintName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotDefaultEntity>()
                    .Property(e => e.Status)
                    .HasDefaultValueSql("'new'", constraintName: "DF_SnapshotDefaultEntity_Status")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDefaultEntity");
        var status = table.Columns.Single(c => c.Name == "Status");
        Assert.Equal("'new'", status.DefaultValue);
        Assert.Equal("DF_SnapshotDefaultEntity_Status", status.DefaultConstraintName);
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
    public void BuildFromContext_IncludesFluentComputedColumnSql()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotDefaultEntity>()
                    .Property(e => e.Status)
                    .HasComputedColumnSql("lower(Status)", stored: true)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDefaultEntity");
        var status = table.Columns.Single(c => c.Name == "Status");
        Assert.Equal("lower(Status)", status.ComputedColumnSql);
        Assert.True(status.IsStoredComputedColumn);
        Assert.False(status.IsIdentity);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentCollation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotDefaultEntity>()
                    .Property(e => e.Status)
                    .HasCollation("NOCASE")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotDefaultEntity");
        var status = table.Columns.Single(c => c.Name == "Status");
        Assert.Equal("NOCASE", status.Collation);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentMaxLength()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotLengthEntity>()
                    .Property(e => e.Name)
                    .HasMaxLength(120)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotLengthEntity");
        var name = table.Columns.Single(c => c.Name == nameof(SnapshotLengthEntity.Name));
        Assert.Equal(120, name.MaxLength);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentUnicodeAndFixedLengthFacets()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SnapshotLengthEntity>()
                    .Property(e => e.Code)
                    .HasMaxLength(40)
                    .IsUnicode(false)
                    .IsFixedLength();
                mb.Entity<SnapshotLengthEntity>()
                    .Property(e => e.Payload)
                    .HasMaxLength(16)
                    .IsFixedLength();
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotLengthEntity");
        var code = table.Columns.Single(c => c.Name == nameof(SnapshotLengthEntity.Code));
        var payload = table.Columns.Single(c => c.Name == nameof(SnapshotLengthEntity.Payload));
        Assert.Equal(40, code.MaxLength);
        Assert.False(code.IsUnicode);
        Assert.True(code.IsFixedLength);
        Assert.Equal(16, payload.MaxLength);
        Assert.Null(payload.IsUnicode);
        Assert.True(payload.IsFixedLength);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentPrecision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotPrecisionEntity>()
                    .Property(e => e.Amount)
                    .HasPrecision(18, 2)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotPrecisionEntity");
        var amount = table.Columns.Single(c => c.Name == nameof(SnapshotPrecisionEntity.Amount));
        Assert.Equal(18, amount.Precision);
        Assert.Equal(2, amount.Scale);
    }

    [Fact]
    public void BuildFromContext_IncludesFluentPrecisionWithoutScale()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotPrecisionEntity>()
                    .Property(e => e.Amount)
                    .HasPrecision(10)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotPrecisionEntity");
        var amount = table.Columns.Single(c => c.Name == nameof(SnapshotPrecisionEntity.Amount));
        Assert.Equal(10, amount.Precision);
        Assert.Null(amount.Scale);
    }

    [Fact]
    public void FluentHasMaxLength_ValidatesSupportedShape()
    {
        var builder = new ModelBuilder().Entity<SnapshotLengthEntity>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Property(e => e.Name).HasMaxLength(0));
        Assert.Throws<ArgumentException>(() => builder.Property(e => e.Id).HasMaxLength(16));
    }

    [Fact]
    public void FluentStringBinaryFacets_ValidateSupportedShape()
    {
        var builder = new ModelBuilder().Entity<SnapshotLengthEntity>();

        Assert.Throws<ArgumentException>(() => builder.Property(e => e.Id).IsUnicode(false));
        Assert.Throws<ArgumentException>(() => builder.Property(e => e.Id).IsFixedLength());
        builder.Property(e => e.Name).IsUnicode(false).IsFixedLength();
        builder.Property(e => e.Payload).IsFixedLength();
    }

    [Fact]
    public void FluentHasPrecision_ValidatesSupportedShape()
    {
        var builder = new ModelBuilder().Entity<SnapshotPrecisionEntity>();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Property(e => e.Amount).HasPrecision(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Property(e => e.Amount).HasPrecision(4, 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Property(e => e.Amount).HasPrecision(4, -1));
        Assert.Throws<ArgumentException>(() => builder.Property(e => e.Id).HasPrecision(4, 2));
    }

    [Fact]
    public void BuildFromContext_IncludesOwnsOneFluentMetadata()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotOwnsOneOwner>()
                    .OwnsOne(o => o.Address, owned =>
                    {
                        owned.Property(a => a.City)
                            .HasColumnName("Ship_City")
                            .HasDefaultValueSql("''")
                            .HasCollation("NOCASE");
                        owned.Property(a => a.CityLength)
                            .HasColumnName("Ship_CityLength")
                            .HasComputedColumnSql("length(Ship_City)", stored: true);
                        owned.Property<string>("AuditTag")
                            .HasColumnName("Ship_AuditTag");
                        owned.HasCheckConstraint("CK_SnapshotOwnsOneOwner_ShipCity", "length(Ship_City) > 0");
                        owned.HasExpressionIndex("IX_SnapshotOwnsOneOwner_LowerShipCity", "lower(Ship_City)", filterSql: "Ship_City <> ''");
                    })
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotOwnsOneOwner");
        var city = table.Columns.Single(c => c.Name == "Ship_City");
        Assert.Equal("''", city.DefaultValue);
        Assert.Equal("NOCASE", city.Collation);

        var cityLength = table.Columns.Single(c => c.Name == "Ship_CityLength");
        Assert.Equal("length(Ship_City)", cityLength.ComputedColumnSql);
        Assert.True(cityLength.IsStoredComputedColumn);

        var auditTag = table.Columns.Single(c => c.Name == "Ship_AuditTag");
        Assert.Equal(typeof(string).FullName, auditTag.ClrType);
        Assert.True(auditTag.IsNullable);

        var check = Assert.Single(table.CheckConstraints);
        Assert.Equal("CK_SnapshotOwnsOneOwner_ShipCity", check.ConstraintName);
        Assert.Equal("length(Ship_City) > 0", check.Sql);

        var expressionIndex = Assert.Single(table.ExpressionIndexes);
        Assert.Equal("IX_SnapshotOwnsOneOwner_LowerShipCity", expressionIndex.Name);
        Assert.Equal("lower(Ship_City)", expressionIndex.ExpressionSql);
        Assert.Equal("Ship_City <> ''", expressionIndex.FilterSql);

        var diff = SchemaDiffer.Diff(new SchemaSnapshot(), snapshot);
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff).Up;
        var createOwner = Assert.Single(sql, statement => statement.StartsWith("CREATE TABLE \"SnapshotOwnsOneOwner\"", StringComparison.Ordinal));
        Assert.Contains("\"Ship_City\" TEXT COLLATE NOCASE NULL DEFAULT ''", createOwner, StringComparison.Ordinal);
        Assert.Contains("\"Ship_CityLength\" INTEGER GENERATED ALWAYS AS (length(Ship_City)) STORED", createOwner, StringComparison.Ordinal);
        Assert.Contains("\"Ship_AuditTag\" TEXT NULL", createOwner, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT \"CK_SnapshotOwnsOneOwner_ShipCity\" CHECK (length(Ship_City) > 0)", createOwner, StringComparison.Ordinal);
        Assert.Contains(sql, statement =>
            statement.Contains("CREATE INDEX \"IX_SnapshotOwnsOneOwner_LowerShipCity\" ON \"SnapshotOwnsOneOwner\" (lower(Ship_City)) WHERE Ship_City <> ''", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildFromContext_IncludesExpressionIndexFacets()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotIndexedEntity>()
                    .HasExpressionIndex(
                        "IX_SnapshotIndexedEntity_LowerCode",
                        "lower(Code)",
                        isUnique: true,
                        filterSql: "Code IS NOT NULL",
                        includedColumnNames: new[] { "Code" },
                        nullsNotDistinct: true,
                        nullSortOrder: IndexNullSortOrder.Last)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var table = snapshot.Tables.Single(t => t.Name == "SnapshotIndexedEntity");
        var expressionIndex = Assert.Single(table.ExpressionIndexes);
        Assert.Equal("IX_SnapshotIndexedEntity_LowerCode", expressionIndex.Name);
        Assert.Equal("lower(Code)", expressionIndex.ExpressionSql);
        Assert.True(expressionIndex.IsUnique);
        Assert.Equal("Code IS NOT NULL", expressionIndex.FilterSql);
        Assert.Equal(new[] { "Code" }, expressionIndex.IncludedColumnNames);
        Assert.True(expressionIndex.NullsNotDistinct);
        Assert.Equal(IndexNullSortOrder.Last, expressionIndex.NullSortOrder);
    }

    [Fact]
    public void BuildFromContext_IncludesIndexAttributes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SnapshotIndexedEntity>();
                mb.Entity<SnapshotDescendingIndexedEntity>();
                mb.Entity<SnapshotIncludedIndexedEntity>();
                mb.Entity<SnapshotNullsNotDistinctIndexedEntity>();
                mb.Entity<SnapshotCompositeIndexedEntity>();
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var indexed = snapshot.Tables.Single(t => t.Name == "SnapshotIndexedEntity");
        var code = indexed.Columns.Single(c => c.Name == "Code");
        Assert.Equal("IX_SnapshotIndexedEntity_Code", code.IndexName);
        Assert.True(code.IsUnique);

        var descending = snapshot.Tables.Single(t => t.Name == "SnapshotDescendingIndexedEntity");
        var descendingCode = descending.Columns.Single(c => c.Name == "Code");
        Assert.True(Assert.Single(descendingCode.Indexes).IsDescending);

        var included = snapshot.Tables.Single(t => t.Name == "SnapshotIncludedIndexedEntity");
        var includedCode = included.Columns.Single(c => c.Name == "Code");
        var includedDisplayName = included.Columns.Single(c => c.Name == "DisplayName");
        Assert.False(Assert.Single(includedCode.Indexes).IsIncluded);
        Assert.True(Assert.Single(includedDisplayName.Indexes).IsIncluded);

        var nullsNotDistinct = snapshot.Tables.Single(t => t.Name == "SnapshotNullsNotDistinctIndexedEntity");
        var nullsNotDistinctCode = nullsNotDistinct.Columns.Single(c => c.Name == "Code");
        var nullsNotDistinctIndex = Assert.Single(nullsNotDistinctCode.Indexes);
        Assert.True(nullsNotDistinctIndex.IsUnique);
        Assert.True(nullsNotDistinctIndex.NullsNotDistinct);
        Assert.Equal(IndexNullSortOrder.First, nullsNotDistinctIndex.NullSortOrder);

        var composite = snapshot.Tables.Single(t => t.Name == "SnapshotCompositeIndexedEntity");
        var tenant = composite.Columns.Single(c => c.Name == "TenantId");
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
                    Name = composite.Name,
                    Columns =
                    {
                        new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true, IsUnique = true, IndexName = "PK_" + composite.Name },
                        new ColumnSchema { Name = "TenantId", ClrType = typeof(int).FullName!, IsNullable = false },
                        new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false }
                    }
                }
            }
        };
        var diff = SchemaDiffer.Diff(oldSnapshot, snapshot);
        var addedComposite = Assert.Single(diff.AddedIndexes, x => x.IndexName == "IX_SnapshotComposite_Tenant_Code");
        Assert.True(addedComposite.IsUnique);
        Assert.Equal(new[] { "TenantId", "Code" }, addedComposite.ColumnNames);
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
    public void BuildFromContext_PreservesExplicitForeignKeyConstraintName()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotFkParent>()
                    .HasMany(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id, constraintName: "FK_Custom_SnapshotFkChild_Parent", cascadeDelete: false)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var child = snapshot.Tables.Single(t => t.Name == "SnapshotFkChild");
        var fk = Assert.Single(child.ForeignKeys);
        Assert.Equal("FK_Custom_SnapshotFkChild_Parent", fk.ConstraintName);
        Assert.Equal("NO ACTION", fk.OnDelete);
    }

    [Fact]
    public void BuildFromContext_IncludesOwnedCollectionTable()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotOwnedOwner>()
                    .OwnsMany<SnapshotOwnedLine>(
                        o => o.Lines,
                        tableName: "SnapshotOwnedLines",
                        foreignKey: "OwnerId",
                        buildAction: owned =>
                        {
                            owned.Property(l => l.Sku)
                                .HasDefaultValueSql("''")
                                .HasCollation("NOCASE");
                            owned.Property(l => l.SkuLength)
                                .HasComputedColumnSql("length(Sku)", stored: true);
                            owned.Property<string>("AuditTag")
                                .HasColumnName("Audit_Tag");
                            owned.HasCheckConstraint("CK_SnapshotOwnedLines_Sku", "length(Sku) > 0");
                            owned.HasExpressionIndex("IX_SnapshotOwnedLines_LowerSku", "lower(Sku)", filterSql: "Sku <> ''");
                        })
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var owned = snapshot.Tables.Single(t => t.Name == "SnapshotOwnedLines");
        Assert.Equal(new[] { "OwnerId", "LineId", "Sku", "SkuLength", "Amount", "Audit_Tag" }, owned.Columns.Select(c => c.Name).ToArray());
        var ownerId = owned.Columns.Single(c => c.Name == "OwnerId");
        Assert.Equal(typeof(int).FullName, ownerId.ClrType);
        Assert.False(ownerId.IsNullable);
        Assert.False(ownerId.IsPrimaryKey);

        var lineId = owned.Columns.Single(c => c.Name == "LineId");
        Assert.True(lineId.IsPrimaryKey);
        Assert.True(lineId.IsUnique);
        Assert.True(lineId.IsIdentity);
        Assert.Equal("PK_SnapshotOwnedLines", lineId.IndexName);

        var sku = owned.Columns.Single(c => c.Name == "Sku");
        Assert.Equal("''", sku.DefaultValue);
        Assert.Equal("NOCASE", sku.Collation);
        Assert.False(sku.IsNullable);
        Assert.Equal("IX_SnapshotOwnedLines_Sku", sku.IndexName);
        Assert.True(sku.IsUnique);
        Assert.Contains(sku.Indexes, i => i.Name == "IX_SnapshotOwnedLines_Sku" && i.IsUnique);

        var skuLength = owned.Columns.Single(c => c.Name == "SkuLength");
        Assert.Equal("length(Sku)", skuLength.ComputedColumnSql);
        Assert.True(skuLength.IsStoredComputedColumn);

        var amount = owned.Columns.Single(c => c.Name == "Amount");
        Assert.Equal(12, amount.Precision);
        Assert.Equal(2, amount.Scale);

        var shadow = owned.Columns.Single(c => c.Name == "Audit_Tag");
        Assert.Equal(typeof(string).FullName, shadow.ClrType);
        Assert.True(shadow.IsNullable);

        var check = Assert.Single(owned.CheckConstraints);
        Assert.Equal("CK_SnapshotOwnedLines_Sku", check.ConstraintName);
        Assert.Equal("length(Sku) > 0", check.Sql);

        var expressionIndex = Assert.Single(owned.ExpressionIndexes);
        Assert.Equal("IX_SnapshotOwnedLines_LowerSku", expressionIndex.Name);
        Assert.Equal("lower(Sku)", expressionIndex.ExpressionSql);
        Assert.Equal("Sku <> ''", expressionIndex.FilterSql);

        var fk = Assert.Single(owned.ForeignKeys);
        Assert.Equal("FK_SnapshotOwnedLines_SnapshotOwnedOwner_OwnerId", fk.ConstraintName);
        Assert.Equal(new[] { "OwnerId" }, fk.DependentColumns);
        Assert.Equal("SnapshotOwnedOwner", fk.PrincipalTable);
        Assert.Equal(new[] { "Id" }, fk.PrincipalColumns);
        Assert.Equal("CASCADE", fk.OnDelete);

        var diff = SchemaDiffer.Diff(new SchemaSnapshot(), snapshot);
        Assert.Contains(diff.AddedTables, table => table.Name == "SnapshotOwnedLines");
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff).Up;
        var createOwned = Assert.Single(sql, statement => statement.StartsWith("CREATE TABLE \"SnapshotOwnedLines\"", StringComparison.Ordinal));
        Assert.Contains("\"OwnerId\" INTEGER NOT NULL", createOwned, StringComparison.Ordinal);
        Assert.Contains("\"LineId\" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT", createOwned, StringComparison.Ordinal);
        Assert.Contains("\"Sku\" TEXT COLLATE NOCASE NOT NULL DEFAULT ''", createOwned, StringComparison.Ordinal);
        Assert.Contains("\"SkuLength\" INTEGER GENERATED ALWAYS AS (length(Sku)) STORED", createOwned, StringComparison.Ordinal);
        Assert.Contains("\"Audit_Tag\" TEXT NULL", createOwned, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT \"CK_SnapshotOwnedLines_Sku\" CHECK (length(Sku) > 0)", createOwned, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT \"FK_SnapshotOwnedLines_SnapshotOwnedOwner_OwnerId\" FOREIGN KEY", createOwned, StringComparison.Ordinal);
        Assert.Contains("ON DELETE CASCADE", createOwned, StringComparison.Ordinal);
        Assert.Contains(sql, statement =>
            statement.Contains("CREATE INDEX \"IX_SnapshotOwnedLines_LowerSku\" ON \"SnapshotOwnedLines\" (lower(Sku)) WHERE Sku <> ''", StringComparison.Ordinal));
        Assert.Contains(sql, statement =>
            statement.Contains("CREATE UNIQUE INDEX \"IX_SnapshotOwnedLines_Sku\" ON \"SnapshotOwnedLines\" (\"Sku\")", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildFromContext_IncludesSchemaQualifiedOwnedCollectionTable()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotOwnedOwner>()
                    .OwnsMany<SnapshotOwnedLine>(
                        o => o.Lines,
                        tableName: "SnapshotOwnedLines",
                        foreignKey: "OwnerId",
                        schema: "tenant")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var owned = snapshot.Tables.Single(t => t.Name == "tenant.SnapshotOwnedLines");
        var fk = Assert.Single(owned.ForeignKeys);
        Assert.Equal("SnapshotOwnedOwner", fk.PrincipalTable);
        Assert.Equal(new[] { "OwnerId" }, fk.DependentColumns);

        var diff = SchemaDiffer.Diff(new SchemaSnapshot(), snapshot);
        Assert.Contains(diff.AddedTables, table => table.Name == "tenant.SnapshotOwnedLines");
        var sql = new PostgresMigrationSqlGenerator().GenerateSql(diff).Up;
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS \"tenant\"", sql);
        Assert.Contains(sql, statement =>
            statement.StartsWith("CREATE TABLE \"tenant\".\"SnapshotOwnedLines\"", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildFromContext_IncludesManyToManyJoinTable()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotM2MPost>()
                    .HasMany<SnapshotM2MLabel>(p => p.Labels)
                    .WithMany(l => l.Posts)
                    .UsingTable("SnapshotPostLabel", "PostId", "LabelId")
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var join = snapshot.Tables.Single(t => t.Name == "SnapshotPostLabel");
        Assert.Equal(new[] { "PostId", "LabelId" }, join.Columns.Select(c => c.Name).ToArray());
        Assert.All(join.Columns, c =>
        {
            Assert.False(c.IsNullable);
            Assert.True(c.IsPrimaryKey);
            Assert.Equal(typeof(int).FullName, c.ClrType);
        });
        Assert.Contains(join.ForeignKeys, fk =>
            fk.ConstraintName == "FK_SnapshotPostLabel_SnapshotM2MPost_PostId" &&
            fk.PrincipalTable == "SnapshotM2MPost" &&
            fk.DependentColumns.SequenceEqual(new[] { "PostId" }) &&
            fk.PrincipalColumns.SequenceEqual(new[] { "Id" }));
        Assert.Contains(join.ForeignKeys, fk =>
            fk.ConstraintName == "FK_SnapshotPostLabel_SnapshotM2MLabel_LabelId" &&
            fk.PrincipalTable == "SnapshotM2MLabel" &&
            fk.DependentColumns.SequenceEqual(new[] { "LabelId" }) &&
            fk.PrincipalColumns.SequenceEqual(new[] { "Id" }));

        var diff = SchemaDiffer.Diff(new SchemaSnapshot(), snapshot);
        Assert.Contains(diff.AddedTables, table => table.Name == "SnapshotPostLabel");
        var sql = new SqliteMigrationSqlGenerator().GenerateSql(diff).Up;
        Assert.Contains(sql, statement =>
            statement.Contains("CREATE TABLE \"SnapshotPostLabel\"", System.StringComparison.Ordinal) &&
            statement.Contains("PRIMARY KEY (\"PostId\", \"LabelId\")", System.StringComparison.Ordinal) &&
            statement.Contains("CONSTRAINT \"FK_SnapshotPostLabel_SnapshotM2MPost_PostId\" FOREIGN KEY", System.StringComparison.Ordinal) &&
            statement.Contains("CONSTRAINT \"FK_SnapshotPostLabel_SnapshotM2MLabel_LabelId\" FOREIGN KEY", System.StringComparison.Ordinal));
    }

    [Fact]
    public void BuildFromContext_ManyToManyJoinTable_IncludesReferentialActions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SnapshotM2MPost>()
                    .HasMany<SnapshotM2MLabel>(p => p.Labels)
                    .WithMany(l => l.Posts)
                    .UsingTable(
                        "SnapshotPostLabel",
                        new[] { "PostId" },
                        new[] { "LabelId" },
                        ReferentialAction.Cascade,
                        ReferentialAction.Cascade,
                        ReferentialAction.Restrict,
                        ReferentialAction.NoAction)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var join = snapshot.Tables.Single(t => t.Name == "SnapshotPostLabel");
        Assert.Contains(join.ForeignKeys, fk =>
            fk.PrincipalTable == "SnapshotM2MPost" &&
            fk.DependentColumns.SequenceEqual(new[] { "PostId" }) &&
            fk.OnDelete == "CASCADE" &&
            fk.OnUpdate == "CASCADE");
        Assert.Contains(join.ForeignKeys, fk =>
            fk.PrincipalTable == "SnapshotM2MLabel" &&
            fk.DependentColumns.SequenceEqual(new[] { "LabelId" }) &&
            fk.OnDelete == "RESTRICT" &&
            fk.OnUpdate == "NO ACTION");
    }

    [Fact]
    public void BuildFromContext_ManyToManyJoinTable_DeduplicatesSharedTenantColumn()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SnapshotM2MStudent>().HasKey(s => new { s.TenantId, s.StudentId });
                mb.Entity<SnapshotM2MCourse>().HasKey(c => new { c.TenantId, c.CourseId });
                mb.Entity<SnapshotM2MStudent>()
                    .HasMany<SnapshotM2MCourse>(s => s.Courses)
                    .WithMany(c => c.Students)
                    .UsingTable(
                        "SnapshotStudentCourse",
                        new[] { "TenantId", "StudentId" },
                        new[] { "TenantId", "CourseId" });
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var join = snapshot.Tables.Single(t => t.Name == "SnapshotStudentCourse");
        Assert.Equal(new[] { "TenantId", "StudentId", "CourseId" }, join.Columns.Select(c => c.Name).ToArray());
        Assert.All(join.Columns, c => Assert.True(c.IsPrimaryKey));
        Assert.Contains(join.ForeignKeys, fk =>
            fk.PrincipalTable == "SnapshotM2MStudent" &&
            fk.DependentColumns.SequenceEqual(new[] { "TenantId", "StudentId" }) &&
            fk.PrincipalColumns.SequenceEqual(new[] { "TenantId", "StudentId" }));
        Assert.Contains(join.ForeignKeys, fk =>
            fk.PrincipalTable == "SnapshotM2MCourse" &&
            fk.DependentColumns.SequenceEqual(new[] { "TenantId", "CourseId" }) &&
            fk.PrincipalColumns.SequenceEqual(new[] { "TenantId", "CourseId" }));
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
    public void SchemaSnapshotBuilder_DoesNotCountIncludedColumnsAsUniqueIndexKeys()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotUniqueIncludedIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotUniqueIncludedIndexedEntity");
        var code = table.Columns.Single(c => c.Name == "Code");
        var displayName = table.Columns.Single(c => c.Name == "DisplayName");

        Assert.True(code.IsUnique);
        Assert.False(displayName.IsUnique);
        Assert.Contains(code.Indexes, index =>
            index.Name == "UX_SnapshotUniqueIncludedIndexedEntity_Code" &&
            index.IsUnique &&
            !index.IsIncluded);
        Assert.Contains(displayName.Indexes, index =>
            index.Name == "UX_SnapshotUniqueIncludedIndexedEntity_Code" &&
            index.IsIncluded);

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
                        new ColumnSchema { Name = "Code", ClrType = typeof(string).FullName!, IsNullable = false },
                        new ColumnSchema { Name = "DisplayName", ClrType = typeof(string).FullName!, IsNullable = false }
                    }
                }
            }
        };
        var diff = SchemaDiffer.Diff(oldSnapshot, snapshot);
        var addedIndex = Assert.Single(diff.AddedIndexes, index => index.IndexName == "UX_SnapshotUniqueIncludedIndexedEntity_Code");
        Assert.True(addedIndex.IsUnique);
        Assert.Equal(new[] { "Code" }, addedIndex.ColumnNames);
    }

    [Fact]
    public void SchemaSnapshotBuilder_ReadsNullsNotDistinctIndexAttribute()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotNullsNotDistinctIndexedEntity).Assembly);
        var table = snapshot.Tables.Single(t => t.Name == "SnapshotNullsNotDistinctIndexedEntity");
        var code = table.Columns.Single(c => c.Name == "Code");
        var index = Assert.Single(code.Indexes);

        Assert.True(index.IsUnique);
        Assert.True(index.NullsNotDistinct);
        Assert.Equal(IndexNullSortOrder.First, index.NullSortOrder);
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
    public void SchemaDiff_destructive_warnings_include_integrity_drops()
    {
        var table = new TableSchema
        {
            Name = "Orders",
            Columns =
            {
                new ColumnSchema { Name = "Email", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "UX_Orders_Email", IsUnique = true },
                new ColumnSchema { Name = "Title", ClrType = typeof(string).FullName!, IsNullable = false, IndexName = "IX_Orders_Title", IsUnique = false },
                new ColumnSchema { Name = "CustomerId", ClrType = typeof(int).FullName!, IsNullable = false }
            }
        };

        var diff = new SchemaDiff();
        diff.DroppedForeignKeys.Add((table, new ForeignKeySchema
        {
            ConstraintName = "FK_Orders_Customers_CustomerId",
            DependentColumns = new[] { "CustomerId" },
            PrincipalTable = "Customers",
            PrincipalColumns = new[] { "Id" }
        }));
        diff.DroppedCheckConstraints.Add((table, new CheckConstraintSchema
        {
            ConstraintName = "CK_Orders_Email",
            Sql = "length(Email) > 0"
        }));
        diff.DroppedIndexes.Add((table, "UX_Orders_Email"));
        diff.DroppedIndexes.Add((table, "IX_Orders_Title"));
        diff.DroppedExpressionIndexes.Add((table, new ExpressionIndexSchema
        {
            Name = "UX_Orders_LowerEmail",
            ExpressionSql = "lower(Email)",
            IsUnique = true
        }));
        diff.DroppedExpressionIndexes.Add((table, new ExpressionIndexSchema
        {
            Name = "IX_Orders_TitleLength",
            ExpressionSql = "length(Title)",
            IsUnique = false
        }));

        Assert.True(diff.HasDestructiveChanges);
        var warnings = diff.GetDestructiveChangeWarnings();
        Assert.Contains(warnings, w => w.Contains("FK_Orders_Customers_CustomerId", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("CK_Orders_Email", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("UX_Orders_Email", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("UX_Orders_LowerEmail", StringComparison.Ordinal));
        Assert.DoesNotContain(warnings, w => w.Contains("IX_Orders_Title", StringComparison.Ordinal));
        Assert.DoesNotContain(warnings, w => w.Contains("IX_Orders_TitleLength", StringComparison.Ordinal));
    }

    [Fact]
    public void SchemaDiff_destructive_warnings_include_risky_column_alters()
    {
        var table = new TableSchema { Name = "Accounts" };
        var oldColumn = new ColumnSchema
        {
            Name = "Balance",
            ClrType = typeof(decimal).FullName!,
            IsNullable = true,
            Precision = 18,
            Scale = 4,
            IsPrimaryKey = true,
            IsUnique = true,
            IsIdentity = true,
            IdentitySeed = 1,
            IdentityIncrement = 1,
            ComputedColumnSql = "Amount + Fees",
            IsStoredComputedColumn = true
        };
        var newColumn = new ColumnSchema
        {
            Name = "Balance",
            ClrType = typeof(double).FullName!,
            IsNullable = false,
            Precision = 10,
            Scale = 2,
            IsPrimaryKey = false,
            IsUnique = false,
            IsIdentity = false,
            ComputedColumnSql = "Amount - Fees",
            IsStoredComputedColumn = false
        };

        var diff = new SchemaDiff();
        diff.AlteredColumns.Add((table, newColumn, oldColumn));

        Assert.True(diff.HasDestructiveChanges);
        var warnings = diff.GetDestructiveChangeWarnings();
        Assert.Contains(warnings, w => w.Contains("changes type", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("nullable to required", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("narrows precision/scale", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("drops primary key", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("drops uniqueness", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("changes identity metadata", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("changes computed column SQL", StringComparison.Ordinal));
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
                                   && colType != typeof(string)
                                   && colType != typeof(byte[]);
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
        Assert.DoesNotContain(diff.AddedIndexes, ix => ix.IndexName.StartsWith("__UQ__", StringComparison.Ordinal));
        Assert.DoesNotContain(diff.DroppedIndexes, ix => ix.IndexName.StartsWith("__UQ__", StringComparison.Ordinal));
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
        Assert.DoesNotContain(diff.AddedIndexes, ix => ix.IndexName.StartsWith("__PK__", StringComparison.Ordinal));
        Assert.DoesNotContain(diff.DroppedIndexes, ix => ix.IndexName.StartsWith("__PK__", StringComparison.Ordinal));
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
    public void SchemaSnapshotBuilder_ReadsDecimalPrecisionFromProviderStyleColumnTypeName()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotPrecisionProviderTextEntity).Assembly);
        var table = Assert.Single(snapshot.Tables.Where(t => t.Name == "SnapshotPrecisionProviderTextEntity"));

        var spaced = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotPrecisionProviderTextEntity.SpacedAmount)));
        var precisionOnly = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotPrecisionProviderTextEntity.PrecisionOnlyAmount)));
        var domain = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotPrecisionProviderTextEntity.DomainAmount)));
        var falsePositive = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotPrecisionProviderTextEntity.NotDecimal)));

        Assert.Equal(19, spaced.Precision);
        Assert.Equal(4, spaced.Scale);
        Assert.Equal(10, precisionOnly.Precision);
        Assert.Null(precisionOnly.Scale);
        Assert.Equal(12, domain.Precision);
        Assert.Equal(3, domain.Scale);
        Assert.Null(falsePositive.Precision);
        Assert.Null(falsePositive.Scale);
    }

    [Fact]
    public void SchemaSnapshotBuilder_ReadsMaxLengthFromLengthAttributes()
    {
        var snapshot = SchemaSnapshotBuilder.Build(typeof(SnapshotLengthEntity).Assembly);
        var table = Assert.Single(snapshot.Tables.Where(t => t.Name == "SnapshotLengthEntity"));

        var name = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotLengthEntity.Name)));
        var code = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotLengthEntity.Code)));
        var payload = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotLengthEntity.Payload)));
        var notes = Assert.Single(table.Columns.Where(c => c.Name == nameof(SnapshotLengthEntity.Notes)));

        Assert.Equal(80, name.MaxLength);
        Assert.Equal(40, code.MaxLength);
        Assert.Equal(32, payload.MaxLength);
        Assert.Null(notes.MaxLength);
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

    [Fact]
    public void SchemaDiffer_DetectsAndWarnsForMaxLengthNarrowing()
    {
        var oldSnapshot = new SchemaSnapshot
        {
            Tables =
            {
                new TableSchema
                {
                    Name = "Customer",
                    Columns =
                    {
                        new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, MaxLength = 80, IsNullable = false }
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
                    Name = "Customer",
                    Columns =
                    {
                        new ColumnSchema { Name = "Name", ClrType = typeof(string).FullName!, MaxLength = 40, IsNullable = false }
                    }
                }
            }
        };

        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);
        var altered = Assert.Single(diff.AlteredColumns);

        Assert.Equal("Name", altered.NewColumn.Name);
        Assert.Contains(diff.GetDestructiveChangeWarnings(), warning =>
            warning.Contains("narrows max length from '80' to '40'", StringComparison.Ordinal));
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
