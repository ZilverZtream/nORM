using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for the Value Converter feature: bi-directional conversion between model types and provider types.
/// </summary>
public class ValueConverterTests
{
    // ── Shared entity types ──────────────────────────────────────────────────

    public class Product
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public ProductStatus Status { get; set; }
        public int Score { get; set; }
        public string? Tag { get; set; }
    }

    public enum ProductStatus { Active = 1, Inactive = 2, Archived = 3 }

    // Converts enum↔int
    public class EnumToIntConverter : ValueConverter<ProductStatus, int>
    {
        public override object? ConvertToProvider(ProductStatus v) => (int)v;
        public override object? ConvertFromProvider(int v) => (ProductStatus)v;
    }

    // Converts int↔string
    public class IntToStringConverter : ValueConverter<int, string>
    {
        public override object? ConvertToProvider(int v) => v.ToString();
        public override object? ConvertFromProvider(string v) => int.Parse(v);
    }

    // Converts string↔string (upper-case)
    public class UpperCaseConverter : ValueConverter<string, string>
    {
        public override object? ConvertToProvider(string v) => v?.ToUpperInvariant();
        public override object? ConvertFromProvider(string v) => v?.ToLowerInvariant();
    }

    // Negating int converter (write: negate, read: negate back)
    public class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static SqliteConnection CreateAndOpenDb(string schema)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = schema;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private const string ProductSchema = "CREATE TABLE Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Status INTEGER, Score INTEGER, Tag TEXT)";
    private const string ProductTextStatusSchema = "CREATE TABLE Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Status TEXT, Score TEXT, Tag TEXT)";

    // ── CF-1: Configuration Tests ──────────────────────────────────────────

    [Fact]
    public void Config_HasConversion_IValueConverter_RegistersConverter()
    {
        var builder = new EntityTypeBuilder<Product>();
        var conv = new EnumToIntConverter();
        builder.Property(p => p.Status).HasConversion((IValueConverter)conv);
        var config = builder.Configuration;
        Assert.Single(config.Converters);
        Assert.Same(conv, config.Converters[0].Converter);
    }

    [Fact]
    public void Config_HasConversion_Generic_RegistersConverter()
    {
        var builder = new EntityTypeBuilder<Product>();
        var conv = new EnumToIntConverter();
        builder.Property<ProductStatus>(p => p.Status).HasConversion(conv);
        var config = builder.Configuration;
        Assert.Single(config.Converters);
        Assert.Same(conv, config.Converters[0].Converter);
    }

    [Fact]
    public void Config_HasConversion_PropertyInfo_Matches_Correct_Property()
    {
        var builder = new EntityTypeBuilder<Product>();
        var conv = new EnumToIntConverter();
        builder.Property<ProductStatus>(p => p.Status).HasConversion(conv);
        var config = builder.Configuration;
        Assert.Equal("Status", config.Converters[0].Property.Name);
    }

    [Fact]
    public void Config_MultipleConverters_AreAllRegistered()
    {
        var builder = new EntityTypeBuilder<Product>();
        var conv1 = new EnumToIntConverter();
        var conv2 = new IntToStringConverter();
        builder.Property<ProductStatus>(p => p.Status).HasConversion(conv1);
        builder.Property<int>(p => p.Score).HasConversion(conv2);
        var config = builder.Configuration;
        Assert.Equal(2, config.Converters.Count);
    }

    [Fact]
    public void Config_ConverterFingerprint_NonZero_WhenConverterSet()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Product));
        Assert.NotEqual(0, mapping.ConverterFingerprint);
    }

    [Fact]
    public void Config_ConverterFingerprint_Zero_WhenNoConverter()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(Product));
        Assert.Equal(0, mapping.ConverterFingerprint);
    }

    [Fact]
    public void Config_Column_ConverterField_Set_AfterMapping()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var conv = new EnumToIntConverter();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(conv)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Product));
        var statusCol = mapping.Columns.FirstOrDefault(c => c.PropName == "Status");
        Assert.NotNull(statusCol);
        Assert.Same(conv, statusCol!.Converter);
    }

    [Fact]
    public void Config_NonConverted_Column_Has_Null_Converter()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Product));
        var nameCol = mapping.Columns.FirstOrDefault(c => c.PropName == "Name");
        Assert.NotNull(nameCol);
        Assert.Null(nameCol!.Converter);
    }

    // ── CV-2: IValueConverter interface ────────────────────────────────────

    [Fact]
    public void IValueConverter_ConvertToProvider_EnumToInt()
    {
        IValueConverter conv = new EnumToIntConverter();
        var result = conv.ConvertToProvider(ProductStatus.Active);
        Assert.Equal(1, result);
    }

    [Fact]
    public void IValueConverter_ConvertFromProvider_IntToEnum()
    {
        IValueConverter conv = new EnumToIntConverter();
        var result = conv.ConvertFromProvider(2);
        Assert.Equal(ProductStatus.Inactive, result);
    }

    [Fact]
    public void IValueConverter_ConvertToProvider_Null_ReturnsNull()
    {
        IValueConverter conv = new EnumToIntConverter();
        var result = conv.ConvertToProvider(null);
        // null passes through without invoking override
        Assert.Null(result);
    }

    [Fact]
    public void IValueConverter_ConvertFromProvider_Null_ReturnsNull()
    {
        IValueConverter conv = new EnumToIntConverter();
        var result = conv.ConvertFromProvider(null);
        // null passes through without invoking override
        Assert.Null(result);
    }

    [Fact]
    public void IValueConverter_ModelType_ProviderType_Correct()
    {
        IValueConverter conv = new EnumToIntConverter();
        Assert.Equal(typeof(ProductStatus), conv.ModelType);
        Assert.Equal(typeof(int), conv.ProviderType);
    }

    // ── W-3: Write INSERT Tests ────────────────────────────────────────────

    [Fact]
    public async Task Insert_WithConverter_StoresProviderValue()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Status = ProductStatus.Inactive };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        // Read raw value from DB
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Status FROM Product WHERE Name = 'Widget'";
        var raw = cmd.ExecuteScalar();
        Assert.Equal(2L, raw); // Inactive = 2 stored as INTEGER
    }

    [Fact]
    public async Task Insert_WithStringConverter_StoresProviderValue()
    {
        using var cn = CreateAndOpenDb(ProductTextStatusSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<int>(p => p.Score)
                .HasConversion(new IntToStringConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Score = 42 };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Score FROM Product WHERE Name = 'Widget'";
        var raw = cmd.ExecuteScalar();
        Assert.Equal("42", raw);
    }

    [Fact]
    public async Task BatchInsert_WithConverter_StoresProviderValue()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "A", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "B", Status = ProductStatus.Archived });
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Status FROM Product ORDER BY Name";
        using var reader = cmd.ExecuteReader();
        var statuses = new List<long>();
        while (reader.Read()) statuses.Add(reader.GetInt64(0));
        Assert.Equal(new long[] { 1, 3 }, statuses);
    }

    [Fact]
    public async Task PreparedInsert_WithConverter_StoresProviderValue()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Insert multiple times to exercise PreparedInsertCommand path
        for (int i = 0; i < 3; i++)
        {
            var p = new Product { Name = $"P{i}", Status = ProductStatus.Active };
            ctx.Add(p);
        }
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Product WHERE Status = 1";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(3, count);
    }

    // ── W-4: Write UPDATE Tests ────────────────────────────────────────────

    [Fact]
    public async Task Update_WithConverter_StoresProviderValue()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Status = ProductStatus.Active };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        product.Status = ProductStatus.Inactive;
        ctx.Update(product);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Status FROM Product WHERE Name = 'Widget'";
        var raw = cmd.ExecuteScalar();
        Assert.Equal(2L, raw);
    }

    [Fact]
    public async Task Delete_WithConverter_WorksCorrectly()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Status = ProductStatus.Active };
        ctx.Add(product);
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0, product.Id);

        ctx.Remove(product);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Product";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(0, count);
    }

    // ── R-5: Read/Materialization Tests ────────────────────────────────────

    [Fact]
    public async Task Read_WithConverter_AppliesConvertFromProvider()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        // Insert raw integer for Status
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Product (Name, Status, Score, Tag) VALUES ('Widget', 2, 0, NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var products = await ctx.Query<Product>().ToListAsync();
        Assert.Single(products);
        Assert.Equal(ProductStatus.Inactive, products[0].Status);
    }

    [Fact]
    public async Task Read_StringToInt_AppliesConvertFromProvider()
    {
        using var cn = CreateAndOpenDb(ProductTextStatusSchema);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Product (Name, Status, Score, Tag) VALUES ('Widget', 'unused', '99', NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<int>(p => p.Score)
                .HasConversion(new IntToStringConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var products = await ctx.Query<Product>().ToListAsync();
        Assert.Single(products);
        Assert.Equal(99, products[0].Score);
    }

    [Fact]
    public async Task RoundTrip_WriteAndRead_WithConverter_ValueMatches()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Status = ProductStatus.Archived };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Product>().Where(p => p.Id == product.Id).FirstAsync();
        Assert.Equal(ProductStatus.Archived, loaded.Status);
    }

    [Fact]
    public async Task RoundTrip_MultipleRows_AllValuesConverted()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "A", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "B", Status = ProductStatus.Inactive });
        ctx.Add(new Product { Name = "C", Status = ProductStatus.Archived });
        await ctx.SaveChangesAsync();

        var all = await ctx.Query<Product>().OrderBy(p => p.Name).ToListAsync();
        Assert.Equal(3, all.Count);
        Assert.Equal(ProductStatus.Active, all[0].Status);
        Assert.Equal(ProductStatus.Inactive, all[1].Status);
        Assert.Equal(ProductStatus.Archived, all[2].Status);
    }

    [Fact]
    public async Task Read_NullValue_WithConverter_NullableProperty_ReturnsNull()
    {
        // Use Tag (nullable string) with UpperCaseConverter
        using var cn = CreateAndOpenDb(ProductSchema);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Product (Name, Status, Score, Tag) VALUES ('Widget', 1, 0, NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<string?>(p => p.Tag)
                .HasConversion((IValueConverter)new UpperCaseConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var products = await ctx.Query<Product>().ToListAsync();
        Assert.Single(products);
        Assert.Null(products[0].Tag);
    }

    [Fact]
    public async Task Read_MultipleConverters_AllApplied()
    {
        using var cn = CreateAndOpenDb(ProductTextStatusSchema);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Product (Name, Status, Score, Tag) VALUES ('Widget', 'unused', '77', 'HELLO')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Product>()
                    .Property<int>(p => p.Score)
                    .HasConversion(new IntToStringConverter());
                mb.Entity<Product>()
                    .Property<string?>(p => p.Tag)
                    .HasConversion((IValueConverter)new UpperCaseConverter());
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var products = await ctx.Query<Product>().ToListAsync();
        Assert.Single(products);
        Assert.Equal(77, products[0].Score);
        Assert.Equal("hello", products[0].Tag); // UpperCaseConverter.ConvertFromProvider lowercases
    }

    // ── QP-6: Query/WHERE Tests ────────────────────────────────────────────

    [Fact]
    public async Task Where_WithConverter_EnumComparison_FiltersCorrectly()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "A", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "B", Status = ProductStatus.Inactive });
        await ctx.SaveChangesAsync();

        var active = await ctx.Query<Product>().Where(p => p.Status == ProductStatus.Active).ToListAsync();
        Assert.Single(active);
        Assert.Equal("A", active[0].Name);
    }

    [Fact]
    public async Task Where_ByName_WorksWhenConverterOnOtherProperty()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "Alpha", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "Beta", Status = ProductStatus.Inactive });
        await ctx.SaveChangesAsync();

        var result = await ctx.Query<Product>().Where(p => p.Name == "Alpha").ToListAsync();
        Assert.Single(result);
        Assert.Equal(ProductStatus.Active, result[0].Status);
    }

    [Fact]
    public async Task OrderBy_WithConverter_SortsCorrectly()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "C", Status = ProductStatus.Archived });  // 3
        ctx.Add(new Product { Name = "A", Status = ProductStatus.Active });    // 1
        ctx.Add(new Product { Name = "B", Status = ProductStatus.Inactive });  // 2
        await ctx.SaveChangesAsync();

        var all = await ctx.Query<Product>().OrderBy(p => p.Status).ToListAsync();
        Assert.Equal(new[] { "A", "B", "C" }, all.Select(p => p.Name).ToArray());
    }

    [Fact]
    public async Task Select_ProjectionWithConverter_IsCorrect()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "Widget", Status = ProductStatus.Inactive });
        await ctx.SaveChangesAsync();

        var names = await ctx.Query<Product>().Select(p => p.Name).ToListAsync();
        Assert.Equal(new[] { "Widget" }, names.ToArray());
    }

    // ── PERF-7: No-converter case is not affected ──────────────────────────

    [Fact]
    public async Task NoConverter_Roundtrip_WorksNormally()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        using var ctx = new DbContext(cn, new SqliteProvider());
        var product = new Product { Name = "NoConv", Status = ProductStatus.Active, Score = 5 };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Product>().FirstAsync();
        Assert.Equal(ProductStatus.Active, loaded.Status);
        Assert.Equal(5, loaded.Score);
    }

    [Fact]
    public async Task NoConverter_BatchInsert_WorksNormally()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        using var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 0; i < 5; i++)
            ctx.Add(new Product { Name = $"P{i}", Status = ProductStatus.Active });
        await ctx.SaveChangesAsync();

        var count = await ctx.Query<Product>().CountAsync();
        Assert.Equal(5, count);
    }

    // ── NegatingConverter tests ────────────────────────────────────────────

    [Fact]
    public async Task NegatingConverter_WriteAndRead_ValuesNegatedCorrectly()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<int>(p => p.Score)
                .HasConversion(new NegatingConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Score = 42 };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        // Verify stored as -42
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT Score FROM Product";
            var raw = cmd.ExecuteScalar();
            Assert.Equal(-42L, raw);
        }

        // Verify read back as 42
        var loaded = await ctx.Query<Product>().FirstAsync();
        Assert.Equal(42, loaded.Score);
    }

    [Fact]
    public async Task UpperCaseConverter_Write_StoresUpperCase()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<string?>(p => p.Tag)
                .HasConversion((IValueConverter)new UpperCaseConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Tag = "hello" };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Tag FROM Product";
        var raw = cmd.ExecuteScalar();
        Assert.Equal("HELLO", raw);
    }

    [Fact]
    public async Task UpperCaseConverter_Read_ReturnsLowerCase()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Product (Name, Status, Score, Tag) VALUES ('Widget', 1, 0, 'HELLO')";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<string?>(p => p.Tag)
                .HasConversion((IValueConverter)new UpperCaseConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var products = await ctx.Query<Product>().ToListAsync();
        Assert.Equal("hello", products[0].Tag);
    }

    // ── COL-8: Column-level converter checks ──────────────────────────────

    [Fact]
    public void Column_Converter_IsNull_WhenNotConfigured()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(Product));
        foreach (var col in mapping.Columns)
            Assert.Null(col.Converter);
    }

    [Fact]
    public void Column_Converter_OnlyTargetColumn_HasConverter()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var conv = new EnumToIntConverter();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(conv)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var mapping = ctx.GetMapping(typeof(Product));
        var withConverter = mapping.Columns.Where(c => c.Converter != null).ToList();
        Assert.Single(withConverter);
        Assert.Equal("Status", withConverter[0].PropName);
    }

    // ── COUNT and aggregate tests ──────────────────────────────────────────

    [Fact]
    public async Task Count_WithConverter_WorksCorrectly()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Product { Name = "A", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "B", Status = ProductStatus.Active });
        ctx.Add(new Product { Name = "C", Status = ProductStatus.Inactive });
        await ctx.SaveChangesAsync();

        var count = await ctx.Query<Product>().CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task FirstOrDefault_WithConverter_ReturnsNull_WhenEmpty()
    {
        using var cn = CreateAndOpenDb(ProductSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<ProductStatus>(p => p.Status)
                .HasConversion(new EnumToIntConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var result = await ctx.Query<Product>().FirstOrDefaultAsync();
        Assert.Null(result);
    }

    // ── Type coverage ──────────────────────────────────────────────────────

    [Fact]
    public async Task IntToString_RoundTrip_CorrectValue()
    {
        using var cn = CreateAndOpenDb(ProductTextStatusSchema);
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Product>()
                .Property<int>(p => p.Score)
                .HasConversion(new IntToStringConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var product = new Product { Name = "Widget", Score = 100 };
        ctx.Add(product);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<Product>().FirstAsync();
        Assert.Equal(100, loaded.Score);
    }
}
