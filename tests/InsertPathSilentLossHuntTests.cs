using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial INSERT write-path hunt: for every scenario we INSERT via nORM, then read back the
/// RAW stored value with a plain SqliteCommand and assert the in-memory entity graph is correct.
/// Any assertion that fails is a candidate silent-data-loss / corruption finding.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class InsertPathSilentLossHuntTests
{
    // ── helpers ───────────────────────────────────────────────────────────────
    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static object? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static List<object?[]> Rows(SqliteConnection cn, string sql, int cols)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var list = new List<object?[]>();
        while (r.Read())
        {
            var a = new object?[cols];
            for (int i = 0; i < cols; i++) a[i] = r.IsDBNull(i) ? null : r.GetValue(i);
            list.Add(a);
        }
        return list;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Enum stored as string via HasConversion
    // ══════════════════════════════════════════════════════════════════════════
    public enum Colour { Red = 0, Green = 1, Blue = 2 }

    [Table("EnumStr")]
    public sealed class EnumStrEntity
    {
        [Key] public int Id { get; set; }
        public Colour Colour { get; set; }
    }

    private sealed class ColourToStringConverter : ValueConverter<Colour, string>
    {
        public override object? ConvertToProvider(Colour value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Colour>(value);
    }

    [Fact]
    public async Task EnumAsString_Insert_RawStoredIsEnumName()
    {
        using var cn = Open("CREATE TABLE EnumStr (Id INTEGER PRIMARY KEY, Colour TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<EnumStrEntity>()
                .Property(e => e.Colour).HasConversion(new ColourToStringConverter())
        });
        ctx.Add(new EnumStrEntity { Id = 1, Colour = Colour.Blue });
        await ctx.SaveChangesAsync();

        Assert.Equal("Blue", Scalar(cn, "SELECT Colour FROM EnumStr WHERE Id=1"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // High-precision decimal (no converter) round-trip
    // ══════════════════════════════════════════════════════════════════════════
    [Table("DecPrec")]
    public sealed class DecEntity
    {
        [Key] public int Id { get; set; }
        public decimal Value { get; set; }
    }

    [Fact]
    public async Task Decimal_HighPrecision_Insert_RawPreservesValue()
    {
        using var cn = Open("CREATE TABLE DecPrec (Id INTEGER PRIMARY KEY, Value TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var v = 0.1234567890123456789012345678m; // 28 significant fractional digits
        ctx.Add(new DecEntity { Id = 1, Value = v });
        await ctx.SaveChangesAsync();

        var raw = (string)Scalar(cn, "SELECT Value FROM DecPrec WHERE Id=1")!;
        Assert.Equal(v, decimal.Parse(raw, NumberStyles.Number, CultureInfo.InvariantCulture));

        var reread = await ctx.Query<DecEntity>().Where(e => e.Id == 1).ToListAsync();
        Assert.Equal(v, reread[0].Value);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DateTimeOffset offset preservation on insert
    // ══════════════════════════════════════════════════════════════════════════
    [Table("DtoIns")]
    public sealed class DtoEntity
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset When { get; set; }
    }

    [Fact]
    public async Task DateTimeOffset_Insert_PreservesOffsetAndInstant()
    {
        using var cn = Open("CREATE TABLE DtoIns (Id INTEGER PRIMARY KEY, `When` TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var when = new DateTimeOffset(2023, 1, 15, 10, 30, 45, TimeSpan.FromMinutes(330)); // +05:30
        ctx.Add(new DtoEntity { Id = 1, When = when });
        await ctx.SaveChangesAsync();

        var raw = (string)Scalar(cn, "SELECT `When` FROM DtoIns WHERE Id=1")!;
        var parsed = DateTimeOffset.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.None);
        Assert.Equal(when.Offset, parsed.Offset);       // offset preserved in storage
        Assert.Equal(when.UtcDateTime, parsed.UtcDateTime);

        var reread = (await ctx.Query<DtoEntity>().Where(e => e.Id == 1).ToListAsync())[0];
        Assert.Equal(when.Offset, reread.When.Offset);  // offset preserved on read-back
        Assert.Equal(when.UtcDateTime, reread.When.UtcDateTime);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // TimeSpan multi-day + sub-second
    // ══════════════════════════════════════════════════════════════════════════
    [Table("TsIns")]
    public sealed class TsEntity
    {
        [Key] public int Id { get; set; }
        public TimeSpan Span { get; set; }
    }

    [Fact]
    public async Task TimeSpan_MultiDaySubSecond_Insert_RoundTrips()
    {
        using var cn = Open("CREATE TABLE TsIns (Id INTEGER PRIMARY KEY, Span TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var span = new TimeSpan(3, 5, 30, 15, 123) + TimeSpan.FromTicks(4567); // 3d + sub-ms
        ctx.Add(new TsEntity { Id = 1, Span = span });
        await ctx.SaveChangesAsync();

        var reread = (await ctx.Query<TsEntity>().Where(e => e.Id == 1).ToListAsync())[0];
        Assert.Equal(span, reread.Span);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // TimeOnly sub-second
    // ══════════════════════════════════════════════════════════════════════════
    [Table("ToIns")]
    public sealed class ToEntity
    {
        [Key] public int Id { get; set; }
        public TimeOnly T { get; set; }
    }

    [Fact]
    public async Task TimeOnly_SubSecond_Insert_RoundTrips()
    {
        using var cn = Open("CREATE TABLE ToIns (Id INTEGER PRIMARY KEY, T TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var t = new TimeOnly(10, 30, 45, 123).Add(TimeSpan.FromTicks(4567));
        ctx.Add(new ToEntity { Id = 1, T = t });
        await ctx.SaveChangesAsync();

        var reread = (await ctx.Query<ToEntity>().Where(e => e.Id == 1).ToListAsync())[0];
        Assert.Equal(t, reread.T);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Store-generated convention key: multiple in one save get distinct real keys
    // ══════════════════════════════════════════════════════════════════════════
    [Table("MultiKey")]
    public sealed class MultiKeyEntity
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task ConventionKey_ThreeInserts_DistinctKeysMatchStoredRows()
    {
        using var cn = Open("CREATE TABLE MultiKey (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var a = new MultiKeyEntity { Name = "a" };
        var b = new MultiKeyEntity { Name = "b" };
        var c = new MultiKeyEntity { Name = "c" };
        ctx.Add(a); ctx.Add(b); ctx.Add(c);
        await ctx.SaveChangesAsync();

        Assert.True(a.Id > 0 && b.Id > 0 && c.Id > 0);
        Assert.Equal(3, new[] { a.Id, b.Id, c.Id }.Distinct().Count());

        // Each stored row's Name matches the key the entity was assigned.
        Assert.Equal("a", Scalar(cn, $"SELECT Name FROM MultiKey WHERE Id={a.Id}"));
        Assert.Equal("b", Scalar(cn, $"SELECT Name FROM MultiKey WHERE Id={b.Id}"));
        Assert.Equal("c", Scalar(cn, $"SELECT Name FROM MultiKey WHERE Id={c.Id}"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Parent + children (both new, DB-generated keys) → child FK == parent generated key (RAW)
    // ══════════════════════════════════════════════════════════════════════════
    [Table("HuntBlog")]
    public sealed class Blog
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("HuntPost")]
    public sealed class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public string Text { get; set; } = "";
        public Blog Blog { get; set; } = default!;
    }

    private static DbContext BlogCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Blog>().HasKey(b => b.Id);
            mb.Entity<Post>().HasKey(p => p.Id);
            mb.Entity<Blog>().HasMany(b => b.Posts).WithOne(p => p.Blog).HasForeignKey(p => p.BlogId, b => b.Id);
        }
    });

    [Fact]
    public async Task ParentChildGraph_ChildRawFkEqualsGeneratedParentKey()
    {
        using var cn = Open(
            "CREATE TABLE HuntBlog (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
            "CREATE TABLE HuntPost (Id INTEGER PRIMARY KEY AUTOINCREMENT, BlogId INTEGER NOT NULL, Text TEXT NOT NULL);");
        await using var ctx = BlogCtx(cn);
        var blog = new Blog { Title = "b" };
        blog.Posts.Add(new Post { Text = "p1" });
        blog.Posts.Add(new Post { Text = "p2" });
        ctx.Add(blog);
        await ctx.SaveChangesAsync();

        Assert.True(blog.Id > 0);
        var raw = Rows(cn, "SELECT BlogId FROM HuntPost ORDER BY Id", 1);
        Assert.Equal(2, raw.Count);
        Assert.All(raw, r => Assert.Equal((long)blog.Id, Convert.ToInt64(r[0])));
        Assert.All(blog.Posts, p => Assert.Equal(blog.Id, p.BlogId)); // in-memory fixup
    }

    // ══════════════════════════════════════════════════════════════════════════
    // OwnsOne — owned columns written; null owned reference
    // ══════════════════════════════════════════════════════════════════════════
    [Owned]
    public sealed class Address
    {
        public string Street { get; set; } = "";
        public string City { get; set; } = "";
    }

    [Table("HuntCust")]
    public sealed class Customer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public Address Home { get; set; } = default!;
    }

    private static DbContext CustCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Customer>().HasKey(c => c.Id);
            mb.Entity<Customer>().OwnsOne(c => c.Home);
        }
    });

    [Fact]
    public async Task OwnsOne_Insert_WritesOwnedColumns()
    {
        using var cn = Open("CREATE TABLE HuntCust (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Home_Street TEXT, Home_City TEXT);");
        await using var ctx = CustCtx(cn);
        ctx.Add(new Customer { Id = 1, Name = "n", Home = new Address { Street = "Main", City = "Metropolis" } });
        await ctx.SaveChangesAsync();

        Assert.Equal("Main", Scalar(cn, "SELECT Home_Street FROM HuntCust WHERE Id=1"));
        Assert.Equal("Metropolis", Scalar(cn, "SELECT Home_City FROM HuntCust WHERE Id=1"));
    }

    [Fact]
    public async Task OwnsOne_NullOwnedRef_Insert_WritesNullColumns()
    {
        using var cn = Open("CREATE TABLE HuntCust (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Home_Street TEXT, Home_City TEXT);");
        await using var ctx = CustCtx(cn);
        ctx.Add(new Customer { Id = 1, Name = "n", Home = null! });
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.Null(ex);
        // Rows() maps SQL NULL to C# null, empty-string stays "" — so this distinguishes them.
        var raw = Rows(cn, "SELECT Home_Street, Home_City FROM HuntCust WHERE Id=1", 2)[0];
        Assert.Null(raw[0]); // a null owned reference must store NULL, not ""
        Assert.Null(raw[1]);

        // And a re-query must resolve Home back to null, not a phantom non-null empty Address.
        var reread = (await ctx.Query<Customer>().Where(c => c.Id == 1).ToListAsync())[0];
        Assert.Null(reread.Home);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Many-to-many: new right entity with convention key → correct join both-side keys
    // ══════════════════════════════════════════════════════════════════════════
    [Table("HuntDoc")]
    public sealed class Doc
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Label> Labels { get; set; } = new();
    }

    [Table("HuntLabel")]
    public sealed class Label
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext DocCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
            mb.Entity<Doc>().HasMany(d => d.Labels).WithMany().UsingTable("HuntDocLabel", "DocId", "LabelId")
    });

    [Fact]
    public async Task M2M_NewOwnerAndNewRight_ConventionKeys_JoinRowHasBothGeneratedKeys()
    {
        using var cn = Open(
            "CREATE TABLE HuntDoc (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
            "CREATE TABLE HuntLabel (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE HuntDocLabel (DocId INTEGER NOT NULL, LabelId INTEGER NOT NULL);");
        await using var ctx = DocCtx(cn);
        var doc = new Doc { Title = "d" };
        var l1 = new Label { Name = "l1" };
        var l2 = new Label { Name = "l2" };
        doc.Labels.Add(l1);
        doc.Labels.Add(l2);
        ctx.Add(doc);
        ctx.Add(l1);
        ctx.Add(l2);
        await ctx.SaveChangesAsync();

        Assert.True(doc.Id > 0 && l1.Id > 0 && l2.Id > 0);
        var join = Rows(cn, "SELECT DocId, LabelId FROM HuntDocLabel ORDER BY LabelId", 2);
        Assert.Equal(2, join.Count);
        Assert.All(join, r => Assert.Equal((long)doc.Id, Convert.ToInt64(r[0])));
        Assert.Equal(new[] { (long)l1.Id, (long)l2.Id }.OrderBy(x => x).ToArray(),
            join.Select(r => Convert.ToInt64(r[1])).OrderBy(x => x).ToArray());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Identity map: add the same instance twice → exactly one row
    // ══════════════════════════════════════════════════════════════════════════
    [Fact]
    public async Task IdentityMap_AddSameInstanceTwice_OneRow()
    {
        using var cn = Open("CREATE TABLE MultiKey (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var e = new MultiKeyEntity { Name = "dup" };
        ctx.Add(e);
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, Convert.ToInt64(Scalar(cn, "SELECT COUNT(*) FROM MultiKey")));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DB DEFAULT column the app leaves at CLR default (nullable, no ValueGeneratedOnAdd)
    // ══════════════════════════════════════════════════════════════════════════
    [Table("DefCol")]
    public sealed class DefEntity
    {
        [Key] public int Id { get; set; }
        public string? Status { get; set; }
    }

    [Fact]
    public async Task DbDefault_NullableColumnLeftNull_ReflectsDatabaseDefault()
    {
        using var cn = Open("CREATE TABLE DefCol (Id INTEGER PRIMARY KEY, Status TEXT DEFAULT 'active');");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new DefEntity { Id = 1, Status = null });
        await ctx.SaveChangesAsync();

        // Documented behavior (EF-consistent for an UNCONFIGURED raw DDL default): the mapped, non-store-
        // generated column is written explicitly as NULL, so the DB DEFAULT is NOT applied. Honoring a raw
        // DDL default nORM was never told about would require omitting a column it has no default metadata
        // for. (The HasDefaultValueSql DDL-only-vs-EF-runtime-default gap is a separately recorded pending
        // decision.) This asserts the ACTUAL behavior so the file stays green except for genuine findings.
        var raw = Rows(cn, "SELECT Status FROM DefCol WHERE Id=1", 1)[0];
        Assert.Null(raw[0]);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ValueGeneratedOnAdd non-key column with DB default: omitted from insert, read back
    // ══════════════════════════════════════════════════════════════════════════
    [Table("VgoaCol")]
    public sealed class VgoaEntity
    {
        [Key] public int Id { get; set; }
        public int Seq { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task ValueGeneratedOnAdd_NonKeyColumn_HydratesDbDefaultAfterInsert()
    {
        using var cn = Open("CREATE TABLE VgoaCol (Id INTEGER PRIMARY KEY, Seq INTEGER NOT NULL DEFAULT 77, Name TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<VgoaEntity>().Property(e => e.Seq).ValueGeneratedOnAdd()
        });
        var e = new VgoaEntity { Id = 1, Name = "n" }; // Seq left unset (0)
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        Assert.Equal(77L, Convert.ToInt64(Scalar(cn, "SELECT Seq FROM VgoaCol WHERE Id=1"))); // DB got its default
        Assert.Equal(77, e.Seq); // entity hydrated to the DB default
    }

    // ══════════════════════════════════════════════════════════════════════════
    // OwnsOne owned property carries a value converter (enum → string)
    // ══════════════════════════════════════════════════════════════════════════
    [Owned]
    public sealed class Palette
    {
        public Colour Colour { get; set; }
    }

    [Table("HuntPaint")]
    public sealed class Paint
    {
        [Key] public int Id { get; set; }
        public Palette Swatch { get; set; } = new();
    }

    [Fact]
    public async Task OwnsOne_OwnedPropertyConverter_Insert_RawIsProviderForm()
    {
        using var cn = Open("CREATE TABLE HuntPaint (Id INTEGER PRIMARY KEY, Swatch_Colour TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Paint>()
                .OwnsOne(p => p.Swatch, b => b.Property(x => x.Colour).HasConversion(new ColourToStringConverter()))
        });
        ctx.Add(new Paint { Id = 1, Swatch = new Palette { Colour = Colour.Blue } });
        await ctx.SaveChangesAsync();

        // The owned column's converter must be applied on write → stored TEXT is the enum NAME, not "2".
        Assert.Equal("Blue", Scalar(cn, "SELECT Swatch_Colour FROM HuntPaint WHERE Id=1"));

        var reread = (await ctx.Query<Paint>().Where(p => p.Id == 1).ToListAsync())[0];
        Assert.Equal(Colour.Blue, reread.Swatch.Colour);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // OwnsOne owned decimal column preserves high precision
    // ══════════════════════════════════════════════════════════════════════════
    [Owned]
    public sealed class Money
    {
        public decimal Amount { get; set; }
    }

    [Table("HuntPrice")]
    public sealed class Priced
    {
        [Key] public int Id { get; set; }
        public Money Cost { get; set; } = new();
    }

    [Fact]
    public async Task OwnsOne_OwnedDecimalColumn_Insert_PreservesPrecision()
    {
        using var cn = Open("CREATE TABLE HuntPrice (Id INTEGER PRIMARY KEY, Cost_Amount TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Priced>().OwnsOne(p => p.Cost)
        });
        var amount = 12345.678901234567890123456m;
        ctx.Add(new Priced { Id = 1, Cost = new Money { Amount = amount } });
        await ctx.SaveChangesAsync();

        var raw = (string)Scalar(cn, "SELECT Cost_Amount FROM HuntPrice WHERE Id=1")!;
        Assert.Equal(amount, decimal.Parse(raw, NumberStyles.Number, CultureInfo.InvariantCulture));
        var reread = (await ctx.Query<Priced>().Where(p => p.Id == 1).ToListAsync())[0];
        Assert.Equal(amount, reread.Cost.Amount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Converter DateTime → long ticks: full-precision provider value on insert
    // ══════════════════════════════════════════════════════════════════════════
    private sealed class DateTimeToTicksConverter : ValueConverter<DateTime, long>
    {
        public override object? ConvertToProvider(DateTime value) => value.Ticks;
        public override object? ConvertFromProvider(long value) => new DateTime(value);
    }

    [Table("HuntTick")]
    public sealed class TickEntity
    {
        [Key] public int Id { get; set; }
        public DateTime When { get; set; }
    }

    [Fact]
    public async Task Converter_DateTimeToTicks_Insert_RawIsExactTicks()
    {
        using var cn = Open("CREATE TABLE HuntTick (Id INTEGER PRIMARY KEY, `When` INTEGER NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TickEntity>()
                .Property(e => e.When).HasConversion(new DateTimeToTicksConverter())
        });
        var when = new DateTime(2023, 7, 14, 12, 34, 56).AddTicks(1234567);
        ctx.Add(new TickEntity { Id = 1, When = when });
        await ctx.SaveChangesAsync();

        Assert.Equal(when.Ticks, Convert.ToInt64(Scalar(cn, "SELECT `When` FROM HuntTick WHERE Id=1")));
        var reread = (await ctx.Query<TickEntity>().Where(e => e.Id == 1).ToListAsync())[0];
        Assert.Equal(when, reread.When);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DateOnly round-trips on insert
    // ══════════════════════════════════════════════════════════════════════════
    [Table("HuntDate")]
    public sealed class DateEntity
    {
        [Key] public int Id { get; set; }
        public DateOnly D { get; set; }
    }

    [Fact]
    public async Task DateOnly_Insert_RoundTrips()
    {
        using var cn = Open("CREATE TABLE HuntDate (Id INTEGER PRIMARY KEY, D TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());
        var d = new DateOnly(2024, 2, 29); // leap day
        ctx.Add(new DateEntity { Id = 1, D = d });
        await ctx.SaveChangesAsync();

        Assert.Equal("2024-02-29", Scalar(cn, "SELECT D FROM HuntDate WHERE Id=1"));
        var reread = (await ctx.Query<DateEntity>().Where(e => e.Id == 1).ToListAsync())[0];
        Assert.Equal(d, reread.D);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Guid PK explicit + Guid reference-nav FK fixup, raw canonical text
    // ══════════════════════════════════════════════════════════════════════════
    [Table("HuntGParent")]
    public sealed class GParent
    {
        [Key] public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public List<GChild> Children { get; set; } = new();
    }

    [Table("HuntGChild")]
    public sealed class GChild
    {
        [Key] public Guid Id { get; set; }
        public Guid GParentId { get; set; }
        public string Name { get; set; } = "";
        public GParent Parent { get; set; } = default!;
    }

    [Fact]
    public async Task GuidPk_ParentChild_ChildRawFkIsParentGuidCanonicalText()
    {
        using var cn = Open(
            "CREATE TABLE HuntGParent (Id TEXT PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE HuntGChild (Id TEXT PRIMARY KEY, GParentId TEXT NOT NULL, Name TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<GParent>().HasKey(p => p.Id);
                mb.Entity<GChild>().HasKey(c => c.Id);
                mb.Entity<GParent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.GParentId, p => p.Id);
            }
        });
        var pid = Guid.NewGuid();
        var parent = new GParent { Id = pid, Name = "p" };
        parent.Children.Add(new GChild { Id = Guid.NewGuid(), Name = "c1" });
        parent.Children.Add(new GChild { Id = Guid.NewGuid(), Name = "c2" });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        var raw = Rows(cn, "SELECT GParentId FROM HuntGChild ORDER BY Name", 1);
        Assert.Equal(2, raw.Count);
        Assert.All(raw, r => Assert.Equal(pid.ToString("D"), (string)r[0]!));
        Assert.All(parent.Children, c => Assert.Equal(pid, c.GParentId));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Caller-managed transaction rollback: nothing persisted; generated key reset
    // ══════════════════════════════════════════════════════════════════════════
    [Fact]
    public async Task CallerTransactionRollback_NothingPersisted_AndKeyResetForRetry()
    {
        using var cn = Open("CREATE TABLE MultiKey (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var e = new MultiKeyEntity { Name = "rolled" };
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(e);
            await ctx.SaveChangesAsync();
            Assert.True(e.Id > 0);
            await tx.RollbackAsync();
        }

        // Nothing left in the table.
        Assert.Equal(0L, Convert.ToInt64(Scalar(cn, "SELECT COUNT(*) FROM MultiKey")));

        // Re-saving the same entity in a fresh committed transaction must actually insert it,
        // not skip it as "already persisted" (which would be silent data loss).
        await using (var tx2 = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.SaveChangesAsync();
            await tx2.CommitAsync();
        }
        Assert.Equal(1L, Convert.ToInt64(Scalar(cn, "SELECT COUNT(*) FROM MultiKey")));
        Assert.Equal("rolled", Scalar(cn, $"SELECT Name FROM MultiKey WHERE Id={e.Id}"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Nested OwnsOne (owned inside owned) written on insert
    // ══════════════════════════════════════════════════════════════════════════
    [Owned]
    public sealed class GeoPoint
    {
        public double Lat { get; set; }
        public double Lng { get; set; }
    }

    [Owned]
    public sealed class Location
    {
        public string City { get; set; } = "";
        public GeoPoint Point { get; set; } = new();
    }

    [Table("HuntPlace")]
    public sealed class Place
    {
        [Key] public int Id { get; set; }
        public Location Where { get; set; } = new();
    }

    [Fact]
    public async Task NestedOwnsOne_Insert_CurrentlyFailsLoud_NotSilent()
    {
        // Secondary finding (FAIL-LOUD, not silent): nested OwnsOne is not flattened recursively — the
        // inner owned reference (Where.Point) is emitted as a single bogus scalar column "Where_Point"
        // instead of "Where_Point_Lat"/"Where_Point_Lng". This throws loudly rather than losing data
        // silently, so it is an unsupported-feature gap, not a data-loss finding. Asserting the current
        // fail-loud behavior keeps this file's only RED test the genuine silent-loss repro.
        using var cn = Open("CREATE TABLE HuntPlace (Id INTEGER PRIMARY KEY, Where_City TEXT NOT NULL, Where_Point_Lat REAL NOT NULL, Where_Point_Lng REAL NOT NULL);");
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Place>()
                .OwnsOne(p => p.Where, b => b.OwnsOne(w => w.Point))
        });
        ctx.Add(new Place { Id = 1, Where = new Location { City = "Oslo", Point = new GeoPoint { Lat = 59.913869, Lng = 10.752245 } } });
        var ex = await Record.ExceptionAsync(() => ctx.SaveChangesAsync());
        Assert.NotNull(ex); // fails loud today (no silent corruption)
    }
}
