using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// Store-generated key readback across representations: a value-converter-backed generated key (a
/// strongly-typed ID) must be read back through the converter into the model type, and narrow / nullable /
/// unsigned FK widths must propagate correctly. Each scenario saves via nORM and reads the RAW stored value
/// with a plain SqliteCommand, asserting the DB holds the correct value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GeneratedKeyConverterAndWidthTests
{
    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static long? FkScalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        var v = cmd.ExecuteScalar();
        return v == null || v is DBNull ? (long?)null : Convert.ToInt64(v);
    }

    // ── Strongly-typed ID as a store-generated key ─────────────────────────────────────────────
    public readonly struct CustomerId : IEquatable<CustomerId>
    {
        public long Value { get; }
        public CustomerId(long value) => Value = value;
        public bool Equals(CustomerId other) => Value == other.Value;
        public override bool Equals(object? o) => o is CustomerId c && Equals(c);
        public override int GetHashCode() => Value.GetHashCode();
    }

    private sealed class CustomerIdConverter : ValueConverter<CustomerId, long>
    {
        public override object ConvertToProvider(CustomerId value) => value.Value;
        public override object ConvertFromProvider(long value) => new CustomerId(value);
    }

    [Table("W52g_Customer")]
    public sealed class S7Customer
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public CustomerId Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task S7_ValueConverterBackedGeneratedKey_ReadBack()
    {
        using var cn = Open("CREATE TABLE W52g_Customer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S7Customer>().HasKey(c => c.Id);
                mb.Entity<S7Customer>().Property(c => c.Id).HasConversion(new CustomerIdConverter());
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var cust = new S7Customer { Name = "Ann" };
        ctx.Add(cust);

        // SetPrimaryKey now runs the generated key back through the column's converter (ConvertFromProvider),
        // yielding the strongly-typed CustomerId; the row persists with the DB-generated value.
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0L, cust.Id.Value);
        Assert.Equal(cust.Id.Value, FkScalar(cn, "SELECT Id FROM W52g_Customer WHERE Name='Ann'"));
    }

    // ── nullable INT FK, INT parent PK (matching underlying) collection nav ─────────────────────
    [Table("W52h_Parent")]
    public sealed class S8Parent
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<S8Child> Children { get; set; } = new();
    }

    [Table("W52h_Child")]
    public sealed class S8Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? ParentId { get; set; }              // nullable FK, int parent PK
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S8_NullableIntFk_IntParentPk_CollectionNav()
    {
        using var cn = Open(
            "CREATE TABLE W52h_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52h_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NULL, Tag TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S8Parent>().HasKey(p => p.Id);
                mb.Entity<S8Child>().HasKey(c => c.Id);
                mb.Entity<S8Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId!);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = new S8Parent { Name = "P" };
        parent.Children.Add(new S8Child { Tag = "c8" });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        Assert.Equal((long)parent.Id, FkScalar(cn, "SELECT ParentId FROM W52h_Child WHERE Tag='c8'"));
    }

    // ── BYTE store-generated parent PK + BYTE child FK ─────────────────────────────────────────
    [Table("W52i_Parent")]
    public sealed class S9Parent
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public byte Id { get; set; }
        public string Name { get; set; } = "";
        public List<S9Child> Children { get; set; } = new();
    }

    [Table("W52i_Child")]
    public sealed class S9Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public byte ParentId { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S9_ByteParentPk_ByteChildFk_CollectionNav()
    {
        using var cn = Open(
            "CREATE TABLE W52i_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52i_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S9Parent>().HasKey(p => p.Id);
                mb.Entity<S9Child>().HasKey(c => c.Id);
                mb.Entity<S9Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = new S9Parent { Name = "P" };
        parent.Children.Add(new S9Child { Tag = "c9" });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        Assert.Equal((long)parent.Id, FkScalar(cn, "SELECT ParentId FROM W52i_Child WHERE Tag='c9'"));
    }

    // ── m2m where LEFT PK is LONG and RIGHT PK is INT, both existing, remove-then-readd delta ───
    [Table("W52j_Author")]
    public sealed class S10Author
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public long Id { get; set; }
        public string Name { get; set; } = "";
        public List<S10Book> Books { get; set; } = new();
    }

    [Table("W52j_Book")]
    public sealed class S10Book
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Fact]
    public async Task S10_M2M_LongLeftPk_IntRightPk_Delta()
    {
        using var cn = Open(
            "CREATE TABLE W52j_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52j_Book (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
            "CREATE TABLE W52j_AuthorBook (AuthorId INTEGER NOT NULL, BookId INTEGER NOT NULL, PRIMARY KEY(AuthorId, BookId));");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S10Author>().HasKey(a => a.Id);
                mb.Entity<S10Book>().HasKey(b => b.Id);
                mb.Entity<S10Author>().HasMany(a => a.Books).WithMany()
                    .UsingTable("W52j_AuthorBook", "AuthorId", "BookId");
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var author = new S10Author { Name = "A" };
        var b1 = new S10Book { Title = "b1" };
        var b2 = new S10Book { Title = "b2" };
        author.Books.Add(b1);
        author.Books.Add(b2);
        ctx.Add(author);
        await ctx.SaveChangesAsync();

        // Now remove b1 and save again — delta must DELETE exactly the (author,b1) row and keep (author,b2).
        author.Books.Remove(b1);
        await ctx.SaveChangesAsync();

        var remaining = FkScalar(cn, "SELECT COUNT(*) FROM W52j_AuthorBook");
        var b2Rows = FkScalar(cn, $"SELECT COUNT(*) FROM W52j_AuthorBook WHERE BookId={b2.Id}");
        var b1Rows = FkScalar(cn, $"SELECT COUNT(*) FROM W52j_AuthorBook WHERE BookId={b1.Id}");
        Assert.Equal(1L, remaining);
        Assert.Equal(1L, b2Rows);
        Assert.Equal(0L, b1Rows);
    }
}
