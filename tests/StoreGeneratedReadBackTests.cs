using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A database-computed / default-generated NON-KEY column must be read back into the tracked entity after
/// SaveChanges, matching EF Core. nORM correctly excludes such columns from its own INSERT/UPDATE (so it
/// never clobbers the DB value), but it never hydrated the DB-generated value back — the tracked entity kept
/// its CLR default (0/null) while the row held the real value, a silent stale-read that the application can
/// propagate elsewhere.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StoreGeneratedReadBackTests
{
    [Table("SgrComputed")]
    public class ComputedRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int Total { get; set; }   // DB: GENERATED ALWAYS AS (A + B) STORED
    }

    [Table("SgrDefault")]
    public class DefaultRow
    {
        [Key] public int Id { get; set; }
        public int Counter { get; set; }   // DB DEFAULT 42
    }

    private static SqliteConnection NewDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static long RawLong(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Computed_column_read_back_after_insert()
    {
        using var cn = NewDb("CREATE TABLE SgrComputed (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Total INTEGER GENERATED ALWAYS AS (A + B) STORED);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ComputedRow>().HasKey(r => r.Id);
                mb.Entity<ComputedRow>().Property(r => r.Total).ValueGeneratedOnAddOrUpdate();
            }
        });
        var e = new ComputedRow { Id = 1, A = 3, B = 4 };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        Assert.Equal(7, RawLong(cn, "SELECT Total FROM SgrComputed WHERE Id = 1"));
        Assert.Equal(7, e.Total);   // tracked entity must reflect the computed value
    }

    [Fact]
    public async Task Computed_column_read_back_after_update()
    {
        using var cn = NewDb("CREATE TABLE SgrComputed (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Total INTEGER GENERATED ALWAYS AS (A + B) STORED);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ComputedRow>().HasKey(r => r.Id);
                mb.Entity<ComputedRow>().Property(r => r.Total).ValueGeneratedOnAddOrUpdate();
            }
        });
        var e = new ComputedRow { Id = 1, A = 3, B = 4 };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        e.A = 10;
        await ctx.SaveChangesAsync();

        Assert.Equal(14, RawLong(cn, "SELECT Total FROM SgrComputed WHERE Id = 1"));
        Assert.Equal(14, e.Total);
    }

    [Fact]
    public async Task Default_column_read_back_after_insert()
    {
        using var cn = NewDb("CREATE TABLE SgrDefault (Id INTEGER PRIMARY KEY, Counter INTEGER NOT NULL DEFAULT 42);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DefaultRow>().HasKey(r => r.Id);
                mb.Entity<DefaultRow>().Property(r => r.Counter).ValueGeneratedOnAdd();
            }
        });
        var e = new DefaultRow { Id = 1 };
        ctx.Add(e);
        await ctx.SaveChangesAsync();

        Assert.Equal(42, RawLong(cn, "SELECT Counter FROM SgrDefault WHERE Id = 1"));
        Assert.Equal(42, e.Counter);
    }
}
