using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The direct active-record write path (InsertAsync/UpdateAsync) must hydrate computed / store-generated
/// non-key columns back onto the entity after the write, exactly as the batched SaveChanges path does
/// (StoreGeneratedReadBackTests). Without it the DB row is correct but the in-memory object is left stale
/// (0), a silent stale-read the application can propagate.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DirectWriteStoreGeneratedReadBackTests
{
    [Table("PfComputed")]
    public class ComputedRow { [Key] public int Id { get; set; } public int A { get; set; } public int B { get; set; } public int Total { get; set; } }

    [Table("PfDefault")]
    public class DefaultRow { [Key] public int Id { get; set; } public int Counter { get; set; } }

    private static SqliteConnection NewDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c = cn.CreateCommand();
        c.CommandText = ddl;
        c.ExecuteNonQuery();
        return cn;
    }

    private static long RawLong(SqliteConnection cn, string sql)
    {
        using var c = cn.CreateCommand();
        c.CommandText = sql;
        return (long)c.ExecuteScalar()!;
    }

    private static DbContext MakeCtx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<ComputedRow>().HasKey(r => r.Id);
            mb.Entity<ComputedRow>().Property(r => r.Total).ValueGeneratedOnAddOrUpdate();
            mb.Entity<DefaultRow>().HasKey(r => r.Id);
            mb.Entity<DefaultRow>().Property(r => r.Counter).ValueGeneratedOnAdd();
        }
    });

    [Fact]
    public async Task Direct_InsertAsync_hydrates_computed_column()
    {
        using var cn = NewDb("CREATE TABLE PfComputed (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Total INTEGER GENERATED ALWAYS AS (A + B) STORED);");
        using var ctx = MakeCtx(cn);
        var e = new ComputedRow { Id = 1, A = 3, B = 4 };
        await ctx.InsertAsync(e);
        Assert.Equal(7, RawLong(cn, "SELECT Total FROM PfComputed WHERE Id = 1")); // DB stored correctly
        Assert.Equal(7, e.Total);   // in-memory hydrated
    }

    [Fact]
    public async Task Direct_UpdateAsync_hydrates_computed_column()
    {
        using var cn = NewDb("CREATE TABLE PfComputed (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Total INTEGER GENERATED ALWAYS AS (A + B) STORED);");
        using var ctx = MakeCtx(cn);
        var e = new ComputedRow { Id = 1, A = 3, B = 4 };
        await ctx.InsertAsync(e);
        e.A = 10;
        await ctx.UpdateAsync(e);
        Assert.Equal(14, RawLong(cn, "SELECT Total FROM PfComputed WHERE Id = 1"));
        Assert.Equal(14, e.Total);
    }

    [Fact]
    public async Task Direct_InsertAsync_hydrates_db_default_column()
    {
        using var cn = NewDb("CREATE TABLE PfDefault (Id INTEGER PRIMARY KEY, Counter INTEGER NOT NULL DEFAULT 42);");
        using var ctx = MakeCtx(cn);
        var e = new DefaultRow { Id = 1 };
        await ctx.InsertAsync(e);
        Assert.Equal(42, RawLong(cn, "SELECT Counter FROM PfDefault WHERE Id = 1"));
        Assert.Equal(42, e.Counter);
    }
}
