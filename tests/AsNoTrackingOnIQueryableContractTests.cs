using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that AsNoTracking (and AsSplitQuery) compose off the plain IQueryable&lt;T&gt; returned by
/// Query&lt;T&gt;()/Set&lt;T&gt;() and off standard operators WITHOUT the (INormQueryable&lt;T&gt;)
/// cast — the same front-door fix as Include. AsNoTracking must also actually suppress tracking.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsNoTrackingOnIQueryableContractTests
{
    [Table("AntRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static async Task<DbContext> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AntRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO AntRow VALUES (1, 'a'), (2, 'b');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return ctx;
    }

    [Fact]
    public async Task AsNoTracking_composes_without_a_cast_and_suppresses_tracking()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // No (INormQueryable<Row>) cast: directly off Query<T>(), and after Where.
        var all = await ctx.Query<Row>().AsNoTracking().ToListAsync();
        Assert.Equal(2, all.Count);

        var filtered = await ctx.Query<Row>().Where(r => r.Id == 1).AsNoTracking().ToListAsync();
        var one = Assert.Single(filtered);
        Assert.Equal("a", one.Name);

        // Untracked: the entry is not in the change tracker, so editing it and SaveChanges is a no-op.
        one.Name = "edited";
        await ctx.SaveChangesAsync();
        var reread = await ctx.Query<Row>().AsNoTracking().Where(r => r.Id == 1).ToListAsync();
        Assert.Equal("a", Assert.Single(reread).Name);
    }

    [Fact]
    public async Task AsSplitQuery_composes_without_a_cast()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // AsSplitQuery is a no-op accepted for EF parity — it just has to compile and run.
        var rows = await ctx.Set<Row>().AsSplitQuery().Where(r => r.Id >= 1).ToListAsync();
        Assert.Equal(2, rows.Count);
    }
}
