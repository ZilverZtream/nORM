using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins DbContext.FindAsync — the EF-parity primary-key lookup: identity-map-first (a tracked
/// entity returns the SAME instance with no round trip), DB fallback that tracks the result,
/// null for a miss, coercion of the supplied key to the key CLR type, composite keys, and
/// fail-loud on the wrong number of key values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FindAsyncContractTests
{
    [Table("FaRow")]
    public class Row
    {
        [Key] public long Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("FaComposite")]
    public class Composite
    {
        [Key, Column(Order = 0)] public int TenantId { get; set; }
        [Key, Column(Order = 1)] public int LocalId { get; set; }
        public string Label { get; set; } = "";
    }

    private static async Task<DbContext> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FaRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE FaComposite (TenantId INTEGER NOT NULL, LocalId INTEGER NOT NULL, Label TEXT NOT NULL, PRIMARY KEY (TenantId, LocalId));
                INSERT INTO FaRow VALUES (1, 'one');
                INSERT INTO FaRow VALUES (2, 'two');
                INSERT INTO FaComposite VALUES (10, 100, 'a');
                INSERT INTO FaComposite VALUES (20, 100, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Row>().HasKey(r => r.Id);
                mb.Entity<Composite>().HasKey(c => new { c.TenantId, c.LocalId });
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return ctx;
    }

    [Fact]
    public async Task FindAsync_loads_from_database_and_tracks()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        var one = await ctx.FindAsync<Row>(1L);
        Assert.NotNull(one);
        Assert.Equal("one", one!.Name);

        // Now tracked: a second FindAsync returns the SAME instance with no round trip.
        var oneAgain = await ctx.FindAsync<Row>(1L);
        Assert.Same(one, oneAgain);
    }

    [Fact]
    public async Task FindAsync_returns_null_for_a_missing_key()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        Assert.Null(await ctx.FindAsync<Row>(999L));
    }

    [Fact]
    public async Task FindAsync_coerces_the_supplied_key_type()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // Key is long; caller passes an int. EF coerces, so does nORM.
        var two = await ctx.FindAsync<Row>(2);
        Assert.NotNull(two);
        Assert.Equal("two", two!.Name);
    }

    [Fact]
    public async Task FindAsync_returns_distinct_rows_for_distinct_keys()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // Exercises the parameterized key predicate across two distinct keys.
        Assert.Equal("one", (await ctx.FindAsync<Row>(1L))!.Name);
        Assert.Equal("two", (await ctx.FindAsync<Row>(2L))!.Name);
    }

    [Fact]
    public async Task FindAsync_supports_composite_keys()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        var a = await ctx.FindAsync<Composite>(10, 100);
        var b = await ctx.FindAsync<Composite>(20, 100);
        Assert.Equal("a", a!.Label);
        Assert.Equal("b", b!.Label);
        Assert.Null(await ctx.FindAsync<Composite>(30, 100));

        // Tracked composite entity short-circuits by key too.
        Assert.Same(a, await ctx.FindAsync<Composite>(10, 100));
    }

    [Fact]
    public async Task FindAsync_rejects_wrong_key_arity()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        await Assert.ThrowsAsync<NormUsageException>(async () => await ctx.FindAsync<Composite>(10));
        await Assert.ThrowsAsync<NormUsageException>(async () => await ctx.FindAsync<Row>(1L, 2L));
    }
}
