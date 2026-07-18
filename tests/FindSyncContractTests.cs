using System;
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
/// Pins the synchronous DbContext.Find&lt;T&gt; — the EF-parity primary-key lookup mirroring FindAsync:
/// identity-map-first (a tracked entity returns the SAME instance with no round trip), DB fallback that
/// tracks the result, null for a miss, coercion of the supplied key to the key CLR type, composite keys,
/// and fail-loud on the wrong number of key values or a null component. Sync and async agree.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FindSyncContractTests
{
    [Table("FsRow")]
    public class Row
    {
        [Key] public long Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("FsComposite")]
    public class Composite
    {
        [Key, Column(Order = 0)] public int TenantId { get; set; }
        [Key, Column(Order = 1)] public int LocalId { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FsRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE FsComposite (TenantId INTEGER NOT NULL, LocalId INTEGER NOT NULL, Label TEXT NOT NULL, PRIMARY KEY (TenantId, LocalId));
                INSERT INTO FsRow VALUES (1, 'one'), (2, 'two');
                INSERT INTO FsComposite VALUES (10, 100, 'a'), (20, 100, 'b');
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
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Find_loads_from_database_and_tracks()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var one = ctx.Find<Row>(1L);
        Assert.NotNull(one);
        Assert.Equal("one", one!.Name);

        // Now tracked: a second Find returns the SAME instance with no round trip.
        var oneAgain = ctx.Find<Row>(1L);
        Assert.Same(one, oneAgain);
    }

    [Fact]
    public void Find_returns_already_tracked_instance_without_query()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var queried = ctx.Query<Row>().First(r => r.Id == 2L);
        var found = ctx.Find<Row>(2L);
        Assert.Same(queried, found);
    }

    [Fact]
    public void Find_returns_null_for_a_missing_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        Assert.Null(ctx.Find<Row>(999L));
    }

    [Fact]
    public void Find_coerces_the_supplied_key_to_the_key_type()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        // Supplied as int; the key is long. Coercion must match it.
        var one = ctx.Find<Row>(1);
        Assert.NotNull(one);
        Assert.Equal("one", one!.Name);
    }

    [Fact]
    public void Find_resolves_a_composite_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var b = ctx.Find<Composite>(20, 100);
        Assert.NotNull(b);
        Assert.Equal("b", b!.Label);
    }

    [Fact]
    public void Find_throws_on_wrong_key_arity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        Assert.Throws<NormUsageException>(() => ctx.Find<Composite>(20));
    }

    [Fact]
    public void Find_throws_on_a_null_key_component()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        Assert.Throws<NormUsageException>(() => ctx.Find<Row>(new object?[] { null }));
    }

    [Fact]
    public async Task Find_and_FindAsync_agree()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = Bootstrap(cn);

        var sync = ctx.Find<Row>(1L);
        var async = await ctx.FindAsync<Row>(1L);
        Assert.Same(sync, async);   // both resolve to the one tracked instance
    }
}
