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
/// Probes <c>p.Children.SelectMany(c =&gt; c.GrandChildren).Count()</c> in a
/// projection. SCV's nav-aggregate emit handles single-hop shapes
/// (<c>p.Children.Count()</c>) but the multi-hop form chains a SelectMany
/// between the two navigations. If the SCV pattern doesn't recognise this,
/// the projection falls through to the generic emitter and produces wrong
/// SQL (probably <c>COUNT(*) FROM Parent</c>).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMultiHopNavAggregateInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private MhnContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MhnParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE MhnChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
            CREATE TABLE MhnGrandChild (Id INTEGER PRIMARY KEY, ChildId INTEGER NOT NULL);
            INSERT INTO MhnParent VALUES (1,'Alice'),(2,'Bob');
            -- Alice's tree: child 1 has 2 gc, child 2 has 1 gc → 3 grandchildren total.
            -- Bob's tree:   child 3 has 0 gc → 0 grandchildren.
            INSERT INTO MhnChild VALUES (1,1),(2,1),(3,2);
            INSERT INTO MhnGrandChild VALUES (1,1),(2,1),(3,2);
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<MhnParent>().HasKey(p => p.Id);
                mb.Entity<MhnChild>().HasKey(c => c.Id);
                mb.Entity<MhnParent>().HasMany<MhnChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
                mb.Entity<MhnChild>().HasMany<MhnGrandChild>(c => c.GrandChildren).WithOne().HasForeignKey(g => g.ChildId);
            }
        };
        _ctx = new MhnContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Multi_hop_nav_aggregate_in_projection_returns_correct_counts_or_throws_with_actionable_message()
    {
        // Either the translation works correctly, or it throws an actionable exception.
        // Silent-wrongness (wrong count, no exception) is the failure case this test guards.
        System.Exception? caught = null;
        System.Collections.Generic.IList<dynamic>? result = null;
        try
        {
            var rows = await _ctx.Query<MhnParent>()
                .Select(p => new
                {
                    p.Name,
                    GcCount = p.Children.SelectMany(c => c.GrandChildren).Count()
                })
                .ToListAsync();
            result = rows.Cast<dynamic>().ToList();
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        // Make the actual code path visible in the assertion message.
        if (caught != null)
        {
            Assert.IsType<nORM.Core.NormUnsupportedFeatureException>(caught);
            // Must specifically call out the multi-hop nav chain and either name a working
            // alternative (Include/ThenInclude, manual join) or point at the client-eval policy.
            Assert.Contains("MULTI-HOP", caught.Message, System.StringComparison.Ordinal);
            Assert.Contains("Include", caught.Message, System.StringComparison.Ordinal);
            return;
        }

        Assert.NotNull(result);
        var ordered = result!.Cast<object>()
            .Select(r => (Name: (string)((dynamic)r).Name, GcCount: (int)((dynamic)r).GcCount))
            .OrderBy(r => r.Name)
            .ToArray();
        var dump = string.Join(", ", ordered.Select(r => $"{r.Name}={r.GcCount}"));
        Assert.True(ordered.Length == 2, $"Expected 2 rows, got {ordered.Length}: [{dump}]");
        Assert.True(ordered[0].Name == "Alice" && ordered[0].GcCount == 3,
            $"Alice count wrong (silent miscount or wrong column binding): [{dump}]");
        Assert.True(ordered[1].Name == "Bob" && ordered[1].GcCount == 0,
            $"Bob count wrong: [{dump}]");
    }

    [Table("MhnParent")]
    public sealed class MhnParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<MhnChild> Children { get; set; } = new();
    }

    [Table("MhnChild")]
    public sealed class MhnChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public List<MhnGrandChild> GrandChildren { get; set; } = new();
    }

    [Table("MhnGrandChild")]
    public sealed class MhnGrandChild
    {
        [Key] public int Id { get; set; }
        public int ChildId { get; set; }
    }

    private sealed class MhnContext : DbContext
    {
        public MhnContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
