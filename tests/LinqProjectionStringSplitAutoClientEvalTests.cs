using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pin for `Select(p =&gt; p.Csv.Split(','))` in projection. string.Split
/// returns string[] which has no SQL equivalent; previously this threw
/// NormUnsupportedFeatureException requiring users to opt into
/// ClientEvaluationPolicy.Allow. The auto-route fix lets SAFE leaf-only
/// untranslatable expressions (string.Split, ToCharArray, similar)
/// transparently route through client-eval even under the default
/// Throw policy. Complex untranslatables (correlated subqueries,
/// multi-hop nav) still throw -- only narrow LINQ-to-Objects-pure
/// leaves auto-route.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringSplitAutoClientEvalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PssaItem (Id INTEGER PRIMARY KEY, Csv TEXT NOT NULL);
            INSERT INTO PssaItem VALUES (1, 'a,b,c'), (2, 'x,y'), (3, 'lone');
            """;
        await cmd.ExecuteNonQueryAsync();
        // Default policy (Throw) -- the fix should auto-route safely.
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PssaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Split_in_projection_auto_routes_to_client_eval()
    {
        // Project an anonymous type carrying the Split result so the
        // materializer has a reference-type top-level type (string[]
        // alone fails the ToListAsync TResult: class new() constraint).
        var rows = await _ctx.Query<PssaItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Parts = p.Csv.Split(new[] { ',' }) })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(new[] { "a", "b", "c" }, rows[0].Parts);
        Assert.Equal(new[] { "x", "y" },      rows[1].Parts);
        Assert.Equal(new[] { "lone" },        rows[2].Parts);
    }

    [Table("PssaItem")]
    public sealed class PssaItem
    {
        [Key] public int Id { get; set; }
        public string Csv { get; set; } = string.Empty;
    }
}
