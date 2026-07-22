using System.Collections.Generic;
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
/// GroupBy with an element selector (<c>GroupBy(key, s =&gt; s.Amount)</c>) whose parameterless or identity
/// aggregate appears inside a COMPUTED projection body (a string concat / arithmetic expression, not a bare
/// aggregate or anonymous type) — e.g. <c>Select(g =&gt; g.Key + ":" + g.Sum())</c>. This shape is translated
/// by the ETSV/computed-body path, which now threads the element selector so the aggregate operand becomes
/// the element selector's body (<c>SUM(Amount)</c>). Verified against a LINQ-to-objects oracle. Guards the
/// element-selector Path 2 fix; the explicit member-selector form (no element selector) still works.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GroupByElementSelectorComputedBodyTests
{
    [Table("GesbSale")]
    public sealed class Sale
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public int Amount { get; set; }
    }

    private static readonly List<Sale> Data = new()
    {
        new Sale { Id = 1, Region = "N", Amount = 10 },
        new Sale { Id = 2, Region = "N", Amount = 5 },
        new Sale { Id = 3, Region = "S", Amount = 7 },
        new Sale { Id = 4, Region = "S", Amount = 3 },
        new Sale { Id = 5, Region = "E", Amount = 20 },
    };

    private static async Task<DbContext> Ctx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE GesbSale (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Amount INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { OnModelCreating = mb => mb.Entity<Sale>().HasKey(x => x.Id) });
        foreach (var s in Data) ctx.Add(s);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task ParameterlessSum_InComputedConcatBody_MatchesLinq()
    {
        await using var ctx = await Ctx();
        var norm = (await ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount)
            .Select(g => g.Key + ":" + g.Sum()).ToListAsync()).OrderBy(x => x).ToList();
        var oracle = Data.GroupBy(s => s.Region, s => s.Amount)
            .Select(g => g.Key + ":" + g.Sum()).OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task IdentitySum_InComputedConcatBody_MatchesLinq()
    {
        await using var ctx = await Ctx();
        var norm = (await ctx.Query<Sale>().GroupBy(s => s.Region, s => s.Amount)
            .Select(g => g.Key + ":" + g.Sum(x => x)).ToListAsync()).OrderBy(x => x).ToList();
        var oracle = Data.GroupBy(s => s.Region, s => s.Amount)
            .Select(g => g.Key + ":" + g.Sum(x => x)).OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);
    }

    [Fact]
    public async Task MemberSelectorSum_InComputedConcatBody_StillMatchesLinq()
    {
        await using var ctx = await Ctx();
        var norm = (await ctx.Query<Sale>().GroupBy(s => s.Region)
            .Select(g => g.Key + ":" + g.Sum(x => x.Amount)).ToListAsync()).OrderBy(x => x).ToList();
        var oracle = Data.GroupBy(s => s.Region)
            .Select(g => g.Key + ":" + g.Sum(x => x.Amount)).OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);
    }
}
