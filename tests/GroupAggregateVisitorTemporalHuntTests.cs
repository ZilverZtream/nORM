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

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Same root cause as the decimal group-aggregate hunt, but for TimeSpan (multi-day, 'c'-TEXT storage):
/// MIN/MAX emitted through the ExpressionToSqlVisitor group-aggregate path (HAVING) skip the
/// MinMaxAggregateOperand collation, so a multi-day duration MIN/MAX compares TEXT lexically
/// ("10.00:00:00" &lt; "2.00:00:00" because '1' &lt; '2'). Seeded so the lexical and numeric extremes
/// straddle the HAVING threshold. Diffed against the LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupAggregateVisitorTemporalHuntTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("TsItem")]
    public sealed class TsItem
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public TimeSpan Dur { get; set; }
    }

    private static readonly TsItem[] Seed =
    {
        // Cat "a": 10 days and 2 days. 'c' TEXT = "10.00:00:00", "2.00:00:00".
        //   numeric MIN = 2d, numeric MAX = 10d.
        //   lexical MIN = "10.00:00:00" (10d, WRONG), lexical MAX = "2.00:00:00" (2d, WRONG).
        new TsItem { Id = 1, Cat = "a", Dur = TimeSpan.FromDays(10) },
        new TsItem { Id = 2, Cat = "a", Dur = TimeSpan.FromDays(2) },
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TsItem (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Dur TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<TsItem>().HasKey(i => i.Id)
        });
        foreach (var r in Seed) _ctx.Add(new TsItem { Id = r.Id, Cat = r.Cat, Dur = r.Dur });
        await _ctx.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ---- HAVING MAX over multi-day TimeSpan. True max 10d > 5d -> keep "a". Lexical max 2d -> drop. ----
    [Fact]
    public void Having_Max_over_timespan_filters_numerically()
    {
        var threshold = TimeSpan.FromDays(5);
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Max(x => x.Dur) > threshold).Select(g => g.Key).OrderBy(k => k).ToList();
        var norm = _ctx.Query<TsItem>().GroupBy(x => x.Cat).Where(g => g.Max(x => x.Dur) > threshold).Select(g => g.Key).OrderBy(k => k).ToList();
        Assert.Equal(oracle, norm); // ["a"]
    }

    // ---- HAVING MIN over multi-day TimeSpan. True min 2d NOT > 5d -> drop "a". Lexical min 10d -> keep. ----
    [Fact]
    public void Having_Min_over_timespan_filters_numerically()
    {
        var threshold = TimeSpan.FromDays(5);
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Min(x => x.Dur) > threshold).Select(g => g.Key).OrderBy(k => k).ToList();
        var norm = _ctx.Query<TsItem>().GroupBy(x => x.Cat).Where(g => g.Min(x => x.Dur) > threshold).Select(g => g.Key).OrderBy(k => k).ToList();
        Assert.Equal(oracle, norm); // []
    }

    // ---- CLEAN-BILL CONTRAST: bare aggregate projection uses the correct MinMax collation path. ----
    [Fact]
    public void Bare_projection_Max_over_timespan_is_correct()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => new { M = g.Max(x => x.Dur) }).Single();
        var norm = _ctx.Query<TsItem>().GroupBy(x => x.Cat).Where(g => g.Key == "a").Select(g => new { M = g.Max(x => x.Dur) }).ToList().Single();
        Assert.Equal(oracle.M, norm.M); // 10 days
    }
}
