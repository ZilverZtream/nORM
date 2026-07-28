using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Probes within the GroupBy/aggregate/OrderBy surface: ordering by a projected decimal aggregate member,
/// grouping by a string-backed enum converter column, and Distinct-then-GroupBy. Diffed against the
/// LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupByConverterOrderByProbeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public enum Status { New, Active, Closed }

    private sealed class StatusToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    [Table("PbRow")]
    public sealed class PbRow
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public Status St { get; set; }
    }

    private static readonly PbRow[] Seed =
    {
        new PbRow { Id = 1, Cat = "a", Amount = 10.5m, St = Status.Active },
        new PbRow { Id = 2, Cat = "a", Amount = 20.0m, St = Status.Active },
        new PbRow { Id = 3, Cat = "b", Amount = 2.0m,  St = Status.New },
        new PbRow { Id = 4, Cat = "b", Amount = 3.0m,  St = Status.Closed },
        new PbRow { Id = 5, Cat = "c", Amount = 9.9m,  St = Status.New },
        new PbRow { Id = 6, Cat = "c", Amount = 100m,  St = Status.Active },
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PbRow (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Amount TEXT NOT NULL, St TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PbRow>().HasKey(i => i.Id);
                mb.Entity<PbRow>().Property<Status>(i => i.St).HasConversion(new StatusToNameConverter());
            }
        });
        foreach (var r in Seed) _ctx.Add(new PbRow { Id = r.Id, Cat = r.Cat, Amount = r.Amount, St = r.St });
        await _ctx.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    // ---- OrderBy a projected decimal MIN aggregate: mins are a=10.5, b=2.0, c=9.9 -> numeric asc [b,c,a]. ----
    [Fact]
    public void OrderBy_projected_decimal_min_aggregate_sorts_numerically()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Select(g => new { K = g.Key, M = g.Min(x => x.Amount) })
                         .OrderBy(x => x.M).Select(x => x.K).ToList();
        var norm = _ctx.Query<PbRow>().GroupBy(x => x.Cat).Select(g => new { K = g.Key, M = g.Min(x => x.Amount) })
                         .OrderBy(x => x.M).Select(x => x.K).ToList();
        Assert.Equal(oracle, norm); // [b, c, a]
    }

    // ---- GroupBy on a string-backed enum converter column, project Key + Count. ----
    [Fact]
    public void GroupBy_enum_converter_key_roundtrips()
    {
        var oracle = Seed.GroupBy(x => x.St).Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        var norm = _ctx.Query<PbRow>().GroupBy(x => x.St).Select(g => new { K = g.Key, C = g.Count() }).ToList().OrderBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- Distinct then GroupBy: distinct (Cat, St) pairs grouped by Cat. ----
    [Fact]
    public void Distinct_then_groupby_counts_distinct_pairs()
    {
        var oracle = Seed.Select(x => new { x.Cat, x.St }).Distinct().GroupBy(x => x.Cat)
                         .Select(g => new { K = g.Key, C = g.Count() }).OrderBy(x => x.K).ToList();
        var norm = _ctx.Query<PbRow>().Select(x => new { x.Cat, x.St }).Distinct().GroupBy(x => x.Cat)
                         .Select(g => new { K = g.Key, C = g.Count() }).ToList().OrderBy(x => x.K).ToList();
        Assert.Equal(oracle.Select(o => (o.K, o.C)).ToList(), norm.Select(o => (o.K, o.C)).ToList());
    }

    // ---- OrderByDescending a projected decimal SUM aggregate: sums a=30.5, b=5.0, c=109.9 -> desc [c,a,b]. ----
    [Fact]
    public void OrderByDescending_projected_decimal_sum_aggregate_sorts_numerically()
    {
        var oracle = Seed.GroupBy(x => x.Cat).Select(g => new { K = g.Key, T = g.Sum(x => x.Amount) })
                         .OrderByDescending(x => x.T).Select(x => x.K).ToList();
        var norm = _ctx.Query<PbRow>().GroupBy(x => x.Cat).Select(g => new { K = g.Key, T = g.Sum(x => x.Amount) })
                         .OrderByDescending(x => x.T).Select(x => x.K).ToList();
        Assert.Equal(oracle, norm); // [c, a, b]
    }
}
