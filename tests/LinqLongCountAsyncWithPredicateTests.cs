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
/// EF-parity overload: <c>LongCountAsync(predicate)</c> -- direct sister of
/// the CountAsync(predicate) and AnyAsync(predicate) wrappers. Without the
/// overload, <c>await q.LongCountAsync(p =&gt; p.IsActive)</c> hits CS1660
/// (lambda binds to the CancellationToken parameter). One-line wrapper
/// redispatches through <c>source.Where(predicate).LongCountAsync()</c>
/// so the existing 64-bit LongCount pipeline (and the e438a53 LongCount
/// rewrite gate arm) covers it uniformly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqLongCountAsyncWithPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE LcPredItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO LcPredItem VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<LcPredItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task LongCountAsync_predicate_returns_filtered_count_as_long()
    {
        long count = await _ctx.Query<LcPredItem>().LongCountAsync(i => i.Amount > 20);
        Assert.Equal(3L, count); // {30, 40, 50}
    }

    [Fact]
    public async Task LongCountAsync_predicate_returns_zero_when_no_matches()
    {
        // Silent-wrongness would return 5L (predicate dropped) on a non-empty
        // table; correctly-routed wrapper returns 0L.
        long count = await _ctx.Query<LcPredItem>().LongCountAsync(i => i.Amount > 1000);
        Assert.Equal(0L, count);
    }

    [Fact]
    public async Task LongCountAsync_predicate_composes_after_outer_where()
    {
        // Outer Where + predicate-overload -- both predicates AND together.
        // Amount >= 30 -> {30, 40, 50}; inner Amount < 50 -> {30, 40}.
        long count = await _ctx.Query<LcPredItem>()
            .Where(i => i.Amount >= 30)
            .LongCountAsync(i => i.Amount < 50);
        Assert.Equal(2L, count);
    }

    [Fact]
    public async Task LongCountAsync_predicate_with_alwaystrue_matches_no_predicate_form()
    {
        // Sanity: predicate-overload with a true-for-all predicate must
        // match the no-predicate form's result.
        long all = await _ctx.Query<LcPredItem>().LongCountAsync();
        long predTrue = await _ctx.Query<LcPredItem>().LongCountAsync(i => i.Amount > 0);
        Assert.Equal(5L, all);
        Assert.Equal(all, predTrue);
    }

    [Table("LcPredItem")]
    public sealed class LcPredItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
