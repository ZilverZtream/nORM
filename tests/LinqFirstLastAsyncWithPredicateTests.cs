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
/// EF-parity overloads for the single-row terminal async family:
/// FirstAsync / FirstOrDefaultAsync / LastAsync / LastOrDefaultAsync.
/// Without the predicate overloads, <c>q.FirstAsync(p =&gt; p.Active)</c>
/// produces CS1660 (lambda binds to CancellationToken parameter).
/// Each wrapper redispatches through <c>source.Where(predicate)</c>
/// into the no-predicate overload so the existing single-row pipeline
/// (including the Last-flips-ORDER-BY behavior) covers them uniformly.
///
/// Silent-wrongness risks: predicate dropped -> returns first row of
/// outer source regardless; or wrapper resolved to the wrong overload.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqFirstLastAsyncWithPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FlPredItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO FlPredItem VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<FlPredItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task FirstAsync_predicate_returns_first_matching_row()
    {
        // Silent-wrongness: dropped predicate -> Id=1 (Amount=10).
        // Correct: lowest Id with Amount > 20 = Id=3 (Amount=30).
        var row = await _ctx.Query<FlPredItem>()
            .OrderBy(i => i.Id)
            .FirstAsync(i => i.Amount > 20);
        Assert.Equal(3, row.Id);
        Assert.Equal(30, row.Amount);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_predicate_returns_null_when_no_match()
    {
        // Silent-wrongness: dropped predicate -> Id=1 row instead of null.
        var row = await _ctx.Query<FlPredItem>()
            .FirstOrDefaultAsync(i => i.Amount > 1000);
        Assert.Null(row);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_predicate_returns_matching_row_when_present()
    {
        var row = await _ctx.Query<FlPredItem>()
            .OrderBy(i => i.Id)
            .FirstOrDefaultAsync(i => i.Amount == 40);
        Assert.NotNull(row);
        Assert.Equal(4, row!.Id);
    }

    [Fact]
    public async Task LastAsync_predicate_returns_last_matching_row()
    {
        // OrderBy(Id) + LastAsync flips ORDER BY -> reads max Id with
        // Amount < 40. Silent-wrongness: dropped predicate -> Id=5.
        // Correct: Id=3 (Amount=30 is the highest below 40).
        var row = await _ctx.Query<FlPredItem>()
            .OrderBy(i => i.Id)
            .LastAsync(i => i.Amount < 40);
        Assert.Equal(3, row.Id);
        Assert.Equal(30, row.Amount);
    }

    [Fact]
    public async Task LastOrDefaultAsync_predicate_returns_null_when_no_match()
    {
        var row = await _ctx.Query<FlPredItem>()
            .OrderBy(i => i.Id)
            .LastOrDefaultAsync(i => i.Amount > 1000);
        Assert.Null(row);
    }

    [Fact]
    public async Task LastOrDefaultAsync_predicate_returns_matching_row_when_present()
    {
        var row = await _ctx.Query<FlPredItem>()
            .OrderBy(i => i.Id)
            .LastOrDefaultAsync(i => i.Amount <= 30);
        Assert.NotNull(row);
        Assert.Equal(3, row!.Id);
    }

    [Fact]
    public async Task FirstAsync_predicate_composes_after_outer_where()
    {
        // Outer Where then predicate-overload -- both must apply.
        // Outer Amount >= 30 -> {30,40,50}; inner Amount < 50 -> {30,40};
        // OrderBy(Id) ensures First returns Id=3.
        var row = await _ctx.Query<FlPredItem>()
            .Where(i => i.Amount >= 30)
            .OrderBy(i => i.Id)
            .FirstAsync(i => i.Amount < 50);
        Assert.Equal(3, row.Id);
    }

    [Table("FlPredItem")]
    public sealed class FlPredItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
