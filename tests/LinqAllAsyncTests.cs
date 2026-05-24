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
/// EF-parity addition: <c>AllAsync(predicate)</c> -- a very common EF idiom
/// (e.g. <c>q.AllAsync(p =&gt; p.IsActive)</c>) that was missing entirely
/// from nORM's async surface even though the underlying translator already
/// routes <c>Queryable.All</c> via <c>AllTranslator</c>. Single overload
/// because EF's All requires a predicate (no no-arg form).
///
/// Pins LINQ semantics including the vacuously-true contract on empty
/// source and the composition with outer Where. Silent-wrongness risk:
/// predicate inverted (returns true when any row violates) or All routed
/// to Any (returns true when any row satisfies).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAllAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AllItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL, IsActive INTEGER NOT NULL);
            INSERT INTO AllItem VALUES
                (1, 10, 1),
                (2, 20, 1),
                (3, 30, 1);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AllItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task AllAsync_returns_true_when_every_row_satisfies_predicate()
    {
        var all = await _ctx.Query<AllItem>().AllAsync(i => i.Amount > 0);
        Assert.True(all);
    }

    [Fact]
    public async Task AllAsync_returns_false_when_any_row_violates_predicate()
    {
        // Silent-wrongness probe: if All were silently routed to Any, this
        // would return true (Amount > 25 matches Id=3). Correct: false
        // because Id=1 and Id=2 violate.
        var all = await _ctx.Query<AllItem>().AllAsync(i => i.Amount > 25);
        Assert.False(all);
    }

    [Fact]
    public async Task AllAsync_returns_true_on_empty_source_vacuously()
    {
        // LINQ semantics: All over an empty source is true. Silent-wrongness
        // probe: a naive `NOT EXISTS (WHERE NOT predicate)` emit must still
        // hit this case (no rows -> NOT EXISTS is true -> result true).
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM AllItem";
            await cmd.ExecuteNonQueryAsync();
        }
        var all = await _ctx.Query<AllItem>().AllAsync(i => i.Amount > 1000);
        Assert.True(all);
    }

    [Fact]
    public async Task AllAsync_composes_after_outer_where()
    {
        // Outer Where narrows to {30}; AllAsync(Amount > 25) is true on that
        // subset. Silent-wrongness: dropped outer Where would evaluate over
        // all 3 rows and return false.
        var all = await _ctx.Query<AllItem>()
            .Where(i => i.Amount >= 30)
            .AllAsync(i => i.Amount > 25);
        Assert.True(all);
    }

    [Fact]
    public async Task AllAsync_returns_false_when_one_row_in_filtered_subset_violates()
    {
        // Outer Where keeps {20, 30}; inner predicate Amount > 25 violates
        // for Amount=20. Correct: false.
        var all = await _ctx.Query<AllItem>()
            .Where(i => i.Amount >= 20)
            .AllAsync(i => i.Amount > 25);
        Assert.False(all);
    }

    [Table("AllItem")]
    public sealed class AllItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
        public int IsActive { get; set; }
    }
}
