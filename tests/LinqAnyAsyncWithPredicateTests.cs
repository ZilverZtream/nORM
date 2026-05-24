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
/// EF-parity overload: <c>AnyAsync(predicate)</c> lets users write
/// <c>await q.AnyAsync(p =&gt; p.IsActive)</c> instead of having to
/// chain <c>.Where(p).AnyAsync()</c>. Without the overload the C#
/// compiler tries to bind the lambda to the only single-arg overload
/// (CancellationToken) and produces CS1660. Silent-wrongness risks
/// if the wrapper is mis-routed: predicate dropped -> always true
/// when any row exists; or wrapper resolved to a different Any
/// overload that ignores its argument.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAnyAsyncWithPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AnyPredItem (Id INTEGER PRIMARY KEY, Amount INTEGER NOT NULL);
            INSERT INTO AnyPredItem VALUES (1, 10), (2, 20), (3, 30);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AnyPredItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task AnyAsync_predicate_returns_true_when_matches_exist()
    {
        var any = await _ctx.Query<AnyPredItem>().AnyAsync(i => i.Amount > 15);
        Assert.True(any);
    }

    [Fact]
    public async Task AnyAsync_predicate_returns_false_when_no_matches()
    {
        // Threshold above all rows -- silent-wrongness would return true
        // (predicate dropped) since table is non-empty.
        var any = await _ctx.Query<AnyPredItem>().AnyAsync(i => i.Amount > 1000);
        Assert.False(any);
    }

    [Fact]
    public async Task AnyAsync_predicate_returns_false_on_empty_source()
    {
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM AnyPredItem";
            await cmd.ExecuteNonQueryAsync();
        }
        var any = await _ctx.Query<AnyPredItem>().AnyAsync(i => i.Amount > 0);
        Assert.False(any);
    }

    [Fact]
    public async Task AnyAsync_predicate_composes_after_where()
    {
        // Outer Where + AnyAsync(predicate) -- the wrapper feeds Where(p)
        // into the chain, so the resulting expression is
        // Where(outer).Where(inner).Any(). Both predicates must apply.
        var any = await _ctx.Query<AnyPredItem>()
            .Where(i => i.Amount >= 20)
            .AnyAsync(i => i.Amount < 25);
        Assert.True(any); // 20 qualifies
    }

    [Fact]
    public async Task AnyAsync_predicate_composes_after_where_no_overlap()
    {
        // Two predicates that cannot both be true at once. Silent-wrongness
        // would short-circuit one of them and return true.
        var any = await _ctx.Query<AnyPredItem>()
            .Where(i => i.Amount >= 25)
            .AnyAsync(i => i.Amount < 25);
        Assert.False(any);
    }

    [Table("AnyPredItem")]
    public sealed class AnyPredItem
    {
        [Key] public int Id { get; set; }
        public int Amount { get; set; }
    }
}
