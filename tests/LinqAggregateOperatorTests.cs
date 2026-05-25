using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins LINQ <c>Aggregate</c> over a column where the accumulator is a
/// recognisable fold pattern. Two shapes are supported:
///   1. Numeric sum-fold: <c>q.Aggregate((acc, x) =&gt; acc + x.Member)</c> — lowers to SUM.
///   2. Numeric sum-fold with seed: <c>q.Aggregate(0L, (acc, x) =&gt; acc + x.Member)</c>.
/// Other fold shapes (non-numeric concat, custom binary ops, three-arg overload
/// with resultSelector) remain client-side and aren't covered here. The point
/// of this implementation is to recognise the dominant pattern people reach
/// for and avoid silently sucking the whole table into memory.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateOperatorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AggRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            INSERT INTO AggRow VALUES (1, 10), (2, 20), (3, 30), (4, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Aggregate_sum_fold_with_seed_lowers_to_SUM_on_server()
    {
        await Task.CompletedTask;
        long total = _ctx.Query<AggRow>()
            .Select(r => (long)r.Score)
            .Aggregate(0L, (acc, s) => acc + s);
        Assert.Equal(100L, total);
    }

    [Fact]
    public async Task Aggregate_sum_fold_no_seed_overload_lowers_to_SUM_on_server()
    {
        await Task.CompletedTask;
        long total = _ctx.Query<AggRow>()
            .Select(r => (long)r.Score)
            .Aggregate((acc, s) => acc + s);
        Assert.Equal(100L, total);
    }

    [Fact]
    public async Task Aggregate_sum_fold_with_seed_returns_seed_when_table_empty()
    {
        await using (var del = _cn.CreateCommand()) { del.CommandText = "DELETE FROM AggRow"; await del.ExecuteNonQueryAsync(); }
        long total = _ctx.Query<AggRow>()
            .Select(r => (long)r.Score)
            .Aggregate(7L, (acc, s) => acc + s);
        Assert.Equal(7L, total);
    }

    [Table("AggRow")]
    public sealed class AggRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
