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
/// Pins LINQ <c>Aggregate</c> over a numeric column with a max/min-fold
/// accumulator. Lowers to server-side <c>MAX(col)</c> / <c>MIN(col)</c>
/// via the same rewrite path used by the sum-fold (b3ca42b). Recognised
/// shapes:
///   max: <c>(acc, x) =&gt; x &gt; acc ? x : acc</c> AND <c>Math.Max(acc, x)</c>
///   min: <c>(acc, x) =&gt; x &lt; acc ? x : acc</c> AND <c>Math.Min(acc, x)</c>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateMinMaxFoldTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MfRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            INSERT INTO MfRow VALUES (1, 10), (2, 40), (3, 25), (4, 5);
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
    public async Task Aggregate_max_fold_via_conditional_lowers_to_MAX_on_server()
    {
        await Task.CompletedTask;
        int max = _ctx.Query<MfRow>()
            .Select(r => r.Score)
            .Aggregate((acc, x) => x > acc ? x : acc);
        Assert.Equal(40, max);
    }

    [Fact]
    public async Task Aggregate_max_fold_via_Math_Max_lowers_to_MAX_on_server()
    {
        await Task.CompletedTask;
        int max = _ctx.Query<MfRow>()
            .Select(r => r.Score)
            .Aggregate((acc, x) => Math.Max(acc, x));
        Assert.Equal(40, max);
    }

    [Fact]
    public async Task Aggregate_min_fold_via_Math_Min_lowers_to_MIN_on_server()
    {
        await Task.CompletedTask;
        int min = _ctx.Query<MfRow>()
            .Select(r => r.Score)
            .Aggregate((acc, x) => Math.Min(acc, x));
        Assert.Equal(5, min);
    }

    [Fact]
    public async Task Aggregate_max_fold_with_seed_clamps_below_max_with_seed_floor()
    {
        await Task.CompletedTask;
        // Seed = 100 (above all values) → result = 100 (seed wins).
        int result = _ctx.Query<MfRow>()
            .Select(r => r.Score)
            .Aggregate(100, (acc, x) => Math.Max(acc, x));
        Assert.Equal(100, result);
    }

    [Table("MfRow")]
    public sealed class MfRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
