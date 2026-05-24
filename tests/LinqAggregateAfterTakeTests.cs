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
/// Sister of <see cref="LinqWhereAfterTakeTests"/> for terminal aggregates
/// after Take/Skip. <c>q.Take(3).Count()</c> must return at most 3 (the count
/// of the windowed set). If the translator emits `SELECT COUNT(*) … LIMIT 3`
/// SQL evaluates COUNT first then LIMIT — the count returns the full-table
/// count and LIMIT picks the first row, returning that value. Silent.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AatRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            INSERT INTO AatRow VALUES (1,10),(2,20),(3,30),(4,40),(5,50);
            -- Full-table Count = 5. Take(3).Count() must = 3.
            -- Full-table Sum = 150. OrderBy(Id).Take(3).Sum() must = 60 (10+20+30).
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
    public async Task Count_after_take_returns_windowed_count_or_throws_actionable_pin()
    {
        System.Exception? caught = null;
        int? result = null;
        try
        {
            result = await _ctx.Query<AatRow>()
                .OrderBy(r => r.Id)
                .Take(3)
                .CountAsync();
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            Assert.IsType<NormUnsupportedFeatureException>(caught);
            Assert.Contains("Count", caught.Message, System.StringComparison.Ordinal);
            Assert.Contains("Take", caught.Message, System.StringComparison.Ordinal);
            return;
        }

        // Acceptable: windowed count = 3.
        Assert.True(result == 3,
            $"Expected Count(Take(3)) = 3, got {result} — likely the silent-wrongness bug (COUNT ran on full table, LIMIT applied separately).");
    }

    [Fact]
    public async Task Sum_after_take_returns_windowed_sum_or_throws_actionable_pin()
    {
        System.Exception? caught = null;
        int? result = null;
        try
        {
            result = await _ctx.Query<AatRow>()
                .OrderBy(r => r.Id)
                .Take(3)
                .SumAsync(r => r.Score);
        }
        catch (System.Exception ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            Assert.IsType<NormUnsupportedFeatureException>(caught);
            Assert.Contains("Sum", caught.Message, System.StringComparison.Ordinal);
            Assert.Contains("Take", caught.Message, System.StringComparison.Ordinal);
            return;
        }

        // Acceptable: windowed sum = 10+20+30 = 60.
        Assert.True(result == 60,
            $"Expected Sum(Take(3).Score) = 60 (10+20+30), got {result} — likely the silent-wrongness bug (SUM ran on full table = 150, then LIMIT 3 picked first scalar row).");
    }

    [Table("AatRow")]
    public sealed class AatRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
