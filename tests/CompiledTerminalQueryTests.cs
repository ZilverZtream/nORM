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
/// Covers <see cref="Norm.CompileTerminalQuery{TContext,TParam,TResult}"/>: compiled
/// delegates whose expression ends in a terminal operator. Terminal semantics must
/// match the non-compiled runtime — First on empty throws, Single enforces
/// cardinality, OrDefault returns default — and parameter values must bind per call
/// rather than being baked into the cached plan.
/// </summary>
[Trait("Category", "Fast")]
public class CompiledTerminalQueryTests
{
    [Table("CtqItem")]
    private class CtqItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(int rowCount)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE CtqItem (
                    Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name  TEXT    NOT NULL,
                    Score INTEGER NOT NULL
                )";
            cmd.ExecuteNonQuery();
        }
        for (var i = 1; i <= rowCount; i++)
        {
            using var insert = cn.CreateCommand();
            insert.CommandText = "INSERT INTO CtqItem (Name, Score) VALUES (@n, @s)";
            insert.Parameters.AddWithValue("@n", $"item{i}");
            insert.Parameters.AddWithValue("@s", i * 10);
            insert.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // Static delegates: compiled once per process, per the compiled-query test guidance.
    private static readonly Func<DbContext, int, Task<CtqItem>> _firstByMinScore =
        Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<CtqItem>().Where(x => x.Score >= minScore).OrderBy(x => x.Id).First());

    private static readonly Func<DbContext, int, Task<CtqItem?>> _firstOrDefaultByMinScore =
        Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<CtqItem>().Where(x => x.Score >= minScore).OrderBy(x => x.Id).FirstOrDefault());

    private static readonly Func<DbContext, int, Task<CtqItem>> _singleByScore =
        Norm.CompileTerminalQuery((DbContext c, int score) =>
            c.Query<CtqItem>().Single(x => x.Score == score));

    private static readonly Func<DbContext, int, Task<int>> _countByMinScore =
        Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<CtqItem>().Count(x => x.Score >= minScore));

    private static readonly Func<DbContext, long, Task<long>> _longCountAll =
        Norm.CompileTerminalQuery((DbContext c, long _) =>
            c.Query<CtqItem>().LongCount());

    private static readonly Func<DbContext, int, Task<bool>> _anyByMinScore =
        Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<CtqItem>().Any(x => x.Score >= minScore));

    private static readonly Func<DbContext, int, Task<int>> _sumScores =
        Norm.CompileTerminalQuery((DbContext c, int minScore) =>
            c.Query<CtqItem>().Where(x => x.Score >= minScore).Sum(x => x.Score));

    private static readonly Func<DbContext, int, Task<int>> _maxScore =
        Norm.CompileTerminalQuery((DbContext c, int _) =>
            c.Query<CtqItem>().Max(x => x.Score));

    [Fact]
    public async Task First_returns_the_first_matching_row_in_order()
    {
        var (cn, ctx) = CreateContext(5);
        using var _cn = cn;
        using var _ctx = ctx;

        var first = await _firstByMinScore(ctx, 25);
        Assert.Equal(30, first.Score);
        Assert.Equal("item3", first.Name);
    }

    [Fact]
    public async Task First_on_no_match_throws_invalid_operation_like_linq()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() => _firstByMinScore(ctx, 999));
    }

    [Fact]
    public async Task FirstOrDefault_on_no_match_returns_null()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Null(await _firstOrDefaultByMinScore(ctx, 999));
        Assert.NotNull(await _firstOrDefaultByMinScore(ctx, 10));
    }

    [Fact]
    public async Task Single_enforces_cardinality_like_linq()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var single = await _singleByScore(ctx, 20);
        Assert.Equal("item2", single.Name);

        await Assert.ThrowsAsync<InvalidOperationException>(() => _singleByScore(ctx, 999));
    }

    [Fact]
    public async Task Count_binds_the_parameter_per_call_instead_of_baking_it()
    {
        var (cn, ctx) = CreateContext(5);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(5, await _countByMinScore(ctx, 0));
        Assert.Equal(3, await _countByMinScore(ctx, 30));
        Assert.Equal(0, await _countByMinScore(ctx, 999));
    }

    [Fact]
    public async Task LongCount_returns_long_typed_result()
    {
        var (cn, ctx) = CreateContext(4);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(4L, await _longCountAll(ctx, 0L));
    }

    [Fact]
    public async Task Any_reflects_matching_rows()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.True(await _anyByMinScore(ctx, 30));
        Assert.False(await _anyByMinScore(ctx, 31));
    }

    [Fact]
    public async Task Sum_and_Max_compute_server_side_aggregates()
    {
        var (cn, ctx) = CreateContext(4);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(10 + 20 + 30 + 40, await _sumScores(ctx, 0));
        Assert.Equal(30 + 40, await _sumScores(ctx, 30));
        Assert.Equal(40, await _maxScore(ctx, 0));
    }

    [Fact]
    public async Task Compiled_terminal_delegate_is_reusable_across_contexts()
    {
        var (cn1, ctx1) = CreateContext(2);
        using var _cn1 = cn1;
        using var _ctx1 = ctx1;
        var (cn2, ctx2) = CreateContext(5);
        using var _cn2 = cn2;
        using var _ctx2 = ctx2;

        Assert.Equal(2, await _countByMinScore(ctx1, 0));
        Assert.Equal(5, await _countByMinScore(ctx2, 0));
        Assert.Equal(2, await _countByMinScore(ctx1, 0));
    }

    [Fact]
    public void Sequence_shaped_body_is_rejected_with_guidance()
    {
        var ex = Assert.Throws<NormUsageException>(() =>
            Norm.CompileTerminalQuery((DbContext c, int minScore) =>
                c.Query<CtqItem>().Where(x => x.Score >= minScore)));
        Assert.Contains("List-returning overload", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Non_query_body_is_rejected_with_guidance()
    {
        var ex = Assert.Throws<NormUsageException>(() =>
            Norm.CompileTerminalQuery((DbContext c, int value) => value));
        Assert.Contains("terminal operator", ex.Message, StringComparison.Ordinal);
    }
}
