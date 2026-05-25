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
/// Pins SQL emit for set operations (<c>Union</c>, <c>Concat</c>, <c>Intersect</c>,
/// <c>Except</c>) where one or both arms have <c>Take</c> / <c>Skip</c> applied
/// (typically after an <c>OrderBy</c>). nORM previously threw with a workaround
/// pin because the unwrapped composition <c>SELECT ... LIMIT n UNION SELECT ...</c>
/// is rejected by SQLite (and most dialects); the implementation wraps each arm
/// in parentheses so <c>(SELECT ... LIMIT n) UNION (SELECT ...)</c> parses and
/// executes correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSetOpWithTakeSkipPagingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SoRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, Cat TEXT NOT NULL);
            INSERT INTO SoRow VALUES
                (1, 10, 'a'),
                (2, 20, 'a'),
                (3, 30, 'a'),
                (4, 40, 'b'),
                (5, 50, 'b'),
                (6, 60, 'b');
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
    public async Task Union_with_ordered_take_on_left_arm_returns_top_n_then_full_right()
    {
        var leftTop2 = _ctx.Query<SoRow>().Where(r => r.Cat == "a").OrderBy(r => r.V).Take(2).Select(r => new { r.V });
        var right    = _ctx.Query<SoRow>().Where(r => r.Cat == "b").Select(r => new { r.V });
        var combined = (await leftTop2.Union(right).ToListAsync())
            .Select(x => x.V).OrderBy(v => v).ToArray();
        Assert.Equal(new[] { 10, 20, 40, 50, 60 }, combined);
    }

    [Fact]
    public async Task Union_with_ordered_take_on_both_arms_returns_top_n_of_each()
    {
        var leftTop2  = _ctx.Query<SoRow>().Where(r => r.Cat == "a").OrderBy(r => r.V).Take(2).Select(r => new { r.V });
        var rightTop1 = _ctx.Query<SoRow>().Where(r => r.Cat == "b").OrderBy(r => r.V).Take(1).Select(r => new { r.V });
        var combined  = (await leftTop2.Union(rightTop1).ToListAsync())
            .Select(x => x.V).OrderBy(v => v).ToArray();
        Assert.Equal(new[] { 10, 20, 40 }, combined);
    }

    [Fact]
    public async Task Concat_with_skip_then_take_on_right_arm_returns_concatenated_window()
    {
        var left     = _ctx.Query<SoRow>().Where(r => r.Cat == "a").OrderBy(r => r.V).Select(r => new { r.V });
        var rightMid = _ctx.Query<SoRow>().Where(r => r.Cat == "b").OrderBy(r => r.V).Skip(1).Take(1).Select(r => new { r.V });
        var combined = (await left.Concat(rightMid).ToListAsync())
            .Select(x => x.V).ToArray();
        // 'a' fully (10, 20, 30) then 'b' Skip(1).Take(1) → 50.
        Assert.Equal(new[] { 10, 20, 30, 50 }, combined);
    }

    [Fact]
    public async Task Intersect_with_take_on_left_arm_keeps_intersected_subset()
    {
        //  - left arm (cat=a top 2 by V) = V ∈ {10, 20}
        //  - right arm (everything) = V ∈ {10, 20, 30, 40, 50, 60}
        // Intersect → {10, 20}
        var leftTop2 = _ctx.Query<SoRow>().Where(r => r.Cat == "a").OrderBy(r => r.V).Take(2).Select(r => new { r.V });
        var rightAll = _ctx.Query<SoRow>().Select(r => new { r.V });
        var combined = (await leftTop2.Intersect(rightAll).ToListAsync())
            .Select(x => x.V).OrderBy(v => v).ToArray();
        Assert.Equal(new[] { 10, 20 }, combined);
    }

    [Table("SoRow")]
    public sealed class SoRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string Cat { get; set; } = string.Empty;
    }
}
