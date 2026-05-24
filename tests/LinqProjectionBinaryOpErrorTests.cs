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
/// Strict pin for the bit-shift binary operators (LeftShift / RightShift)
/// in projection. Originally pinned as throw-or-correct ('throws with
/// actionable message pointing at the multiply workaround') -- that was a
/// cop-out: SQLite, SQL Server, MySQL, and PostgreSQL all support &lt;&lt;
/// and &gt;&gt; directly. The throw forced users into a needless rewrite
/// for an operator the planner accepts natively. Pin flipped to strict
/// per the implement-first feedback.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionBinaryOpErrorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PbRow (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL);
            INSERT INTO PbRow VALUES (1, 4),(2, 8);
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
    public async Task LeftShift_in_projection_doubles_value_per_row()
    {
        var result = await _ctx.Query<PbRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Doubled = r.Value << 1 })
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(8, result[0].Doubled);   // 4 << 1
        Assert.Equal(16, result[1].Doubled);  // 8 << 1
    }

    [Fact]
    public async Task RightShift_in_projection_halves_value_per_row()
    {
        var result = await _ctx.Query<PbRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Halved = r.Value >> 1 })
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal(2, result[0].Halved);   // 4 >> 1
        Assert.Equal(4, result[1].Halved);   // 8 >> 1
    }

    [Fact]
    public async Task LeftShift_in_Where_filters_rows_strictly()
    {
        // (Value << 1) > 10 -> rows where Value > 5 -> only Id 2 (Value 8).
        var result = await _ctx.Query<PbRow>()
            .Where(r => (r.Value << 1) > 10)
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PbRow")]
    public sealed class PbRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
