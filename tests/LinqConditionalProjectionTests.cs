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
/// Pins ternary (<c>?:</c>) conditional in a projection — should emit a
/// <c>CASE WHEN ... THEN ... ELSE ... END</c> SQL expression. Silent-wrongness
/// risk if the conditional is dropped or both branches collapse to the same
/// expression.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqConditionalProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CpRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            INSERT INTO CpRow VALUES (1,-5),(2,0),(3,10),(4,-3),(5,7);
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
    public async Task Conditional_string_projection_returns_per_row_branch_value()
    {
        var rows = (await _ctx.Query<CpRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Sign = r.Score > 0 ? "pos" : "non-pos" })
            .ToListAsync())
            .Select(r => (r.Id, r.Sign))
            .ToArray();
        Assert.Equal(new[] { (1, "non-pos"), (2, "non-pos"), (3, "pos"), (4, "non-pos"), (5, "pos") }, rows);
    }

    [Fact]
    public async Task Conditional_integer_projection_returns_per_row_branch_value()
    {
        var rows = (await _ctx.Query<CpRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Absish = r.Score < 0 ? -r.Score : r.Score })
            .ToListAsync())
            .Select(r => (r.Id, r.Absish))
            .ToArray();
        Assert.Equal(new[] { (1, 5), (2, 0), (3, 10), (4, 3), (5, 7) }, rows);
    }

    [Table("CpRow")]
    public sealed class CpRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
