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
/// Covers two related projection shapes against real data:
///  - Casting an enum column to int (or to its nullable int equivalent), which application code
///    routinely does when feeding values into a numeric DTO or comparison.
///  - Ternary conditional expressions in WHERE and Select clauses, which need to translate to
///    SQL CASE WHEN ... END rather than throwing or evaluating client-side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqEnumAndConditionalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EcRow (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO EcRow VALUES
                (1, 0, 90),
                (2, 1, 70),
                (3, 2, 30),
                (4, 0, 10);
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
    public async Task Enum_cast_to_int_in_projection_returns_underlying_value()
    {
        var values = (await _ctx.Query<EcRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { V = (int)r.Status })
            .ToListAsync())
            .Select(r => r.V).ToArray();
        Assert.Equal(new[] { 0, 1, 2, 0 }, values);
    }

    [Fact]
    public async Task Enum_equality_in_where_filters_by_status()
    {
        var ids = (await _ctx.Query<EcRow>()
            .Where(r => r.Status == EcStatus.Active)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 4 }, ids);
    }

    [Fact]
    public async Task Conditional_in_where_translates_to_CASE_WHEN()
    {
        // Ids where (Score > 50 ? Score : 0) > 60
        var ids = (await _ctx.Query<EcRow>()
            .Where(r => (r.Score > 50 ? r.Score : 0) > 60)
            .ToListAsync())
            .Select(r => r.Id).OrderBy(i => i).ToArray();
        // Score > 50: id 1 (90), id 2 (70). > 60: both. Id 3 (30) -> 0, id 4 (10) -> 0 -> excluded.
        Assert.Equal(new[] { 1, 2 }, ids);
    }

    [Fact]
    public async Task Conditional_in_projection_returns_categorized_value()
    {
        var labels = (await _ctx.Query<EcRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Bucket = r.Score >= 80 ? "high" : (r.Score >= 50 ? "mid" : "low") })
            .ToListAsync())
            .ToArray();
        Assert.Equal(4, labels.Length);
        Assert.Equal("high", labels[0].Bucket); // 90
        Assert.Equal("mid",  labels[1].Bucket); // 70
        Assert.Equal("low",  labels[2].Bucket); // 30
        Assert.Equal("low",  labels[3].Bucket); // 10
    }

    public enum EcStatus { Active = 0, Pending = 1, Archived = 2 }

    [Table("EcRow")]
    public sealed class EcRow
    {
        [Key] public int Id { get; set; }
        public EcStatus Status { get; set; }
        public int Score { get; set; }
    }
}
