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
/// Pins the deterministic error message for the
/// <c>g.OrderBy(...).Select(...).First()</c> shape inside a grouping
/// projection. The shape requires a correlated subquery emission that
/// nORM does not yet wire — surface a clear exception that points the
/// user at the supported `g.Min(selector)` / `g.Max(selector)` equivalents
/// rather than silently producing wrong results or a confusing
/// materializer crash. The original failing path crashed inside
/// SqliteDataReader during materialization because TranslateGroupAggregateMethod
/// returned null and SelectClauseVisitor emitted nothing.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupOrderByFirstUnsupportedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GofuRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO GofuRow VALUES
                (1, 'NA',   90),
                (2, 'NA',   60),
                (3, 'EMEA', 70);
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
    public async Task GroupBy_orderby_first_in_projection_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<GofuRow>()
                .GroupBy(r => r.Region)
                .Select(g => new { Region = g.Key, TopId = g.OrderByDescending(r => r.Score).Select(r => r.Id).FirstOrDefault() })
                .ToListAsync();
        });
        // Either NormQueryException (unwrapped) or NormUnsupportedFeatureException.
        Assert.Contains("OrderBy", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("GofuRow")]
    public sealed class GofuRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
