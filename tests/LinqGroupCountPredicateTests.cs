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
/// Pins <c>g.Count(predicate)</c> inside a grouped projection where the
/// predicate captures a literal constant — exercises the count-with-predicate
/// path in ExpressionToSqlVisitor that allocates a sub-visitor for the
/// predicate body. The constant binding has to land in the outer translator's
/// parameter dict for the COUNT(CASE WHEN ... END) expression to be runnable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqGroupCountPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GcpRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO GcpRow VALUES
                (1, 'NA',  90),
                (2, 'NA',  60),
                (3, 'NA',  85),
                (4, 'EMEA', 70),
                (5, 'EMEA', 55);
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
    public async Task GroupBy_with_count_predicate_against_literal_returns_filtered_counts()
    {
        // Count rows per region where Score >= 80. NA: 2 (90, 85); EMEA: 0.
        var groups = (await _ctx.Query<GcpRow>()
            .GroupBy(r => r.Region)
            .Select(g => new { Region = g.Key, HighScorers = g.Count(r => r.Score >= 80) })
            .ToListAsync())
            .OrderBy(g => g.Region).ToArray();

        Assert.Equal(2, groups.Length);
        Assert.Equal(("EMEA", 0), (groups[0].Region, groups[0].HighScorers));
        Assert.Equal(("NA",   2), (groups[1].Region, groups[1].HighScorers));
    }

    [Table("GcpRow")]
    public sealed class GcpRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
