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
/// Pins <c>Select(mappedColumn).Distinct().GroupJoin(...)</c>. The DISTINCT
/// scalar outer must be materialized as the grouping key while the left join
/// still preserves keys with no matching inner rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctBeforeGroupJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DbgjLeft  (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            CREATE TABLE DbgjRight (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO DbgjLeft  VALUES (1, 'A'),(2, 'A'),(3, 'B'),(4, 'C');
            INSERT INTO DbgjRight VALUES (1,'A','x'),(2,'A','y'),(3,'B','z');
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
    public async Task Distinct_then_groupjoin_executes_against_distinct_outer_keys()
    {
        var rows = await _ctx.Query<DbgjLeft>()
            .Select(l => l.Code)
            .Distinct()
            .GroupJoin(
                _ctx.Query<DbgjRight>(),
                code => code,
                r => r.Code,
                (code, rs) => new { code, Tags = rs.Select(r => r.Tag).OrderBy(tag => tag).ToList() })
            .OrderBy(row => row.code)
            .ToListAsync();

        Assert.Equal(new[] { "A", "B", "C" }, rows.Select(row => row.code).ToArray());
        Assert.Equal(new[] { "x", "y" }, rows[0].Tags);
        Assert.Equal(new[] { "z" }, rows[1].Tags);
        Assert.Empty(rows[2].Tags);
    }

    [Table("DbgjLeft")]
    public sealed class DbgjLeft
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    [Table("DbgjRight")]
    public sealed class DbgjRight
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Tag { get; set; } = string.Empty;
    }
}
