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
/// Pins the deterministic error message when callers chain
/// <c>Select(proj).Distinct().Join(...)</c>. nORM doesn't yet emit the
/// subquery wrap (`FROM (SELECT DISTINCT ... FROM tbl) AS T0 INNER JOIN ...`)
/// that this shape needs — without the wrap the outer key resolves to an
/// empty fragment and SQLite throws a cryptic `near "=": syntax error`.
/// Detect the shape at translation time and surface a clear exception with
/// the two supported workarounds.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctBeforeJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DbjLeft  (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            CREATE TABLE DbjRight (Code TEXT NOT NULL PRIMARY KEY, Tag TEXT NOT NULL);
            INSERT INTO DbjLeft  VALUES (1, 'A'),(2, 'A'),(3, 'B'),(4, 'B'),(5, 'C');
            INSERT INTO DbjRight VALUES ('A','alpha'),('B','beta'),('C','gamma');
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
    public async Task Distinct_then_join_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<DbjLeft>()
                .Select(l => l.Code)
                .Distinct()
                .Join(_ctx.Query<DbjRight>(), code => code, r => r.Code, (code, r) => new { code, r.Tag })
                .ToListAsync();
        });
        // Message must identify the constraint and point at the supported workarounds.
        Assert.Contains("Distinct", ex.Message, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Contains", ex.Message, System.StringComparison.OrdinalIgnoreCase);
    }

    [Table("DbjLeft")]
    public sealed class DbjLeft
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    [Table("DbjRight")]
    public sealed class DbjRight
    {
        [Key] public string Code { get; set; } = string.Empty;
        public string Tag { get; set; } = string.Empty;
    }
}
