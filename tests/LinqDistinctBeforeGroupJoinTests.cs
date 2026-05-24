using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Sister of <see cref="LinqDistinctBeforeJoinTests"/> — pins the deterministic
/// error for <c>Select(proj).Distinct().GroupJoin(...)</c>. Same root cause:
/// nORM doesn't yet emit the subquery wrap
/// (`FROM (SELECT DISTINCT ... FROM tbl) AS T0 LEFT JOIN ...`) that this shape
/// needs — without the wrap the outer key resolves to an empty fragment and
/// SQLite throws a cryptic syntax error. Detect at translation time.
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
            INSERT INTO DbgjLeft  VALUES (1, 'A'),(2, 'A'),(3, 'B');
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
    public async Task Distinct_then_groupjoin_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<System.Exception>(async () =>
        {
            await _ctx.Query<DbgjLeft>()
                .Select(l => l.Code)
                .Distinct()
                .GroupJoin(
                    _ctx.Query<DbgjRight>(),
                    code => code,
                    r => r.Code,
                    (code, rs) => new { code, Rights = rs.ToList() })
                .ToListAsync();
        });
        Assert.Contains("Distinct", ex.Message, System.StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GroupJoin", ex.Message, System.StringComparison.OrdinalIgnoreCase);
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
