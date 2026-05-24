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
/// Pins <c>Join(...).Select(new { L.X, R.Y }).Distinct()</c> over a real
/// join where the projection includes columns from both sides. The
/// translator must apply DISTINCT to the joined-projection rows, not to
/// the left or right table individually.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqJoinDistinctAnonymousProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE JdLeft  (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            CREATE TABLE JdRight (Id INTEGER PRIMARY KEY, LeftId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO JdLeft  VALUES (1, 'A'),(2, 'B');
            -- Two right rows per left create 4 join rows but only 2 distinct (Code, Tag)
            -- pairs because the right rows share the same Tag value per LeftId.
            INSERT INTO JdRight VALUES (10, 1, 'X'),(11, 1, 'X'),(20, 2, 'Y'),(21, 2, 'Y');
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
    public async Task Distinct_anonymous_pair_after_join_dedupes_by_projected_columns()
    {
        // Raw join produces 4 rows: (A,X), (A,X), (B,Y), (B,Y).
        // Distinct on (Code, Tag) should reduce to 2 distinct pairs.
        var rows = (await _ctx.Query<JdLeft>()
            .Join(_ctx.Query<JdRight>(), l => l.Id, r => r.LeftId, (l, r) => new { l.Code, r.Tag })
            .Distinct()
            .ToListAsync())
            .OrderBy(p => p.Code).ToArray();

        Assert.Equal(2, rows.Length);
        Assert.Equal(("A", "X"), (rows[0].Code, rows[0].Tag));
        Assert.Equal(("B", "Y"), (rows[1].Code, rows[1].Tag));
    }

    [Table("JdLeft")]
    public sealed class JdLeft
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    [Table("JdRight")]
    public sealed class JdRight
    {
        [Key] public int Id { get; set; }
        public int LeftId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
