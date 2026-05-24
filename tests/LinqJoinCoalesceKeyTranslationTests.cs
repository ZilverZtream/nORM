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
/// Pins JoinTranslator with a computed key expression that allocates a SQL
/// parameter — the parallel of the OrderBy / GroupBy parameter-merge bugs
/// from c46beb9 / 3c02675. A COALESCE fallback in the join key should bind
/// the constant correctly so the join produces matching rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqJoinCoalesceKeyTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE JckLeft  (Id INTEGER PRIMARY KEY, Code TEXT NULL);
            CREATE TABLE JckRight (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Label TEXT NOT NULL);
            INSERT INTO JckLeft  VALUES (1,'NA'),(2,NULL),(3,'EU');
            INSERT INTO JckRight VALUES (10,'NA','North'),(20,'EU','Europe'),(30,'XX','Default');
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
    public async Task Join_with_coalesce_key_matches_null_rows_against_fallback_label()
    {
        // Left side's NULL Code coalesces to 'XX' so row 2 joins to right row 30 (Default).
        var rows = (await _ctx.Query<JckLeft>()
            .Join(_ctx.Query<JckRight>(),
                  l => l.Code ?? "XX",
                  r => r.Code,
                  (l, r) => new { l.Id, r.Label })
            .ToListAsync())
            .OrderBy(x => x.Id).ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal((1, "North"),   (rows[0].Id, rows[0].Label));
        Assert.Equal((2, "Default"), (rows[1].Id, rows[1].Label));
        Assert.Equal((3, "Europe"),  (rows[2].Id, rows[2].Label));
    }

    [Table("JckLeft")]
    public sealed class JckLeft
    {
        [Key] public int Id { get; set; }
        public string? Code { get; set; }
    }

    [Table("JckRight")]
    public sealed class JckRight
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Label { get; set; } = string.Empty;
    }
}
