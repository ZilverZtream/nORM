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
/// Pins server-side translation of <c>char.ToUpper(c)</c> and
/// <c>char.ToLower(c)</c> when applied to an indexed string char. Each lowers
/// to the existing per-provider UPPER / LOWER scalar around the SUBSTR result
/// — common in case-insensitive first-letter filtering like
/// <c>Where(x =&gt; char.ToLower(x.Code[0]) == 'a')</c>.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCharCaseTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ChRow (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO ChRow VALUES
                (1, 'Apple'),
                (2, 'avocado'),
                (3, 'Banana'),
                (4, 'cherry');
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
    public async Task ToLower_on_indexed_first_char_matches_letter_case_insensitively()
    {
        // Rows 1 ('Apple') and 2 ('avocado') both have first char that lowers to 'a'.
        var rows = (await _ctx.Query<ChRow>().Where(r => char.ToLower(r.Code[0]) == 'a').ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal(2, rows[1].Id);
    }

    [Fact]
    public async Task ToUpper_on_indexed_first_char_matches_letter_case_insensitively()
    {
        // 'B' upper of row 3's 'B' and... only row 3 starts with B/b in this fixture.
        var rows = await _ctx.Query<ChRow>().Where(r => char.ToUpper(r.Code[0]) == 'B').ToListAsync();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Table("ChRow")]
    public sealed class ChRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
