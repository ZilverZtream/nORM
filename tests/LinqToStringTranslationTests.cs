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
/// Pins server-side translation of <c>ToString()</c> on non-string columns.
/// Common shapes:
/// <list type="bullet">
///   <item><c>Where(x =&gt; x.IntCol.ToString() == "5")</c></item>
///   <item><c>Where(x =&gt; x.IntCol.ToString().Contains("12"))</c> — composes with LIKE</item>
///   <item><c>Select(x =&gt; x.Id.ToString() + "-" + x.Name)</c> — string concat</item>
/// </list>
/// Each provider gets a CAST(col AS TEXT/VARCHAR/CHAR) translation. Without
/// this, users must hand-write computed columns or move the work client-side.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqToStringTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TsRow (Id INTEGER PRIMARY KEY, Quantity INTEGER NOT NULL, Name TEXT NOT NULL);
            INSERT INTO TsRow VALUES
                (1, 5, 'alpha'),
                (2, 12, 'bravo'),
                (3, 125, 'charlie'),
                (4, 100, 'delta');
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
    public async Task ToString_on_int_column_compared_against_literal_matches_one_row()
    {
        var rows = await _ctx.Query<TsRow>()
            .Where(r => r.Quantity.ToString() == "5")
            .ToListAsync();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
    }

    [Fact]
    public async Task ToString_on_int_column_chained_with_contains_matches_substring()
    {
        // "12" appears in "12" (row 2) and "125" (row 3) and "125" (row 3); also matches
        // "12" in "12" → rows 2 and 3.
        var rows = (await _ctx.Query<TsRow>()
            .Where(r => r.Quantity.ToString().Contains("12"))
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(3, rows[1].Id);
    }

    [Table("TsRow")]
    public sealed class TsRow
    {
        [Key] public int Id { get; set; }
        public int Quantity { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
