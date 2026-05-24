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
/// Pins server-side translation of the string indexer (<c>s[i]</c>). Common
/// shapes:
/// <list type="bullet">
///   <item><c>Where(x =&gt; x.Name[0] == 'A')</c> — first-character match</item>
///   <item><c>Where(x =&gt; x.Name[2] == 'z')</c> — Nth-character match</item>
/// </list>
/// In .NET <c>s[i]</c> compiles to <c>String.get_Chars(i)</c>; the natural SQL
/// lowering is <c>SUBSTR(col, i+1, 1)</c> compared against a single-character
/// literal. Each provider already has the SUBSTR/SUBSTRING shape for 2/3-arg
/// <c>string.Substring</c>; we route through that.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqStringIndexerTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SiRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO SiRow VALUES
                (1, 'Alpha'),
                (2, 'Beta'),
                (3, 'gamma'),
                (4, 'azure');
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
    public async Task First_character_match_filters_rows_starting_with_letter()
    {
        // 'A' matches Alpha only (case-sensitive).
        var rows = await _ctx.Query<SiRow>().Where(r => r.Name[0] == 'A').ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Alpha", rows[0].Name);
    }

    [Fact]
    public async Task Nth_character_match_filters_by_indexed_letter()
    {
        // 'z' at index 1: "azure"[1] == 'z' → match. None of the others have 'z' at index 1.
        var rows = await _ctx.Query<SiRow>().Where(r => r.Name[1] == 'z').ToListAsync();
        Assert.Single(rows);
        Assert.Equal("azure", rows[0].Name);
    }

    [Table("SiRow")]
    public sealed class SiRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
