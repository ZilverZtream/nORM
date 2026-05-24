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
/// Pins translation of <c>string.Format("template", args)</c> when the template
/// uses only positional placeholders (<c>{0}</c>, <c>{1}</c>, …) with no format
/// specifiers. The shape lowers to a provider concat of the literal pieces and
/// the argument expressions:
/// <code>string.Format("Hello {0}!", x.Name)  →  'Hello ' || x.Name || '!'</code>
/// Format specifiers (<c>{0:N2}</c>, <c>{0,5}</c>) stay client-side because no
/// provider has a portable equivalent.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqStringFormatTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FmRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO FmRow VALUES
                (1, 'Alice', 42),
                (2, 'Bob',   7),
                (3, 'Carol', 99);
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
    public async Task Format_with_single_positional_placeholder_emits_concat()
    {
        var greetings = (await _ctx.Query<FmRow>()
            .Select(r => string.Format("Hello {0}!", r.Name))
            .ToListAsync())
            .OrderBy(s => s).ToArray();
        Assert.Equal(3, greetings.Length);
        Assert.Equal("Hello Alice!", greetings[0]);
        Assert.Equal("Hello Bob!",   greetings[1]);
        Assert.Equal("Hello Carol!", greetings[2]);
    }

    [Fact]
    public async Task Format_with_two_placeholders_interleaves_literals_and_args()
    {
        // Score is int — must round-trip through provider's ToString CAST so concat works.
        var lines = (await _ctx.Query<FmRow>()
            .Select(r => string.Format("{0} scored {1}", r.Name, r.Score))
            .ToListAsync())
            .OrderBy(s => s).ToArray();
        Assert.Equal(3, lines.Length);
        Assert.Equal("Alice scored 42", lines[0]);
        Assert.Equal("Bob scored 7",    lines[1]);
        Assert.Equal("Carol scored 99", lines[2]);
    }

    [Table("FmRow")]
    public sealed class FmRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
