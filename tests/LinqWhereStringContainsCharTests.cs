using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pin + implement-first for the <c>string.Contains(char)</c>
/// overload in Where (added in .NET Core 2.1). The (string) overload is
/// already wired; the (char) overload is a separate MethodInfo so the
/// fast-handler dispatcher misses it. Common pattern: split a CSV-like
/// column without splitting -- "does this column contain the ',' char".
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw -> blocks the query.
///   * Char arg widening to int (the char's code point as integer) ->
///     SQL bind binds 44 instead of ',' -> Contains never matches.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringContainsCharTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsccItem (Id INTEGER PRIMARY KEY, Tags TEXT NOT NULL);
            INSERT INTO WsccItem VALUES
                (1, 'alpha'),
                (2, 'red,blue'),
                (3, 'no-comma-here'),
                (4, 'one,two,three');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WsccItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_string_Contains_comma_char_filters_rows_with_comma()
    {
        var result = await _ctx.Query<WsccItem>()
            .Where(i => i.Tags.Contains(','))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_string_Contains_hyphen_char_filters_to_dashed_row()
    {
        var result = await _ctx.Query<WsccItem>()
            .Where(i => i.Tags.Contains('-'))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WsccItem")]
    public sealed class WsccItem
    {
        [Key] public int Id { get; set; }
        public string Tags { get; set; } = string.Empty;
    }
}
