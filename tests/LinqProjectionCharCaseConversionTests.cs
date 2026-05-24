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
/// Strict pin + implement-first for <c>char.ToUpper(c)</c> / <c>char.ToLower(c)</c>
/// in projection. Works in Where via ExpressionToSqlVisitor's char-static
/// handler near the IsDigit/IsLetter branch, but projection routes through
/// provider.TranslateFunction which returned null -- same Where-vs-projection
/// parity gap shape as the d1e9fc5 / 0adce6a / 9a445c9 / ce826da runs.
///
/// Silent-wrongness shapes:
///   * char.ToUpper(c) collapsing to UPPER(c) on a single-char column works
///     fine since SQLite UPPER handles 1-char input. The risk is the SCV
///     fall-through emitting raw 'TOUPPER(...)' (no SQLite function) and
///     crashing instead of returning the char.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharCaseConversionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PcccItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PcccItem VALUES
                (1, 'alpha'),
                (2, 'Beta'),
                (3, 'GAMMA');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PcccItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_ToUpper_first_char_projects_uppercase_per_row()
    {
        var result = await _ctx.Query<PcccItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = char.ToUpper(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(new[] { 'A', 'B', 'G' }, result.Select(r => r.C).ToArray());
    }

    [Fact]
    public async Task Select_char_ToLower_first_char_projects_lowercase_per_row()
    {
        var result = await _ctx.Query<PcccItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = char.ToLower(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(new[] { 'a', 'b', 'g' }, result.Select(r => r.C).ToArray());
    }

    [Table("PcccItem")]
    public sealed class PcccItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
