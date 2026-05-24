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
/// Strict pin + implement-first for <c>char.IsDigit</c> / <c>IsLetter</c> /
/// <c>IsWhiteSpace</c> in projection. These work in Where via
/// ExpressionToSqlVisitor's ASCII-range emission but were not mirrored in
/// SelectClauseVisitor, leaving the projection path to throw
/// NormUnsupportedFeatureException. The fix mirrors the Where shape so the
/// same SQL boolean (0/1) materializes back to bool.
///
/// Silent-wrongness shapes:
///   * IsDigit collapsing to identity returns the char itself (which would
///     be silently boxed) instead of a bool -- type mismatch surfaces only
///     at materialize time.
///   * IsWhiteSpace dropping the tab/LF/CR cases reduces to space-only --
///     would silently miss whitespace in tab-separated content.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharIsDigitLetterTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PclItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PclItem VALUES
                (1, '7abc'),
                (2, 'Abc'),
                (3, ' xyz'),
                (4, '$dollar');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PclItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_IsDigit_first_char_projects_bool_per_row()
    {
        var result = await _ctx.Query<PclItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, IsDig = char.IsDigit(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].IsDig);   // '7'
        Assert.False(result[1].IsDig);  // 'A'
        Assert.False(result[2].IsDig);  // ' '
        Assert.False(result[3].IsDig);  // '$'
    }

    [Fact]
    public async Task Select_char_IsLetter_first_char_projects_bool_per_row()
    {
        var result = await _ctx.Query<PclItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, IsLet = char.IsLetter(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.False(result[0].IsLet); // '7'
        Assert.True(result[1].IsLet);  // 'A'
        Assert.False(result[2].IsLet); // ' '
        Assert.False(result[3].IsLet); // '$'
    }

    [Fact]
    public async Task Select_char_IsWhiteSpace_first_char_projects_bool_per_row()
    {
        var result = await _ctx.Query<PclItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, IsWs = char.IsWhiteSpace(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.False(result[0].IsWs); // '7'
        Assert.False(result[1].IsWs); // 'A'
        Assert.True(result[2].IsWs);  // ' '
        Assert.False(result[3].IsWs); // '$'
    }

    [Table("PclItem")]
    public sealed class PclItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
