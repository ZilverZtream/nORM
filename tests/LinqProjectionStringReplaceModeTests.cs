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
/// Strict pin for <c>string.Replace(old, new, StringComparison)</c> in
/// projection. SQLite REPLACE is case-sensitive; the ignore-case
/// variant needs LOWER-substring detection. For simplicity we only
/// honour the case-sensitive variants (Ordinal/CurrentCulture/
/// InvariantCulture) which collapse to plain REPLACE; ignore-case
/// modes throw an explicit unsupported-feature error rather than
/// silently emitting case-sensitive REPLACE.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringReplaceModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsrmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PsrmItem VALUES
                (1, 'Hello World'),
                (2, 'goodbye World'),
                (3, 'no match here');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsrmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Replace_Ordinal_substitutes_case_sensitively()
    {
        var r = await _ctx.Query<PsrmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = p.V.Replace("World", "Earth", StringComparison.Ordinal) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal("Hello Earth", r[0].V);
        Assert.Equal("goodbye Earth", r[1].V);
        Assert.Equal("no match here", r[2].V);
    }

    [Table("PsrmItem")]
    public sealed class PsrmItem
    {
        [Key] public int Id { get; set; }
        public string V { get; set; } = string.Empty;
    }
}
