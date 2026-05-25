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
/// Strict pin for <c>string.Join(separator, ...)</c> with multiple
/// column arguments in projection. The variadic params-string[] form
/// is most common in templating shapes ("a-b-c" joined). Translates
/// to interleaved SQL concatenation: c1 || sep || c2 || sep || c3.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringJoinMultiArgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsjmItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL, C TEXT NOT NULL);
            INSERT INTO PsjmItem VALUES
                (1, 'foo', 'bar', 'baz'),
                (2, 'x', 'y', 'z');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsjmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Join_dash_separator_three_columns_returns_joined_per_row()
    {
        var r = await _ctx.Query<PsjmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = string.Join("-", p.A, p.B, p.C) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("foo-bar-baz", r[0].V);
        Assert.Equal("x-y-z", r[1].V);
    }

    [Table("PsjmItem")]
    public sealed class PsjmItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
        public string C { get; set; } = string.Empty;
    }
}
