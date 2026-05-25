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
/// Probe pin for `Select(p =&gt; string.Concat(new[] { p.A, p.B, p.C }))`
/// in projection. The expression tree includes a NewArrayInit node;
/// string.Concat over a string[] would lower to a chain of provider
/// concat (col1 || col2 || col3 on SQLite).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringConcatArrayTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscaItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL, C TEXT NOT NULL);
            INSERT INTO PscaItem VALUES (1, 'foo', 'bar', 'baz'), (2, 'x', 'y', 'z');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Concat_array_of_column_refs_concatenates_per_row()
    {
        var rows = await _ctx.Query<PscaItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Cat = string.Concat(new[] { p.A, p.B, p.C }) })
            .ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("foobarbaz", rows[0].Cat);
        Assert.Equal("xyz",       rows[1].Cat);
    }

    [Table("PscaItem")]
    public sealed class PscaItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
        public string C { get; set; } = string.Empty;
    }
}
