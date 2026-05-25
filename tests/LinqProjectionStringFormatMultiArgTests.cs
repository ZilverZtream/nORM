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
/// Probe pin for `string.Format("{0} {1}", col1, col2)` in projection.
/// LINQ-to-Objects formats per-row; the translator should either lower
/// to provider concat (SQLite `col1 || ' ' || col2`) or fall back to
/// client-eval. Verify it produces the expected formatted strings.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringFormatMultiArgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsfmItem (Id INTEGER PRIMARY KEY, First TEXT NOT NULL, Last TEXT NOT NULL);
            INSERT INTO PsfmItem VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsfmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Format_two_column_args_produces_formatted_per_row()
    {
        var rows = await _ctx.Query<PsfmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Full = string.Format("{0} {1}", p.First, p.Last) })
            .ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice Smith", rows[0].Full);
        Assert.Equal("Bob Jones",   rows[1].Full);
    }

    [Table("PsfmItem")]
    public sealed class PsfmItem
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public string Last { get; set; } = string.Empty;
    }
}
