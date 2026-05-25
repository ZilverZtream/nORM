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
/// Strict pin + implement-first for <c>int.Parse(string)</c> in projection
/// AND Where. Common pattern: numeric values stored as text need parsing
/// for arithmetic. SQLite CAST(text AS INTEGER) handles this natively
/// (returns 0 for non-numeric text, matching nothing meaningful but
/// preventing crash).
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Returns the text unparsed -> type mismatch in projection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionIntParseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PipItem (Id INTEGER PRIMARY KEY, ValueText TEXT NOT NULL);
            INSERT INTO PipItem VALUES
                (1, '42'),
                (2, '100'),
                (3, '7'),
                (4, '999');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PipItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_int_Parse_string_column_projects_integer_per_row()
    {
        var result = await _ctx.Query<PipItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = int.Parse(p.ValueText) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.Equal(42, result[0].V);
        Assert.Equal(100, result[1].V);
        Assert.Equal(7, result[2].V);
        Assert.Equal(999, result[3].V);
    }

    [Fact]
    public async Task Where_int_Parse_string_column_compared_to_threshold_filters_rows()
    {
        // int.Parse(text) > 50 -> {2 (100), 4 (999)}.
        var result = await _ctx.Query<PipItem>()
            .Where(p => int.Parse(p.ValueText) > 50)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PipItem")]
    public sealed class PipItem
    {
        [Key] public int Id { get; set; }
        public string ValueText { get; set; } = string.Empty;
    }
}
