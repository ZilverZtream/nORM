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
/// Strict pins for <c>char.ToUpper(c) / char.ToLower(c)</c> static methods
/// in projection. Sisters of string.ToUpper/ToLower, which already map.
/// Common shape: project the case-folded first letter of a name column
/// for indexing or grouping. Maps to SQLite UPPER/LOWER which operate
/// on single-char strings unchanged.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharToUpperLowerTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PctulItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PctulItem VALUES
                (1, 'alpha'),
                (2, 'BETA'),
                (3, 'gamma');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PctulItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_ToUpper_first_letter_returns_uppercased_per_row()
    {
        var r = await _ctx.Query<PctulItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = char.ToUpper(p.Name[0]) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal('A', r[0].C);
        Assert.Equal('B', r[1].C);
        Assert.Equal('G', r[2].C);
    }

    [Fact]
    public async Task Select_char_ToLower_first_letter_returns_lowercased_per_row()
    {
        var r = await _ctx.Query<PctulItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = char.ToLower(p.Name[0]) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal('a', r[0].C);
        Assert.Equal('b', r[1].C);
        Assert.Equal('g', r[2].C);
    }

    [Table("PctulItem")]
    public sealed class PctulItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
