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
/// Strict pin for <c>string.IndexOf(s, StringComparison)</c> in projection.
/// SQLite INSTR is BINARY by default; mirroring the Contains-with-mode
/// pattern from e9de94e, an IgnoreCase variant lowers both sides before
/// the INSTR call. Returns 0-based index or -1, matching .NET's
/// IndexOf semantics (SQLite INSTR returns 1-based or 0; subtract 1).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringIndexOfModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsioItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PsioItem VALUES
                (1, 'Hello World'),
                (2, 'goodbye'),
                (3, 'CherryWORLD');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsioItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_IndexOf_OrdinalIgnoreCase_finds_position_case_insensitive()
    {
        var r = await _ctx.Query<PsioItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, P = p.V.IndexOf("world", StringComparison.OrdinalIgnoreCase) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(6, r[0].P);    // "Hello World" -- index 6
        Assert.Equal(-1, r[1].P);   // "goodbye" -- not found
        Assert.Equal(6, r[2].P);    // "CherryWORLD" -- index 6
    }

    [Fact]
    public async Task Select_string_IndexOf_Ordinal_is_case_sensitive()
    {
        var r = await _ctx.Query<PsioItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, P = p.V.IndexOf("World", StringComparison.Ordinal) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(6, r[0].P);    // matches
        Assert.Equal(-1, r[1].P);
        Assert.Equal(-1, r[2].P);   // "WORLD" all-caps differs from "World"
    }

    [Table("PsioItem")]
    public sealed class PsioItem
    {
        [Key] public int Id { get; set; }
        public string V { get; set; } = string.Empty;
    }
}
