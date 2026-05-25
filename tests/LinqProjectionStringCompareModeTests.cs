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
/// Strict pin for the 3-arg <c>string.Compare(a, b, StringComparison)</c>
/// overload in projection. Same -1/0/1 sign contract as the 2-arg form
/// but dispatched on the StringComparison enum:
///   Ordinal / CurrentCulture / InvariantCulture            -> COLLATE BINARY
///   OrdinalIgnoreCase / CurrentCultureIgnoreCase / etc IC  -> COLLATE NOCASE
/// SQLite's BINARY collation is byte-wise (matches Ordinal); NOCASE folds
/// ASCII case (matches OrdinalIgnoreCase well enough for typical query
/// data -- full Unicode case folding is out of scope without ICU).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringCompareModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscmoItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PscmoItem VALUES
                (1, 'apple', 'APPLE'),
                (2, 'banana', 'cherry'),
                (3, 'CHERRY', 'cherry');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscmoItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Compare_OrdinalIgnoreCase_treats_case_as_equal()
    {
        var r = await _ctx.Query<PscmoItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = string.Compare(p.A, p.B, StringComparison.OrdinalIgnoreCase) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(0, r[0].C);    // apple == APPLE
        Assert.True(r[1].C < 0);    // banana < cherry
        Assert.Equal(0, r[2].C);    // CHERRY == cherry
    }

    [Fact]
    public async Task Select_string_Compare_Ordinal_is_case_sensitive()
    {
        var r = await _ctx.Query<PscmoItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = string.Compare(p.A, p.B, StringComparison.Ordinal) }).ToListAsync();
        Assert.Equal(3, r.Count);
        // Sign only -- magnitude differs between .NET and SQLite for ASCII
        // case differences but both agree on direction.
        Assert.True(r[0].C > 0);    // 'a' (0x61) > 'A' (0x41)
        Assert.True(r[1].C < 0);    // banana < cherry
        Assert.True(r[2].C < 0);    // 'C' (0x43) < 'c' (0x63)
    }

    [Table("PscmoItem")]
    public sealed class PscmoItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
    }
}
