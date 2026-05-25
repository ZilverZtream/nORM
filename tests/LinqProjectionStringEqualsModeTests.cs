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
/// Strict pin for <c>string.Equals(a, b, StringComparison)</c> in projection
/// (static form) and the instance form <c>a.Equals(b, StringComparison)</c>.
/// Same COLLATE BINARY/NOCASE dispatch as the Compare-with-mode fix
/// (892590f); emit is just `a COLLATE X = b COLLATE X` boolean rather
/// than the -1/0/1 CASE.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringEqualsModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsemItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PsemItem VALUES
                (1, 'apple', 'APPLE'),
                (2, 'banana', 'cherry'),
                (3, 'CHERRY', 'cherry');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsemItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Equals_static_OrdinalIgnoreCase_treats_case_as_equal()
    {
        var r = await _ctx.Query<PsemItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, E = string.Equals(p.A, p.B, StringComparison.OrdinalIgnoreCase) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].E);    // apple == APPLE
        Assert.False(r[1].E);   // banana != cherry
        Assert.True(r[2].E);    // CHERRY == cherry
    }

    [Fact]
    public async Task Select_string_Equals_instance_Ordinal_is_case_sensitive()
    {
        var r = await _ctx.Query<PsemItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, E = p.A.Equals(p.B, StringComparison.Ordinal) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.False(r[0].E);   // 'apple' != 'APPLE' under Ordinal
        Assert.False(r[1].E);
        Assert.False(r[2].E);   // 'CHERRY' != 'cherry' under Ordinal
    }

    [Table("PsemItem")]
    public sealed class PsemItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
    }
}
