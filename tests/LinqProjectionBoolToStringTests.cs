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
/// Strict pin + implement-first for <c>bool.ToString()</c> in projection.
/// .NET's bool.ToString returns "True" / "False" (capitalized). SQLite has
/// no bool type so the column is stored as INTEGER 0 / 1; the generic
/// ToString fall-through to CAST AS TEXT returns "0" / "1" -- wrong shape.
///
/// Silent-wrongness shapes:
///   * Returns "0" / "1" (default CAST) instead of "False" / "True".
///   * Returns "false" / "true" lowercase (wrong .NET-parity).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionBoolToStringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PbtsItem (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL);
            INSERT INTO PbtsItem VALUES
                (1, 1),
                (2, 0),
                (3, 1);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PbtsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_bool_ToString_returns_capitalized_True_False_strings()
    {
        var result = await _ctx.Query<PbtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Active.ToString() })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal("True", result[0].S);
        Assert.Equal("False", result[1].S);
        Assert.Equal("True", result[2].S);
    }

    [Table("PbtsItem")]
    public sealed class PbtsItem
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }
}
