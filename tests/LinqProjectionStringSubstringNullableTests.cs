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
/// Pin for <c>p.NullableString.Substring(start, length)</c> in projection
/// when the column is NULL on some rows. SQLite SUBSTR returns NULL when
/// the input is NULL (propagates), so the materializer should round-trip
/// the null cleanly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringSubstringNullableTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PssnItem (Id INTEGER PRIMARY KEY, V TEXT);
            INSERT INTO PssnItem VALUES
                (1, 'hello'),
                (2, NULL),
                (3, 'world');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PssnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Substring_on_nullable_column_returns_null_for_null_rows()
    {
        var r = await _ctx.Query<PssnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.V!.Substring(0, 3) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal("hel", r[0].S);
        Assert.Null(r[1].S);
        Assert.Equal("wor", r[2].S);
    }

    [Table("PssnItem")]
    public sealed class PssnItem
    {
        [Key] public int Id { get; set; }
        public string? V { get; set; }
    }
}
