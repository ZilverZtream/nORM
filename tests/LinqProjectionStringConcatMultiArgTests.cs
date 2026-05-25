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
/// Strict pins for <c>string.Concat</c> with 3+ arguments. The 2-arg
/// overload routes through binary <c>+</c> already, but the
/// 3-arg / 4-arg / params overloads call <c>String.Concat(string,
/// string, string)</c> or <c>String.Concat(params string[])</c> which
/// the provider's string switch may not handle as a chain.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringConcatMultiArgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscmItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL, C TEXT NOT NULL);
            INSERT INTO PscmItem VALUES
                (1, 'foo', 'bar', 'baz'),
                (2, 'hello', '-', 'world');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Concat_three_columns_returns_concatenation_per_row()
    {
        var r = await _ctx.Query<PscmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = string.Concat(p.A, p.B, p.C) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("foobarbaz", r[0].V);
        Assert.Equal("hello-world", r[1].V);
    }

    [Fact]
    public async Task Select_string_Concat_four_columns_returns_concatenation_per_row()
    {
        var r = await _ctx.Query<PscmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = string.Concat(p.A, p.B, p.C, "!") }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("foobarbaz!", r[0].V);
        Assert.Equal("hello-world!", r[1].V);
    }

    [Table("PscmItem")]
    public sealed class PscmItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
        public string C { get; set; } = string.Empty;
    }
}
