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
/// Verify pin for the <c>+</c> operator on string column + string literal
/// in projection (common templating: <c>p.Name + " (archived)"</c>). The
/// C# compiler lowers this to BinaryExpression with NodeType.Add on string
/// operands; SCV maps that to SQLite's <c>||</c> concat operator.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringConcatPlusTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscpItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PscpItem VALUES
                (1, 'alpha'),
                (2, 'beta');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_plus_literal_suffix_returns_concatenated_per_row()
    {
        var r = await _ctx.Query<PscpItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = p.Name + " (archived)" }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("alpha (archived)", r[0].V);
        Assert.Equal("beta (archived)", r[1].V);
    }

    [Fact]
    public async Task Select_literal_prefix_plus_string_returns_concatenated_per_row()
    {
        var r = await _ctx.Query<PscpItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = ">>" + p.Name }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(">>alpha", r[0].V);
        Assert.Equal(">>beta", r[1].V);
    }

    [Table("PscpItem")]
    public sealed class PscpItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
