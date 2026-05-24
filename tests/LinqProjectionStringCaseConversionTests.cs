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
/// Strict pin for <c>string.ToUpper</c> and <c>string.ToLower</c> in
/// projection AND Where. SqliteProvider's TranslateFunction routes both to
/// UPPER / LOWER. Pins both directions so a regression that drops one
/// surfaces immediately (silent-wrongness: ToUpper -> identity returns raw
/// case-preserving rows; case-insensitive equality comparison would then
/// match nothing where it should).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringCaseConversionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PscItem VALUES
                (1, 'Alpha'),
                (2, 'BETA'),
                (3, 'gamma'),
                (4, 'MiXeD'),
                (5, '');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_ToUpper_uppercases_every_row()
    {
        var result = await _ctx.Query<PscItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.ToUpper())
            .ToListAsync();
        Assert.Equal(new[] { "ALPHA", "BETA", "GAMMA", "MIXED", "" }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_ToLower_lowercases_every_row()
    {
        var result = await _ctx.Query<PscItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.ToLower())
            .ToListAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma", "mixed", "" }, result.ToArray());
    }

    [Fact]
    public async Task Where_with_string_ToLower_enables_case_insensitive_equality()
    {
        // Silent-wrongness shape this catches: if ToLower silently becomes
        // identity, this returns 0 rows (raw 'MiXeD' != 'mixed').
        var result = await _ctx.Query<PscItem>()
            .Where(p => p.Name.ToLower() == "mixed")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_string_ToUpper_compared_to_uppercase_literal()
    {
        var result = await _ctx.Query<PscItem>()
            .Where(p => p.Name.ToUpper() == "GAMMA")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PscItem")]
    public sealed class PscItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
