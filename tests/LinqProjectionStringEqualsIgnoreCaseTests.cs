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
/// Strict pin + implement-first for <c>string.Equals(a, b, StringComparison
/// .OrdinalIgnoreCase)</c> in projection. ExpressionToSqlVisitor's fast
/// handler dispatch wires HandleStringEqualsStaticWithComparison /
/// HandleStringEqualsInstanceWithComparison for the Where path, but
/// projection routes through provider.TranslateFunction. Same Where-vs-
/// projection parity gap shape as d1e9fc5 / 0adce6a / 9a445c9 / ce826da /
/// 63cf807.
///
/// Silent-wrongness shapes:
///   * Equals collapsing to '=' (case-sensitive) -> 'MIXED' != 'mixed'
///     returns false for what should be a case-insensitive match.
///   * StringComparison arg silently ignored -> projection returns the
///     OrdinalIgnoreCase semantic only sometimes (e.g. only when LOWER is
///     wrapped on both sides).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringEqualsIgnoreCaseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PseiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PseiItem VALUES
                (1, 'mixed'),
                (2, 'MIXED'),
                (3, 'MiXeD'),
                (4, 'other');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PseiItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Equals_static_OrdinalIgnoreCase_returns_true_for_case_variants()
    {
        var result = await _ctx.Query<PseiItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, M = string.Equals(p.Name, "mixed", StringComparison.OrdinalIgnoreCase) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].M);  // 'mixed'
        Assert.True(result[1].M);  // 'MIXED'
        Assert.True(result[2].M);  // 'MiXeD'
        Assert.False(result[3].M); // 'other'
    }

    [Fact]
    public async Task Select_string_Equals_instance_OrdinalIgnoreCase_returns_true_for_case_variants()
    {
        var result = await _ctx.Query<PseiItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, M = p.Name.Equals("mixed", StringComparison.OrdinalIgnoreCase) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].M);
        Assert.True(result[1].M);
        Assert.True(result[2].M);
        Assert.False(result[3].M);
    }

    [Table("PseiItem")]
    public sealed class PseiItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
