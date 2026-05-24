using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of the case-insensitive string predicates that
/// are common in user search code:
/// <list type="bullet">
///   <item><c>s.Equals(other, StringComparison.OrdinalIgnoreCase)</c></item>
///   <item><c>s.Contains(other, StringComparison.OrdinalIgnoreCase)</c></item>
///   <item><c>s.StartsWith(other, StringComparison.OrdinalIgnoreCase)</c></item>
///   <item><c>s.EndsWith(other, StringComparison.OrdinalIgnoreCase)</c></item>
/// </list>
/// These overloads lower to LOWER(col) = LOWER(literal) / LIKE pattern variants
/// rather than throwing — without that, `Where(u =&gt; u.Email.Equals(query,
/// StringComparison.OrdinalIgnoreCase))` would force users into manual
/// <c>ToLower()</c> chains.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqStringCaseInsensitiveTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CiRow (Id INTEGER PRIMARY KEY, Email TEXT NOT NULL);
            INSERT INTO CiRow VALUES
                (1, 'Alice@Example.com'),
                (2, 'bob@example.com'),
                (3, 'CAROL@EXAMPLE.COM'),
                (4, 'dave@other.com');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Equals_with_ordinal_ignore_case_matches_regardless_of_casing()
    {
        var rows = await _ctx.Query<CiRow>()
            .Where(r => r.Email.Equals("alice@example.com", StringComparison.OrdinalIgnoreCase))
            .ToListAsync();
        Assert.Single(rows);
        Assert.Equal("Alice@Example.com", rows[0].Email);
    }

    [Fact]
    public async Task Contains_with_ordinal_ignore_case_matches_substring_regardless_of_casing()
    {
        // Three rows have an example.com suffix in some casing.
        var rows = (await _ctx.Query<CiRow>()
            .Where(r => r.Email.Contains("EXAMPLE.COM", StringComparison.OrdinalIgnoreCase))
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Alice@Example.com", rows[0].Email);
        Assert.Equal("bob@example.com", rows[1].Email);
        Assert.Equal("CAROL@EXAMPLE.COM", rows[2].Email);
    }

    [Fact]
    public async Task StartsWith_with_ordinal_ignore_case_matches_prefix_regardless_of_casing()
    {
        var rows = (await _ctx.Query<CiRow>()
            .Where(r => r.Email.StartsWith("CAROL", StringComparison.OrdinalIgnoreCase))
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("CAROL@EXAMPLE.COM", rows[0].Email);
    }

    [Fact]
    public async Task EndsWith_with_ordinal_ignore_case_matches_suffix_regardless_of_casing()
    {
        var rows = (await _ctx.Query<CiRow>()
            .Where(r => r.Email.EndsWith("EXAMPLE.COM", StringComparison.OrdinalIgnoreCase))
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(3, rows.Length);
    }

    [Table("CiRow")]
    public sealed class CiRow
    {
        [Key] public int Id { get; set; }
        public string Email { get; set; } = string.Empty;
    }
}
