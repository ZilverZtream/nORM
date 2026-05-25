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
/// Pins translation of <c>char.ToUpperInvariant(c)</c> and
/// <c>char.ToLowerInvariant(c)</c> applied to a character extracted from a
/// string column. Both factory methods need to lower to a provider-native
/// UPPER/LOWER applied to the single-char substring.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCharCaseInvariantTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CciRow (Id INTEGER PRIMARY KEY, S TEXT NOT NULL);
            INSERT INTO CciRow VALUES
                (1, 'apple'),
                (2, 'BANANA'),
                (3, 'Cherry');
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
    public async Task ToUpperInvariant_on_first_char_returns_uppercase()
    {
        var rows = (await _ctx.Query<CciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, First = char.ToUpperInvariant(r.S[0]) })
            .ToListAsync())
            .ToArray();
        Assert.Equal('A', rows[0].First);
        Assert.Equal('B', rows[1].First);
        Assert.Equal('C', rows[2].First);
    }

    [Fact]
    public async Task ToLowerInvariant_on_first_char_returns_lowercase()
    {
        var rows = (await _ctx.Query<CciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, First = char.ToLowerInvariant(r.S[0]) })
            .ToListAsync())
            .ToArray();
        Assert.Equal('a', rows[0].First);
        Assert.Equal('b', rows[1].First);
        Assert.Equal('c', rows[2].First);
    }

    [Fact]
    public async Task String_ToUpperInvariant_on_column_returns_uppercase()
    {
        var rows = (await _ctx.Query<CciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, U = r.S.ToUpperInvariant() })
            .ToListAsync())
            .ToArray();
        Assert.Equal("APPLE",  rows[0].U);
        Assert.Equal("BANANA", rows[1].U);
        Assert.Equal("CHERRY", rows[2].U);
    }

    [Fact]
    public async Task String_ToLowerInvariant_on_column_returns_lowercase()
    {
        var rows = (await _ctx.Query<CciRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, L = r.S.ToLowerInvariant() })
            .ToListAsync())
            .ToArray();
        Assert.Equal("apple",  rows[0].L);
        Assert.Equal("banana", rows[1].L);
        Assert.Equal("cherry", rows[2].L);
    }

    [Table("CciRow")]
    public sealed class CciRow
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = string.Empty;
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqCharCaseInvariantLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task String_ToUpperInvariant_on_column_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LiveCciRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, U = r.S.ToUpperInvariant() })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal("APPLE",  rows[0].U);
                Assert.Equal("BANANA", rows[1].U);
                Assert.Equal("CHERRY", rows[2].U);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task String_ToLowerInvariant_on_column_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LiveCciRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, L = r.S.ToLowerInvariant() })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal("apple",  rows[0].L);
                Assert.Equal("banana", rows[1].L);
                Assert.Equal("cherry", rows[2].L);
            }
            finally { await Teardown(ctx); }
        }
    }

    private static async Task Setup(DbContext ctx, ProviderKind kind)
    {
        await Teardown(ctx);
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = kind switch
        {
            ProviderKind.SqlServer => "CREATE TABLE LiveCciRow (Id INT PRIMARY KEY, S NVARCHAR(50) NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveCciRow\" (\"Id\" INT PRIMARY KEY, \"S\" VARCHAR(50) NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveCciRow (Id INT PRIMARY KEY, S VARCHAR(50) NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveCciRow\" VALUES (1,'apple'),(2,'BANANA'),(3,'Cherry');"
            : "INSERT INTO LiveCciRow VALUES (1,'apple'),(2,'BANANA'),(3,'Cherry');";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveCciRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveCciRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveCciRow")]
    public sealed class LiveCciRow
    {
        [Key] public int Id { get; set; }
        public string S { get; set; } = string.Empty;
    }
}
