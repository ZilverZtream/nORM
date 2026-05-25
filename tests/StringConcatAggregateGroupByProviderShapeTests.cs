using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Companion of <c>c1023ce</c> -- <c>string.Join(sep, group.Select(...))</c>
/// already lowers to STRING_AGG / GROUP_CONCAT. The empty-separator form
/// <c>string.Concat(group.Select(x =&gt; x.Member))</c> takes the same
/// IEnumerable&lt;string&gt; argument shape but reaches the translator
/// through string.Concat -- which the existing handler only matches in
/// its variadic / 2-arg / 3-arg / 4-arg forms, NOT the
/// IEnumerable&lt;string&gt; overload. Lower it through the same per-
/// provider aggregate hook with an empty-string separator literal so
/// both shapes share the emit path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringConcatAggregateGroupByProviderShapeTests : TestBase, IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Chars (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Token TEXT NOT NULL);
            INSERT INTO Chars VALUES
                (1, 'A', 'x'),
                (2, 'A', 'y'),
                (3, 'A', 'z'),
                (4, 'B', 'p'),
                (5, 'B', 'q');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CharRow>().HasKey(c => c.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    public sealed record ConcatResult(string Cat, string Tokens);

    [Fact]
    public async Task GroupBy_with_string_Concat_over_group_Select_emits_aggregate_on_Sqlite()
    {
        var result = await _ctx.Query<CharRow>()
            .GroupBy(g => g.Category)
            .Select(g => new ConcatResult(g.Key, string.Concat(g.Select(x => x.Token))))
            .OrderBy(r => r.Cat)
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal("A", result[0].Cat);
        // Order within group is server-determined; assert character set membership.
        Assert.Equal(3, result[0].Tokens.Length);
        Assert.Contains('x', result[0].Tokens);
        Assert.Contains('y', result[0].Tokens);
        Assert.Contains('z', result[0].Tokens);
        Assert.Equal(2, result[1].Tokens.Length);
        Assert.Contains('p', result[1].Tokens);
        Assert.Contains('q', result[1].Tokens);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "GROUP_CONCAT")]
    [InlineData(ProviderKind.SqlServer, "STRING_AGG")]
    [InlineData(ProviderKind.Postgres,  "STRING_AGG")]
    [InlineData(ProviderKind.MySql,     "GROUP_CONCAT")]
    public void GroupBy_with_string_Concat_emits_provider_aggregate_with_empty_separator(ProviderKind providerKind, string expectedFn)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<CharRow, ConcatResult>(
            q => q.GroupBy(g => g.Category)
                  .Select(g => new ConcatResult(g.Key, string.Concat(g.Select(x => x.Token)))),
            connection,
            provider);

        Assert.Contains(expectedFn + "(", sql);
        // Empty separator literal -- '' in SQL.
        Assert.Contains("''", sql);
    }

    [Table("Chars")]
    public sealed class CharRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Token { get; set; } = string.Empty;
    }
}
