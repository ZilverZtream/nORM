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
/// The LINQ idiom for "comma-separated list of children per group" is
/// <c>g => string.Join(sep, g.Select(x => x.Member))</c> inside a
/// <c>GroupBy</c> projection. None of the providers translated this
/// shape; the closest match was the elementwise <c>string.Join</c> over
/// a fixed-size NewArrayInit, which doesn't apply to a group sequence.
///
/// Add a provider hook GetStringAggregateSql(expr, sep) that emits the
/// per-provider aggregate, and recognise the
/// <c>string.Join(sep, group.Select(member))</c> shape in
/// ExpressionToSqlVisitor and SelectClauseVisitor before the generic
/// string.Join handler runs.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringAggregateGroupByProviderShapeTests : TestBase, IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GroupedRows (Id INTEGER PRIMARY KEY, Category TEXT NOT NULL, Name TEXT NOT NULL);
            INSERT INTO GroupedRows VALUES
                (1, 'A', 'ant'),
                (2, 'A', 'aardvark'),
                (3, 'B', 'bear'),
                (4, 'B', 'badger'),
                (5, 'B', 'bee');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GroupedRow>().HasKey(g => g.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task GroupBy_with_string_Join_over_group_Select_emits_aggregate_on_Sqlite()
    {
        // group.Select(x => x.Name) is IEnumerable<string>;
        // string.Join(", ", IEnumerable<string>) returns the aggregate.
        var result = await _ctx.Query<GroupedRow>()
            .GroupBy(g => g.Category)
            .Select(g => new { Cat = g.Key, Names = string.Join(", ", g.Select(x => x.Name)) })
            .OrderBy(r => r.Cat)
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal("A", result[0].Cat);
        // Order within the group is server-determined; assert membership not literal order.
        Assert.Contains("ant", result[0].Names);
        Assert.Contains("aardvark", result[0].Names);
        Assert.Contains(",", result[0].Names);
        Assert.Equal("B", result[1].Cat);
        Assert.Contains("bear", result[1].Names);
        Assert.Contains("badger", result[1].Names);
        Assert.Contains("bee", result[1].Names);
    }

    public sealed record GroupResult(string Cat, string Names);

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "GROUP_CONCAT")]
    [InlineData(ProviderKind.SqlServer, "STRING_AGG")]
    [InlineData(ProviderKind.Postgres,  "STRING_AGG")]
    [InlineData(ProviderKind.MySql,     "GROUP_CONCAT")]
    public void GroupBy_with_string_Join_emits_provider_aggregate_in_SQL(ProviderKind providerKind, string expectedFn)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // Positional record ctor compiles to NewExpression which the
        // GroupBy projection path recognises directly (the anonymous
        // type-with-cast shape doesn't unwrap through Convert).
        var (sql, _, _) = TranslateQuery<GroupedRow, GroupResult>(
            q => q.GroupBy(g => g.Category)
                  .Select(g => new GroupResult(g.Key, string.Join(", ", g.Select(x => x.Name)))),
            connection,
            provider);

        Assert.Contains(expectedFn + "(", sql);
    }

    [Table("GroupedRows")]
    public sealed class GroupedRow
    {
        [Key] public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }
}
