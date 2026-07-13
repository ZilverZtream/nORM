using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// LINQ Take with a non-positive count returns an empty sequence and Skip with a
/// negative count skips nothing. SQLite's LIMIT treats a negative value as
/// "unlimited", so a computed page size that goes non-positive must never reach the
/// SQL literally — paging code would silently return the whole table.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TakeSkipNonPositiveCountTests
{
    [Table("TakeSkip_Item")]
    private class Item
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TakeSkip_Item (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL
                );
                INSERT INTO TakeSkip_Item (Name) VALUES ('a'),('b'),('c'),('d');
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-5)]
    public void Take_non_positive_inline_returns_empty(int count)
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var rows = count switch
        {
            0 => ctx.Query<Item>().OrderBy(x => x.Id).Take(0).ToList(),
            -1 => ctx.Query<Item>().OrderBy(x => x.Id).Take(-1).ToList(),
            _ => ctx.Query<Item>().OrderBy(x => x.Id).Take(-5).ToList(),
        };
        Assert.Empty(rows);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-5)]
    public void Take_non_positive_closure_returns_empty(int count)
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var rows = ctx.Query<Item>().OrderBy(x => x.Id).Take(count).ToList();
        Assert.Empty(rows);
    }

    [Fact]
    public void Skip_negative_closure_skips_nothing()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var skip = -3;
        var rows = ctx.Query<Item>().OrderBy(x => x.Id).Skip(skip).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1, 2, 3, 4 }, rows);
    }

    [Fact]
    public void Skip_negative_then_take_positive_behaves_like_linq()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var skip = -3;
        var rows = ctx.Query<Item>().OrderBy(x => x.Id).Skip(skip).Take(2).Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1, 2 }, rows);
    }

    [Fact]
    public void Paged_take_with_non_positive_computed_size_returns_empty()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var pageSize = 0;
        var rows = ctx.Query<Item>().OrderBy(x => x.Id).Skip(2).Take(pageSize).ToList();
        Assert.Empty(rows);
    }

    // Compiled queries bind Take/Skip as REAL parameters (not folded literals), so the
    // count reaches the SQL at execution time — a negative value must clamp to LINQ
    // semantics there too, or SQLite's "negative LIMIT means unlimited" dumps the table.
    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<System.Collections.Generic.List<Item>>> _takeN =
        Norm.CompileQuery((DbContext c, int n) => c.Query<Item>().OrderBy(x => x.Id).Take(n));

    private static readonly Func<DbContext, int, System.Threading.Tasks.Task<System.Collections.Generic.List<Item>>> _skipN =
        Norm.CompileQuery((DbContext c, int n) => c.Query<Item>().OrderBy(x => x.Id).Skip(n));

    [Fact]
    public async System.Threading.Tasks.Task Compiled_query_take_negative_parameter_returns_empty()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        Assert.Equal(2, (await _takeN(ctx, 2)).Count);
        Assert.Empty(await _takeN(ctx, -1));
        Assert.Empty(await _takeN(ctx, 0));
    }

    [Fact]
    public async System.Threading.Tasks.Task Compiled_query_skip_negative_parameter_skips_nothing()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        Assert.Equal(2, (await _skipN(ctx, 2)).Count);
        Assert.Equal(4, (await _skipN(ctx, -3)).Count);
    }
}
