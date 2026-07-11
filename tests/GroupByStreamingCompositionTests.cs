using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Raw GroupBy streaming (`GroupBy(key)` with no aggregate projection) groups
/// rows client-side after fetching them, so operators after it must apply to
/// the grouped sequence — a server-side COUNT/LIMIT/OFFSET on the flat rows
/// would count rows instead of groups or truncate a group's elements.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GroupByStreamingCompositionTests
{
    private class GbsItem
    {
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GbsItem(Id INTEGER, Category TEXT, Value INTEGER);" +
                "INSERT INTO GbsItem VALUES(1,'a',10);" +
                "INSERT INTO GbsItem VALUES(2,'a',20);" +
                "INSERT INTO GbsItem VALUES(3,'b',30);" +
                "INSERT INTO GbsItem VALUES(4,'c',40);" +
                "INSERT INTO GbsItem VALUES(5,'c',50);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Count_after_raw_group_by_counts_groups_not_rows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var count = ctx.Query<GbsItem>().OrderBy(x => x.Id).GroupBy(x => x.Category).Count();

        Assert.Equal(3, count);
    }

    [Fact]
    public void First_after_raw_group_by_returns_a_complete_group()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var first = ctx.Query<GbsItem>().OrderBy(x => x.Id).GroupBy(x => x.Category).First();

        // Category 'a' has TWO rows; a server LIMIT 1 would truncate the group.
        Assert.Equal("a", first.Key);
        Assert.Equal(2, first.Count());
    }

    [Fact]
    public void Skip_and_take_after_raw_group_by_page_whole_groups()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        var middle = ctx.Query<GbsItem>().OrderBy(x => x.Id).GroupBy(x => x.Category)
            .Skip(1).Take(1).ToList();

        var group = Assert.Single(middle);
        Assert.Equal("b", group.Key);
        Assert.Single(group);
    }

    [Fact]
    public void Any_after_raw_group_by_reflects_group_existence()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.True(ctx.Query<GbsItem>().OrderBy(x => x.Id).GroupBy(x => x.Category).Any());
    }
}
