using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies LINQ paging-count semantics: Take with a non-positive count returns an
/// empty sequence and Skip with a negative count skips nothing (Enumerable.Take/Skip
/// never throw for out-of-range counts), while zero is valid.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class PagingBoundaryTests
{
    private class PagingItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection, DbContext) CreateSeededContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PagingItem (Id INTEGER PRIMARY KEY, Name TEXT);" +
                              "INSERT INTO PagingItem (Name) VALUES ('a'),('b'),('c')";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Skip_Negative_SkipsNothing()
    {
        var (cn, ctx) = CreateSeededContext();
        using var _ = cn; using var __ = ctx;
        Assert.Equal(3, ctx.Query<PagingItem>().Skip(-1).ToList().Count);
    }

    [Fact]
    public void Take_Negative_ReturnsEmpty()
    {
        var (cn, ctx) = CreateSeededContext();
        using var _ = cn; using var __ = ctx;
        Assert.Empty(ctx.Query<PagingItem>().Take(-1).ToList());
    }

    [Fact]
    public void Skip_Negative_Large_SkipsNothing()
    {
        var (cn, ctx) = CreateSeededContext();
        using var _ = cn; using var __ = ctx;
        Assert.Equal(3, ctx.Query<PagingItem>().Skip(-100).ToList().Count);
    }

    [Fact]
    public void Take_Negative_Large_ReturnsEmpty()
    {
        var (cn, ctx) = CreateSeededContext();
        using var _ = cn; using var __ = ctx;
        Assert.Empty(ctx.Query<PagingItem>().Take(-5).ToList());
    }

    [Fact]
    public void Skip_And_Take_BothNegative_ReturnsEmpty()
    {
        // Skip(-100) skips nothing; Take(-5) then yields the empty window.
        var (cn, ctx) = CreateSeededContext();
        using var _ = cn; using var __ = ctx;
        Assert.Empty(ctx.Query<PagingItem>().Skip(-100).Take(-5).ToList());
    }

    [Fact]
    public void Skip_Zero_IsValid_DoesNotThrow()
    {
        // Create in-memory DB with a table so we don't fail on missing table.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PagingItem (Id INTEGER PRIMARY KEY, Name TEXT)";
        cmd.ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = ctx.Query<PagingItem>().Skip(0).ToList();
        Assert.Empty(result);
    }

    [Fact]
    public void Take_Zero_IsValid_DoesNotThrow()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE PagingItem (Id INTEGER PRIMARY KEY, Name TEXT)";
        cmd.ExecuteNonQuery();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var result = ctx.Query<PagingItem>().Take(0).ToList();
        Assert.Empty(result);
    }
}
