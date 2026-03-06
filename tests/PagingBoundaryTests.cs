using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQL-1: Verifies that negative Skip/Take values throw ArgumentOutOfRangeException
/// at query translation time, while zero is valid.
/// </summary>
public class PagingBoundaryTests
{
    private class PagingItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static IQueryable<PagingItem> CreateQuery()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        return ctx.Query<PagingItem>();
    }

    private static void ForceTranslation(IQueryable<PagingItem> query)
    {
        // Enumerate to trigger translation (will throw at translation time before hitting DB)
        _ = query.ToList();
    }

    [Fact]
    public void Skip_Negative_ThrowsArgumentOutOfRangeException()
    {
        var q = CreateQuery().Skip(-1);
        Assert.Throws<ArgumentOutOfRangeException>(() => ForceTranslation(q));
    }

    [Fact]
    public void Take_Negative_ThrowsArgumentOutOfRangeException()
    {
        var q = CreateQuery().Take(-1);
        Assert.Throws<ArgumentOutOfRangeException>(() => ForceTranslation(q));
    }

    [Fact]
    public void Skip_Negative_Large_ThrowsArgumentOutOfRangeException()
    {
        var q = CreateQuery().Skip(-100);
        Assert.Throws<ArgumentOutOfRangeException>(() => ForceTranslation(q));
    }

    [Fact]
    public void Take_Negative_Large_ThrowsArgumentOutOfRangeException()
    {
        var q = CreateQuery().Take(-5);
        Assert.Throws<ArgumentOutOfRangeException>(() => ForceTranslation(q));
    }

    [Fact]
    public void Skip_And_Take_BothNegative_ThrowsArgumentOutOfRangeException()
    {
        // Either Skip or Take is negative; translation should fail on whichever is reached first.
        var q = CreateQuery().Skip(-100).Take(-5);
        Assert.Throws<ArgumentOutOfRangeException>(() => ForceTranslation(q));
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
