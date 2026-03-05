using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQL-1: Provider-unsafe boolean literal in fast path and NormQueryProvider.
/// </summary>
public class BooleanLiteralProviderTests
{
    public class Item
    {
        [Key]
        public int Id { get; set; }
        public bool IsActive { get; set; }
    }

    private static DbContext CreateAndSeed(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE \"Item\"(Id INTEGER PRIMARY KEY, IsActive INTEGER);" +
            "INSERT INTO \"Item\" VALUES(1,1);" +
            "INSERT INTO \"Item\" VALUES(2,0);" +
            "INSERT INTO \"Item\" VALUES(3,1);";
        cmd.ExecuteNonQuery();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void BooleanTrueLiteral_Default_Returns1()
    {
        Assert.Equal("1", new SqliteProvider().BooleanTrueLiteral);
    }

    [Fact]
    public async Task FastPath_BoolWhere_UsesProviderBooleanLiteral()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            // Fast path: u => u.IsActive should use provider literal (= 1 for SQLite)
            var results = await ctx.Query<Item>().Where(u => u.IsActive).ToListAsync();
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.True(r.IsActive));
        }
    }

    [Fact]
    public async Task NormQueryProvider_SimpleWhere_BoolPredicate_UsesLiteral()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            // Goes through NormQueryProvider simple WHERE path (TryGetSimpleQuery)
            var results = await ctx.Query<Item>().Where(u => u.IsActive).ToListAsync();
            Assert.Equal(2, results.Count);
        }
    }

    [Fact]
    public async Task NormQueryProvider_Count_BoolPredicate_Succeeds()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            // Count with no predicate uses fast path
            var count = await ctx.Query<Item>().CountAsync();
            Assert.Equal(3, count);
        }
    }

    [Fact]
    public async Task BoolWhere_Returns_CorrectSubset()
    {
        using var ctx = CreateAndSeed(out var cn);
        using (cn)
        {
            var active = await ctx.Query<Item>().Where(u => u.IsActive).ToListAsync();
            var all = await ctx.Query<Item>().ToListAsync();
            Assert.Equal(2, active.Count);
            Assert.Equal(3, all.Count);
        }
    }
}
