using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Fast-path SQL template cache must be per-context, not static.
/// Cross-context or cross-model use of the same CLR type must not poison each other's SQL.
/// </summary>
public class FastPathCachePoisoningTests
{
    public class Product
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx(string tableName, bool fluent)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            $"CREATE TABLE \"{tableName}\"(Id INTEGER PRIMARY KEY, Name TEXT);" +
            $"INSERT INTO \"{tableName}\" VALUES(1,'From_{tableName}');";
        setup.ExecuteNonQuery();

        DbContext ctx;
        if (fluent)
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Product>().ToTable(tableName) };
            ctx = new DbContext(cn, new SqliteProvider(), opts);
        }
        else
        {
            ctx = new DbContext(cn, new SqliteProvider());
        }
        return (cn, ctx);
    }

    [Fact]
    public async Task FastPath_CrossContext_DifferentTable_NoCachePoisoning()
    {
        // ctx1 maps Product → "Product" (convention), ctx2 maps Product → "Products" (fluent)
        var (cn1, ctx1) = CreateCtx("Product", fluent: false);
        var (cn2, ctx2) = CreateCtx("Products", fluent: true);

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            // Query via ctx1 first — populates ctx1's per-context cache
            var results1 = await ctx1.Query<Product>().ToListAsync();
            Assert.Single(results1);
            Assert.Equal("From_Product", results1[0].Name);

            // Query via ctx2 — must NOT reuse ctx1's cached SQL pointing at "Product"
            var results2 = await ctx2.Query<Product>().ToListAsync();
            Assert.Single(results2);
            Assert.Equal("From_Products", results2[0].Name);
        }
    }

    [Fact]
    public async Task FastPath_SameProvider_DifferentFluentModel_NoCachePoisoning()
    {
        // Both contexts use SQLite but with different ToTable() names for the same CLR type
        var (cn1, ctx1) = CreateCtx("WidgetA", fluent: true);
        var (cn2, ctx2) = CreateCtx("WidgetB", fluent: true);

        // Override with different names
        var opts1 = new DbContextOptions { OnModelCreating = mb => mb.Entity<Product>().ToTable("WidgetA") };
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<Product>().ToTable("WidgetB") };
        using var ctxA = new DbContext(cn1, new SqliteProvider(), opts1);
        using var ctxB = new DbContext(cn2, new SqliteProvider(), opts2);
        using (cn1) using (cn2) using (ctx1) using (ctx2)
        {
            var resA = await ctxA.Query<Product>().ToListAsync();
            Assert.Single(resA);
            Assert.Equal("From_WidgetA", resA[0].Name);

            var resB = await ctxB.Query<Product>().ToListAsync();
            Assert.Single(resB);
            Assert.Equal("From_WidgetB", resB[0].Name);
        }
    }

    [Fact]
    public void FastPath_SqlCache_IsPerContext_NotSharedAcrossInstances()
    {
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();

        using var ctx1 = new DbContext(cn1, new SqliteProvider());
        using var ctx2 = new DbContext(cn2, new SqliteProvider());
        using (cn1) using (cn2)
        {
            // Each context must have its own independent cache instance
            Assert.NotSame(ctx1.FastPathSqlCache, ctx2.FastPathSqlCache);
        }
    }
}
