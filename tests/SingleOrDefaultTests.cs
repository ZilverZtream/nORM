using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// QP-1: Verifies that Single/SingleOrDefault correctly detect multiple matching rows.
/// Previously both used TAKE 1, so duplicate-row detection was impossible.
/// Fix: Single/SingleOrDefault now use TAKE 2 so the caller can detect the second row.
///
/// NOTE on test design: nORM caches query plans keyed by expression structure (fingerprint),
/// ignoring constant values. All tests for the same method+property pair must use the SAME
/// constant so that every test operates against the same cached plan.
/// </summary>
public class SingleOrDefaultTests
{
    private class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int CategoryId { get; set; }
    }

    // All tests use CategoryId == 1 so they share a stable, consistent plan.
    private const int TestCategoryId = 1;

    private static (SqliteConnection cn, DbContext ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE Product (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Name        TEXT    NOT NULL,
                CategoryId  INTEGER NOT NULL DEFAULT 0
            );";
        cmd.ExecuteNonQuery();

        return (cn, ctx);
    }

    private static void Insert(SqliteConnection cn, string name, int categoryId)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO Product (Name, CategoryId) VALUES (@name, @cat)";
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@cat", categoryId);
        cmd.ExecuteNonQuery();
    }

    // ──────────────────────────── Single ─────────────────────────────

    [Fact]
    public void Single_WithOneMatch_ReturnsElement()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, "Widget", TestCategoryId);

        var product = ctx.Query<Product>().Single(p => p.CategoryId == TestCategoryId);
        Assert.NotNull(product);
        Assert.Equal("Widget", product.Name);
    }

    [Fact]
    public void Single_WithZeroMatches_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // No rows inserted — Single() must throw "no elements"
        var ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Query<Product>().Single(p => p.CategoryId == TestCategoryId));
        Assert.Contains("no elements", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Single_WithMultipleMatches_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Two rows share the same CategoryId — Single() must detect the duplicate and throw
        Insert(cn, "Alpha", TestCategoryId);
        Insert(cn, "Beta",  TestCategoryId);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Query<Product>().Single(p => p.CategoryId == TestCategoryId));
        Assert.Contains("more than one element", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ──────────────────────── SingleOrDefault ────────────────────────

    [Fact]
    public void SingleOrDefault_WithOneMatch_ReturnsElement()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, "Gadget", TestCategoryId);

        var product = ctx.Query<Product>().SingleOrDefault(p => p.CategoryId == TestCategoryId);
        Assert.NotNull(product);
        Assert.Equal("Gadget", product.Name);
    }

    [Fact]
    public void SingleOrDefault_WithZeroMatches_ReturnsDefault()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // No rows inserted — SingleOrDefault() must return null
        var product = ctx.Query<Product>().SingleOrDefault(p => p.CategoryId == TestCategoryId);
        Assert.Null(product);
    }

    [Fact]
    public void SingleOrDefault_WithMultipleMatches_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Two rows share the same CategoryId — SingleOrDefault() must detect the duplicate and throw
        Insert(cn, "X", TestCategoryId);
        Insert(cn, "Y", TestCategoryId);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Query<Product>().SingleOrDefault(p => p.CategoryId == TestCategoryId));
        Assert.Contains("more than one element", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
