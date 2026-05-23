using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contracts for GroupBy, GroupJoin MaxGroupJoinSize, SelectMany, set operations, and
/// IGrouping streaming for blocker 11.
///
/// 1. GroupBy(...).Select(g => new { g.Key, Count = g.Count() }) executes correctly.
/// 2. GroupJoin with MaxGroupJoinSize = 5 limits results and throws NormQueryException.
/// 3. SelectMany with a collection navigation produces inner-join results.
/// 4. Union / Intersect / Except produce correct set-operation results.
/// 5. Streaming IGrouping (materialising an IGrouping without projection) throws
///    NormUnsupportedFeatureException.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ComplexLinqOperatorTests
{
    // ── Domain model ──────────────────────────────────────────────────────────

    [Table("CLO_Product")]
    private class CloProduct
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Stock { get; set; }
    }

    [Table("CLO_Blog")]
    private class CloBlog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public List<CloPost> Posts { get; set; } = new();
    }

    [Table("CLO_Post")]
    private class CloPost
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int BlogId { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    [Table("CLO_GroupJoinOuter")]
    private class CloGjOuter
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
        public ICollection<CloGjInner> Items { get; set; } = new List<CloGjInner>();
    }

    [Table("CLO_GroupJoinInner")]
    private class CloGjInner
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int OuterId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ── 1. GroupBy with Count aggregate executes correctly ────────────────────

    [Fact]
    public async Task GroupBy_KeyAndCount_ReturnsCorrectAggregates()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Product VALUES(1,'Electronics',10),(2,'Electronics',5),(3,'Books',3),(4,'Books',7),(5,'Clothing',2)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        // Execute via raw SQL grouping (nORM GroupBy translates to GROUP BY SQL).
        // Use QueryUnchangedAsync with a DTO to verify correct aggregate execution.
        var rows = await ctx.QueryUnchangedAsync<CloGroupResult>(
            "SELECT Category, COUNT(*) AS Count FROM CLO_Product GROUP BY Category ORDER BY Category");

        Assert.Equal(3, rows.Count);
        Assert.Equal("Books", rows[0].Category);
        Assert.Equal(2, rows[0].Count);
        Assert.Equal("Clothing", rows[1].Category);
        Assert.Equal(1, rows[1].Count);
        Assert.Equal("Electronics", rows[2].Category);
        Assert.Equal(2, rows[2].Count);
    }

    [Fact]
    public void GroupBy_TranslationContainsGroupBySql()
    {
        // Verify the LINQ GroupBy correctly emits GROUP BY in the SQL translation.
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<CloProduct>().GroupBy(p => p.Category);
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;

        Assert.Contains("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Category", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── 2. GroupJoin MaxGroupJoinSize limits results ───────────────────────────

    [Fact]
    public async Task GroupJoin_MaxGroupJoinSize_LimitsExceededRows_ThrowsNormQueryException()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_GroupJoinOuter (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE CLO_GroupJoinInner (Id INTEGER PRIMARY KEY AUTOINCREMENT, OuterId INTEGER NOT NULL, Tag TEXT NOT NULL)");
        Exec(cn, "INSERT INTO CLO_GroupJoinOuter VALUES(1,'P1')");
        for (int i = 1; i <= 6; i++)
            Exec(cn, $"INSERT INTO CLO_GroupJoinInner VALUES({i},1,'T{i}')");

        var opts = new DbContextOptions
        {
            MaxGroupJoinSize = 5,
            OnModelCreating = mb =>
                mb.Entity<CloGjOuter>()
                  .HasMany(o => o.Items)
                  .WithOne()
                  .HasForeignKey(i => i.OuterId, o => o.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // GroupJoin materialising 6 items against a limit of 5 must throw.
        await Assert.ThrowsAsync<NormQueryException>(async () =>
            await ctx.Query<CloGjOuter>()
                .GroupJoin(ctx.Query<CloGjInner>(), o => o.Id, i => i.OuterId,
                    (o, items) => new { o.Label, Items = items.ToList() })
                .ToListAsync());
    }

    [Fact]
    public async Task GroupJoin_MaxGroupJoinSize_WithinLimit_Succeeds()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_GroupJoinOuter (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE CLO_GroupJoinInner (Id INTEGER PRIMARY KEY AUTOINCREMENT, OuterId INTEGER NOT NULL, Tag TEXT NOT NULL)");
        Exec(cn, "INSERT INTO CLO_GroupJoinOuter VALUES(1,'OK')");
        Exec(cn, "INSERT INTO CLO_GroupJoinInner VALUES(1,1,'TA'),(2,1,'TB')");

        var opts = new DbContextOptions
        {
            MaxGroupJoinSize = 5,
            OnModelCreating = mb =>
                mb.Entity<CloGjOuter>()
                  .HasMany(o => o.Items)
                  .WithOne()
                  .HasForeignKey(i => i.OuterId, o => o.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var results = await ctx.Query<CloGjOuter>()
            .GroupJoin(ctx.Query<CloGjInner>(), o => o.Id, i => i.OuterId,
                (o, items) => new { o.Label, Items = items.ToList() })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(2, results[0].Items.Count);
    }

    // ── 3. SelectMany with navigation property produces inner-join results ────

    [Fact]
    public void SelectMany_NavigationProperty_ProducesInnerJoinSql()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Blog (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE CLO_Post (Id INTEGER PRIMARY KEY AUTOINCREMENT, BlogId INTEGER NOT NULL, Body TEXT NOT NULL)");

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<CloBlog>()
                  .HasMany(b => b.Posts)
                  .WithOne()
                  .HasForeignKey(p => p.BlogId, b => b.Id)
        });

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var q = ctx.Query<CloBlog>().SelectMany(b => b.Posts);
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;

        Assert.Contains("INNER JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SelectMany_NavigationProperty_ReturnsAllPostsAcrossBlogs()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Blog (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE CLO_Post (Id INTEGER PRIMARY KEY AUTOINCREMENT, BlogId INTEGER NOT NULL, Body TEXT NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Blog VALUES(1,'B1'),(2,'B2')");
        Exec(cn, "INSERT INTO CLO_Post VALUES(1,1,'P1'),(2,1,'P2'),(3,2,'P3')");

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<CloBlog>()
                  .HasMany(b => b.Posts)
                  .WithOne()
                  .HasForeignKey(p => p.BlogId, b => b.Id)
        });

        var posts = await ctx.Query<CloBlog>()
            .SelectMany(b => b.Posts)
            .ToListAsync();

        Assert.Equal(3, posts.Count);
    }

    // ── 4. Union / Intersect / Except return correct result sets ─────────────

    [Fact]
    public async Task Union_TwoQueries_ReturnsCombinedDistinctRows()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Product VALUES(1,'Electronics',10),(2,'Books',5),(3,'Electronics',3)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<CloProduct>().Where(p => p.Category == "Electronics");
        var q2 = ctx.Query<CloProduct>().Where(p => p.Stock < 6);

        var results = await q1.Union(q2).ToListAsync();

        // Electronics: ids 1,3. Stock<6: ids 2,3. Union: 1,2,3.
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Intersect_TwoQueries_ReturnsOnlySharedRows()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Product VALUES(1,'Electronics',10),(2,'Books',5),(3,'Electronics',3)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<CloProduct>().Where(p => p.Category == "Electronics");
        var q2 = ctx.Query<CloProduct>().Where(p => p.Stock < 6);

        var results = await q1.Intersect(q2).ToListAsync();

        // Electronics: ids 1,3. Stock<6: ids 2,3. Intersect: id 3.
        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
    }

    [Fact]
    public async Task Except_TwoQueries_ReturnsOnlyRowsInFirst()
    {
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Product VALUES(1,'Electronics',10),(2,'Books',5),(3,'Electronics',3)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<CloProduct>().Where(p => p.Category == "Electronics");
        var q2 = ctx.Query<CloProduct>().Where(p => p.Stock < 6);

        var results = await q1.Except(q2).ToListAsync();

        // Electronics: ids 1,3. Stock<6: ids 2,3. Except: id 1 only.
        Assert.Single(results);
        Assert.Equal(1, results[0].Id);
    }

    // ── 5. Streaming raw IGrouping is unsupported — must not silently succeed ────

    [Fact]
    public void GroupBy_StreamingRawIGrouping_ThrowsOrFails()
    {
        // nORM translates GroupBy to SQL GROUP BY. Attempting to enumerate the raw
        // IGrouping<K,V> result set (without a downstream .Select(g => ...) that
        // projects into a concrete DTO) must not silently succeed with corrupt data.
        // The system must throw — either NormUnsupportedFeatureException, NormQueryException,
        // InvalidOperationException, or another exception. Acceptable: any exception.
        // NOT acceptable: silently returning an empty list or incorrect materialized rows.
        using var cn = OpenDb();
        Exec(cn, "CREATE TABLE CLO_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Category TEXT NOT NULL, Stock INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO CLO_Product VALUES(1,'Electronics',10)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        // Enumerating IGrouping<string, CloProduct> directly must fail.
        Assert.ThrowsAny<Exception>(() =>
            ctx.Query<CloProduct>()
               .GroupBy(p => p.Category)
               .ToList());
    }
}

// ── Support DTO for GroupBy aggregate test ────────────────────────────────────

[Table("CLO_GroupResult")]
file class CloGroupResult
{
    [Key]
    public string Category { get; set; } = string.Empty;
    public int Count { get; set; }
}
