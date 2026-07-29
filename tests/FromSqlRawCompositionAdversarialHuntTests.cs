using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: FromSqlRaw COMPOSITION correctness on SQLite that the existing suites don't cover —
/// decimal exact-compare / full-precision ORDER BY over a derived table, raw GROUP BY / JOIN text wrapped and
/// filtered, Last/ElementAt/Distinct/Average terminals, [Column]-renamed columns, and ExecuteSqlRaw cache
/// invalidation. Each probe diffs the composed FromSqlRaw result against a ctx.Query oracle over the SAME data
/// (catches FromSqlRaw-specific divergence) and/or a hand-computed expected value (catches consistent-but-wrong).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class FromSqlRawCompositionAdversarialHuntTests
{
    private readonly ITestOutputHelper _out;
    public FromSqlRawCompositionAdversarialHuntTests(ITestOutputHelper o) => _out = o;

    [Table("HcpPriced")]
    public sealed class Priced
    {
        [Key] public int Id { get; set; }
        public decimal Price { get; set; }
        public string Cat { get; set; } = "";
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> PricedCtx(params (int id, decimal price, string cat)[] rows)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HcpPriced (Id INTEGER PRIMARY KEY, Price TEXT NOT NULL, Cat TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Priced>().HasKey(p => p.Id) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in rows) await ctx.InsertAsync(new Priced { Id = r.id, Price = r.price, Cat = r.cat });
        return (cn, ctx);
    }

    // ── P1: decimal equality after FromSqlRaw must match ctx.Query (exact-compare, trailing-zero insensitive) ──
    [Fact]
    public async Task decimal_equality_after_fromsqlraw_matches_query_and_hand_value()
    {
        var (cn, ctx) = await PricedCtx((1, 19.99m, "a"), (2, 19.90m, "a"), (3, 2.00m, "b"), (4, 10.50m, "b"));
        using var _cn = cn; using var _ctx = ctx;

        // Hand oracle: exactly row 1 has Price 19.99.
        var rawEq = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Where(p => p.Price == 19.99m)
            .OrderBy(p => p.Id).Select(p => p.Id).ToList();
        var qEq = ctx.Query<Priced>().Where(p => p.Price == 19.99m).OrderBy(p => p.Id).Select(p => p.Id).ToList();
        _out.WriteLine($"rawEq=[{string.Join(",", rawEq)}] qEq=[{string.Join(",", qEq)}]");
        Assert.Equal(new[] { 1 }, rawEq);
        Assert.Equal(qEq, rawEq);

        // Trailing-zero insensitivity: 19.9m must match the stored 19.90 (row 2), scale-insensitive like decimal.
        var rawTz = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Where(p => p.Price == 19.9m)
            .OrderBy(p => p.Id).Select(p => p.Id).ToList();
        var qTz = ctx.Query<Priced>().Where(p => p.Price == 19.9m).OrderBy(p => p.Id).Select(p => p.Id).ToList();
        _out.WriteLine($"rawTz=[{string.Join(",", rawTz)}] qTz=[{string.Join(",", qTz)}]");
        Assert.Equal(qTz, rawTz);
        Assert.Equal(new[] { 2 }, rawTz);
    }

    // ── P2: decimal ORDER BY / range after FromSqlRaw must match ctx.Query (full-precision collation) ──
    [Fact]
    public async Task decimal_orderby_and_range_after_fromsqlraw_matches_query()
    {
        var (cn, ctx) = await PricedCtx((1, 19.99m, "a"), (2, 2.00m, "a"), (3, 100.5m, "b"), (4, 10.50m, "b"));
        using var _cn = cn; using var _ctx = ctx;

        var rawOrder = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").OrderBy(p => p.Price).Select(p => p.Id).ToList();
        var qOrder = ctx.Query<Priced>().OrderBy(p => p.Price).Select(p => p.Id).ToList();
        _out.WriteLine($"rawOrder=[{string.Join(",", rawOrder)}] qOrder=[{string.Join(",", qOrder)}]");
        Assert.Equal(qOrder, rawOrder);
        Assert.Equal(new[] { 2, 4, 1, 3 }, rawOrder); // 2.00 < 10.50 < 19.99 < 100.5 (numeric, not lexical)

        var rawRange = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Where(p => p.Price > 10m)
            .OrderBy(p => p.Id).Select(p => p.Id).ToList();
        var qRange = ctx.Query<Priced>().Where(p => p.Price > 10m).OrderBy(p => p.Id).Select(p => p.Id).ToList();
        _out.WriteLine($"rawRange=[{string.Join(",", rawRange)}] qRange=[{string.Join(",", qRange)}]");
        Assert.Equal(qRange, rawRange);
        Assert.Equal(new[] { 1, 3, 4 }, rawRange); // 19.99, 100.5, 10.50 all > 10
    }

    // ── P3: decimal SUM/AVG aggregate after FromSqlRaw must match ctx.Query (full-precision aggregate fns) ──
    [Fact]
    public async Task decimal_sum_avg_after_fromsqlraw_matches_query()
    {
        var (cn, ctx) = await PricedCtx((1, 19.99m, "a"), (2, 0.01m, "a"), (3, 10.00m, "b"));
        using var _cn = cn; using var _ctx = ctx;

        var rawSum = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Sum(p => p.Price);
        var qSum = ctx.Query<Priced>().Sum(p => p.Price);
        _out.WriteLine($"rawSum={rawSum} qSum={qSum}");
        Assert.Equal(30.00m, rawSum);   // 19.99 + 0.01 + 10.00 exactly
        Assert.Equal(qSum, rawSum);

        var rawAvg = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Average(p => p.Price);
        var qAvg = ctx.Query<Priced>().Average(p => p.Price);
        _out.WriteLine($"rawAvg={rawAvg} qAvg={qAvg}");
        Assert.Equal(qAvg, rawAvg);
    }

    // ── P4: raw GROUP BY text wrapped as a derived table, then composed Where/OrderBy on the aggregate ──
    [Table("HcpCatTotal")]
    public sealed class CatTotal
    {
        [Key] public string Cat { get; set; } = "";
        public int Total { get; set; }
    }

    [Fact]
    public void raw_groupby_text_composed_with_where_on_aggregate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HcpItems (Id INTEGER PRIMARY KEY, Cat TEXT NOT NULL, Qty INTEGER NOT NULL);
                INSERT INTO HcpItems VALUES (1,'a',5),(2,'a',7),(3,'b',1),(4,'c',100),(5,'c',3);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<CatTotal>().HasKey(c => c.Cat) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Groups: a=12, b=1, c=103. Filter Total > 10 → a,c. Order by Total → a(12), c(103).
        var got = ctx.FromSqlRaw<CatTotal>("SELECT Cat, SUM(Qty) AS Total FROM HcpItems GROUP BY Cat")
            .Where(c => c.Total > 10)
            .OrderBy(c => c.Total)
            .Select(c => new { c.Cat, c.Total })
            .ToList();
        _out.WriteLine("got=" + string.Join(";", got.Select(x => $"{x.Cat}:{x.Total}")));
        Assert.Equal(new[] { "a", "c" }, got.Select(x => x.Cat).ToArray());
        Assert.Equal(new[] { 12, 103 }, got.Select(x => x.Total).ToArray());

        // Count over the grouped derived table = number of groups (3), not the 5 base rows.
        var groupCount = ctx.FromSqlRaw<CatTotal>("SELECT Cat, SUM(Qty) AS Total FROM HcpItems GROUP BY Cat").Count();
        _out.WriteLine("groupCount=" + groupCount);
        Assert.Equal(3, groupCount);
    }

    // ── P5: raw JOIN text wrapped as a derived table, then composed Where/paging ──
    [Table("HcpJoined")]
    public sealed class Joined
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Tag { get; set; } = "";
    }

    [Fact]
    public void raw_join_text_composed_with_where_and_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HcpW (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE HcpT (WId INTEGER NOT NULL, Tag TEXT NOT NULL);
                INSERT INTO HcpW VALUES (1,'alice'),(2,'bob'),(3,'carol');
                INSERT INTO HcpT VALUES (1,'x'),(2,'y'),(3,'x');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Joined>().HasKey(j => j.Id) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Join produces (1,alice,x),(2,bob,y),(3,carol,x). Filter Tag='x' → 1,3. Order by Id.
        var got = ctx.FromSqlRaw<Joined>(
                "SELECT w.Id AS Id, w.Name AS Name, t.Tag AS Tag FROM HcpW w JOIN HcpT t ON t.WId = w.Id")
            .Where(j => j.Tag == "x")
            .OrderBy(j => j.Id)
            .Select(j => new { j.Id, j.Name })
            .ToList();
        _out.WriteLine("got=" + string.Join(";", got.Select(x => $"{x.Id}:{x.Name}")));
        Assert.Equal(new[] { 1, 3 }, got.Select(x => x.Id).ToArray());
        Assert.Equal(new[] { "alice", "carol" }, got.Select(x => x.Name).ToArray());
    }

    // ── P6: Last / LastOrDefault / ElementAt over an ordered raw source ──
    [Fact]
    public async Task last_and_elementat_over_ordered_raw()
    {
        var (cn, ctx) = await PricedCtx((1, 5m, "a"), (2, 15m, "a"), (3, 25m, "b"), (4, 35m, "b"));
        using var _cn = cn; using var _ctx = ctx;

        var last = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").OrderBy(p => p.Id).Last();
        _out.WriteLine("last.Id=" + last.Id);
        Assert.Equal(4, last.Id);

        var lastPred = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").OrderBy(p => p.Id).LastOrDefault(p => p.Price < 30m);
        _out.WriteLine("lastPred.Id=" + lastPred?.Id);
        Assert.Equal(3, lastPred?.Id);

        var at = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").OrderBy(p => p.Id).Select(p => p.Id).ElementAt(2);
        _out.WriteLine("elementAt2=" + at);
        Assert.Equal(3, at);
    }

    // ── P7: Distinct over a projected column of a raw source ──
    [Fact]
    public async Task distinct_projection_over_raw()
    {
        var (cn, ctx) = await PricedCtx((1, 5m, "a"), (2, 15m, "a"), (3, 25m, "b"), (4, 35m, "c"), (5, 45m, "b"));
        using var _cn = cn; using var _ctx = ctx;

        var cats = ctx.FromSqlRaw<Priced>("SELECT * FROM HcpPriced").Select(p => p.Cat).Distinct().OrderBy(c => c).ToList();
        var qCats = ctx.Query<Priced>().Select(p => p.Cat).Distinct().OrderBy(c => c).ToList();
        _out.WriteLine($"cats=[{string.Join(",", cats)}]");
        Assert.Equal(new[] { "a", "b", "c" }, cats);
        Assert.Equal(qCats, cats);
    }

    // ── P8: [Column]-renamed property — raw SQL exposes the DB column name; outer Where/projection map it ──
    [Table("HcpRenamed")]
    public sealed class Renamed
    {
        [Key] public int Id { get; set; }
        [Column("full_name")] public string Name { get; set; } = "";
        [Column("score_val")] public int Score { get; set; }
    }

    [Fact]
    public void column_renamed_via_attribute_composes_correctly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HcpRenamed (Id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, score_val INTEGER NOT NULL);
                INSERT INTO HcpRenamed VALUES (1,'alice',10),(2,'bob',20),(3,'carol',30);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Renamed>().HasKey(r => r.Id) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Raw SQL must expose the DB column names (full_name, score_val); the outer Where/projection reference
        // the mapped columns. Filter Score > 15 → 2,3. Project Name.
        var got = ctx.FromSqlRaw<Renamed>("SELECT * FROM HcpRenamed")
            .Where(r => r.Score > 15)
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, r.Name })
            .ToList();
        _out.WriteLine("got=" + string.Join(";", got.Select(x => $"{x.Id}:{x.Name}")));
        Assert.Equal(new[] { 2, 3 }, got.Select(x => x.Id).ToArray());
        Assert.Equal(new[] { "bob", "carol" }, got.Select(x => x.Name).ToArray());

        // Whole-entity materialization must land values in the right properties.
        var whole = ctx.FromSqlRaw<Renamed>("SELECT * FROM HcpRenamed").OrderBy(r => r.Id).ToList();
        Assert.Equal("alice", whole[0].Name);
        Assert.Equal(10, whole[0].Score);
    }

    // ── P9: ExecuteSqlRaw non-query invalidates a cached nORM query result over the same table ──
    // CLEAN BILL: InvalidateResultCacheForRawSql parses the target table from the SQL and invalidates the
    // mapped table's cache tag (which equals map.TableName — the same tag SaveChanges uses). Verified with
    // AsNoTracking so the identity map does not mask the re-read: a TRACKING query would return the already-
    // tracked (pre-update) instance regardless of the cache — that is standard identity-resolution behavior
    // (EF Core behaves identically), NOT a cache-invalidation miss.
    [Table("HcpCacheInval")]
    public sealed class CacheInval
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task executesqlraw_update_invalidates_cached_query()
    {
        using var cache = new NormMemoryCacheProvider();
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HcpCacheInval (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);
                INSERT INTO HcpCacheInval VALUES (1,10),(2,20);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            CacheProvider = cache,
            OnModelCreating = mb => mb.Entity<CacheInval>().HasKey(c => c.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var expiry = TimeSpan.FromMinutes(5);

        int Sum() => ctx.Query<CacheInval>().AsNoTracking().Cacheable(expiry).ToList().Sum(c => c.Val);
        Assert.Equal(30, Sum()); // warms cache: 10+20

        // ExecuteSqlRaw UPDATE through nORM invalidates the mapped table's cache tag.
        var affected = await ctx.Database.ExecuteSqlRawAsync("UPDATE HcpCacheInval SET Val = 100 WHERE Id = 1");
        _out.WriteLine("affected=" + affected);
        Assert.Equal(1, affected);

        var after = Sum();
        _out.WriteLine("after=" + after);
        Assert.Equal(120, after); // 100 + 20 — cache invalidated by the raw UPDATE, no tracked-entity staleness
    }
}
