using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial read/query-path parity audit hunting for SILENT data-loss bugs:
/// query shapes where nORM returns DIFFERENT rows/values than LINQ-to-Objects
/// (the ground-truth oracle) WITHOUT throwing. Targets feature COMBINATIONS the
/// seeded LinqParityFuzzTests battery is unlikely to hit: HAVING, ORDER BY on an
/// aggregate + paging, GroupBy Average/Min/Max/nullable aggregates, Distinct then
/// OrderBy then Skip/Take, chained 3-arm set operations, Join then GroupBy,
/// nullable-key ORDER BY with paging, ternary-in-WHERE, ORDER BY over a correlated
/// aggregate, and subquery-in-projection then filter/order.
///
/// A clean <see cref="NormUnsupportedFeatureException"/> (fail-loud decline) is an
/// ACCEPTABLE outcome — only a silent wrong answer is a bug. Every comparison is
/// ordered by a UNIQUE key so ties can't create false positives.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ReadPathDataLossAuditTests
{
    // ── Entities ─────────────────────────────────────────────────────────────
    [System.ComponentModel.DataAnnotations.Schema.Table("ReadLossRow")]
    private sealed class ARow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Cat { get; set; }
        public int IntVal { get; set; }
        public int? NullableInt { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Nick { get; set; }
        public decimal Amount { get; set; }
        public double Price { get; set; }
        public bool Flag { get; set; }
        public DateTime Created { get; set; }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("ReadLossChild")]
    private sealed class AChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int ChildVal { get; set; }
    }

    // ── Dataset ──────────────────────────────────────────────────────────────
    private static readonly ARow[] Rows = BuildRows();
    private static readonly AChild[] Children = BuildChildren();

    private static ARow[] BuildRows()
    {
        var names = new[] { "alpha", "ALPHA", "Alpha", "beta", "", "gamma" };
        var nicks = new[] { null, "alpha", "", "ALPHA", "b", null, "beta" };
        var rows = new List<ARow>();
        for (var i = 0; i < 30; i++)
        {
            // Cat 3 is a small "all-null NullableInt" group; Cat 4 is a single-row group.
            var cat = i switch
            {
                < 6 => 0,
                < 14 => 1,
                < 22 => 2,
                < 27 => 3,          // rows 22..26: force NullableInt null below
                _ => 4,             // rows 27..29
            };
            rows.Add(new ARow
            {
                Id = i + 1,
                Cat = cat,
                IntVal = (i % 7) - 3,                                  // -3..3 with dups
                NullableInt = cat == 3 ? null : (i % 4 == 0 ? null : (i % 6) - 2), // includes 1 and 2
                Name = names[i % names.Length],
                Nick = nicks[i % nicks.Length],
                Amount = ((i * 37) % 11 - 5) + (i % 3) * 0.25m,
                Price = ((i * 13) % 9 - 4) + (i % 2) * 0.5,
                Flag = i % 3 == 0,
                Created = new DateTime(2026, 1, 1).AddDays(i % 9).AddSeconds(i * 977 % 86_400),
            });
        }
        return rows.ToArray();
    }

    private static AChild[] BuildChildren()
    {
        var children = new List<AChild>();
        for (var i = 0; i < 50; i++)
        {
            children.Add(new AChild
            {
                Id = i + 1,
                ParentId = (i * 7) % 34 + 1,   // some parents 0 children, some many; a few past 30
                ChildVal = (i % 9) - 4,
            });
        }
        return children.ToArray();
    }

    // ── Context bootstrap ────────────────────────────────────────────────────
    private static (DbContext ctx, SqliteConnection cn) NewContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ReadLossRow (
                    Id INTEGER PRIMARY KEY,
                    Cat INTEGER NOT NULL,
                    IntVal INTEGER NOT NULL,
                    NullableInt INTEGER NULL,
                    Name TEXT NOT NULL,
                    Nick TEXT NULL,
                    Amount TEXT NOT NULL,
                    Price REAL NOT NULL,
                    Flag INTEGER NOT NULL,
                    Created TEXT NOT NULL);
                CREATE TABLE ReadLossChild (
                    Id INTEGER PRIMARY KEY,
                    ParentId INTEGER NOT NULL,
                    ChildVal INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());
        foreach (var r in Rows) ctx.Add(r);
        foreach (var c in Children) ctx.Add(c);
        ctx.SaveChangesAsync().GetAwaiter().GetResult();
        return (ctx, cn);
    }

    private static bool IsDeclined(Exception ex) => ex is NormUnsupportedFeatureException;

    // ── 1. GroupBy + HAVING + OrderBy(aggregate) + Skip/Take + projection ─────
    [Fact]
    public void GroupBy_Having_OrderByAggregate_Paging()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.Cat)
            .Where(g => g.Count() >= 3)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.IntVal) })
            .OrderByDescending(x => x.S).ThenBy(x => x.Key)
            .Skip(1).Take(3)
            .Select(x => (x.Key, x.C, x.S)).ToList();

        List<(int, int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.Cat)
                .Where(g => g.Count() >= 3)
                .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.IntVal) })
                .OrderByDescending(x => x.S).ThenBy(x => x.Key)
                .Skip(1).Take(3)
                .AsEnumerable().Select(x => (x.Key, x.C, x.S)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"HAVING/order-by-aggregate/paging mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 2. GroupBy(computed key) + Average/Min/Max (fuzzer only tests Count+Sum) ─
    [Fact]
    public void GroupBy_ComputedKey_Average_Min_Max()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.IntVal % 3)
            .Select(g => new { Key = g.Key, C = g.Count(), Mn = g.Min(x => x.IntVal), Mx = g.Max(x => x.IntVal), A = g.Average(x => x.IntVal) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.C, x.Mn, x.Mx, x.A)).ToList();

        List<(int Key, int C, int Mn, int Mx, double A)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.IntVal % 3)
                .Select(g => new { Key = g.Key, C = g.Count(), Mn = g.Min(x => x.IntVal), Mx = g.Max(x => x.IntVal), A = g.Average(x => x.IntVal) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.C, x.Mn, x.Mx, x.A)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.True(expected[i].Key == actual[i].Key && expected[i].C == actual[i].C
                    && expected[i].Mn == actual[i].Mn && expected[i].Mx == actual[i].Mx
                    && Math.Abs(expected[i].A - actual[i].A) < 1e-9,
                $"GroupBy computed-key aggregate mismatch at {i}\nexpected {expected[i]}\nactual   {actual[i]}");
        }
    }

    // ── 3. GroupBy nullable aggregates incl an all-null group ─────────────────
    [Fact]
    public void GroupBy_NullableAggregates_AllNullGroup()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.Cat)
            .Select(g => new { g.Key, Mx = g.Max(x => (int?)x.NullableInt), Mn = g.Min(x => (int?)x.NullableInt) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.Mx, x.Mn)).ToList();

        List<(int Key, int? Mx, int? Mn)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.Cat)
                .Select(g => new { g.Key, Mx = g.Max(x => (int?)x.NullableInt), Mn = g.Min(x => (int?)x.NullableInt) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.Mx, x.Mn)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"GroupBy nullable Max/Min mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 4. Distinct then OrderBy then Skip/Take (paging AFTER distinct) ───────
    [Fact]
    public void Distinct_Then_OrderBy_Skip_Take()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Select(r => r.IntVal).Distinct().OrderBy(x => x).Skip(1).Take(4).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Select(r => r.IntVal).Distinct().OrderBy(x => x).Skip(1).Take(4)
                .AsEnumerable().ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"Distinct→OrderBy→Skip→Take mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 5. Distinct over multi-column projection then Count ───────────────────
    [Fact]
    public void Distinct_MultiColumn_Then_Count()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Select(r => new { r.IntVal, r.Flag }).Distinct().Count();

        int actual;
        try
        {
            actual = ctx.Query<ARow>().Select(r => new { r.IntVal, r.Flag }).Distinct().Count();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected == actual, $"Distinct multi-column Count mismatch: expected {expected} actual {actual}");
    }

    // ── 6. Chained 3-arm set ops: Union then Except ──────────────────────────
    [Fact]
    public void Chained_SetOps_Union_Except()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        // Except arm removes only IntVal==0 so the result is non-empty (a trivial
        // both-empty pass would prove nothing).
        var expected = Rows.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(Rows.Where(r => r.IntVal < 0).Select(r => r.IntVal))
            .Except(Rows.Where(r => r.IntVal == 0).Select(r => r.IntVal))
            .OrderBy(x => x).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(ctx.Query<ARow>().Where(r => r.IntVal < 0).Select(r => r.IntVal))
                .Except(ctx.Query<ARow>().Where(r => r.IntVal == 0).Select(r => r.IntVal))
                .AsEnumerable().OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"Union/Except chain mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 7. Chained Concat preserves duplicates across 3 arms ──────────────────
    [Fact]
    public void Chained_SetOps_Concat_Preserves_Duplicates()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Where(r => r.IntVal >= 1).Select(r => r.IntVal)
            .Concat(Rows.Where(r => r.IntVal <= -1).Select(r => r.IntVal))
            .Concat(Rows.Where(r => r.Flag).Select(r => r.IntVal))
            .OrderBy(x => x).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Where(r => r.IntVal >= 1).Select(r => r.IntVal)
                .Concat(ctx.Query<ARow>().Where(r => r.IntVal <= -1).Select(r => r.IntVal))
                .Concat(ctx.Query<ARow>().Where(r => r.Flag).Select(r => r.IntVal))
                .AsEnumerable().OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"Concat chain (duplicate-preserving) mismatch\nexpected count {expected.Count} actual count {actual.Count}\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 8. Set op then OrderBy/Skip/Take on the combined result ───────────────
    [Fact]
    public void SetOp_Then_OrderBy_Skip_Take()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(Rows.Where(r => r.Flag).Select(r => r.IntVal))
            .OrderByDescending(x => x).Skip(1).Take(3).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(ctx.Query<ARow>().Where(r => r.Flag).Select(r => r.IntVal))
                .OrderByDescending(x => x).Skip(1).Take(3)
                .AsEnumerable().ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"set-op→order→page mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 9. Join then GroupBy then aggregate ───────────────────────────────────
    [Fact]
    public void Join_Then_GroupBy_Aggregate()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Join(Children, r => r.Id, c => c.ParentId, (r, c) => new { r.IntVal, c.ChildVal })
            .GroupBy(x => x.IntVal)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.C, x.S)).ToList();

        List<(int, int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().Join(ctx.Query<AChild>(), r => r.Id, c => c.ParentId, (r, c) => new { r.IntVal, c.ChildVal })
                .GroupBy(x => x.IntVal)
                .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.C, x.S)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"Join→GroupBy mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 10. ORDER BY nullable key (asc & desc) with tiebreak + paging ─────────
    [Fact]
    public void OrderBy_NullableKey_Asc_Desc_Paging()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        // Ascending
        var expAsc = Rows.OrderBy(r => r.NullableInt).ThenBy(r => r.Id).Skip(2).Take(12).Select(r => r.Id).ToList();
        // Descending
        var expDesc = Rows.OrderByDescending(r => r.NullableInt).ThenBy(r => r.Id).Skip(2).Take(12).Select(r => r.Id).ToList();

        List<int> actAsc, actDesc;
        try
        {
            actAsc = ctx.Query<ARow>().OrderBy(r => r.NullableInt).ThenBy(r => r.Id).Skip(2).Take(12)
                .AsEnumerable().Select(r => r.Id).ToList();
            actDesc = ctx.Query<ARow>().OrderByDescending(r => r.NullableInt).ThenBy(r => r.Id).Skip(2).Take(12)
                .AsEnumerable().Select(r => r.Id).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expAsc.SequenceEqual(actAsc),
            $"nullable ASC ordering mismatch\nexpected [{string.Join(",", expAsc)}]\nactual   [{string.Join(",", actAsc)}]");
        Assert.True(expDesc.SequenceEqual(actDesc),
            $"nullable DESC ordering mismatch\nexpected [{string.Join(",", expDesc)}]\nactual   [{string.Join(",", actDesc)}]");
    }

    // ── 11. Ternary in WHERE where one branch is a nullable column ────────────
    [Fact]
    public void Ternary_In_Where_With_Nullable_Branch()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Where(r => (r.Flag ? r.NullableInt : (int?)r.IntVal) > 0)
            .Select(r => r.Id).OrderBy(x => x).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Where(r => (r.Flag ? r.NullableInt : (int?)r.IntVal) > 0)
                .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"ternary-in-WHERE nullable-branch mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 12. ORDER BY over a correlated aggregate, tiebroken, then Take ────────
    [Fact]
    public void OrderBy_By_Correlated_Count_Then_Take()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.OrderBy(r => Children.Count(c => c.ParentId == r.Id)).ThenBy(r => r.Id)
            .Take(18).Select(r => r.Id).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().OrderBy(r => ctx.Query<AChild>().Count(c => c.ParentId == r.Id)).ThenBy(r => r.Id)
                .Take(18).AsEnumerable().Select(r => r.Id).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"ORDER BY correlated-count mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 13. Subquery in projection, then Where on it, then OrderBy + Take ─────
    [Fact]
    public void Projection_Subquery_Then_Where_Then_OrderBy_Take()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Select(r => new { r.Id, N = Children.Count(c => c.ParentId == r.Id) })
            .Where(x => x.N >= 2)
            .OrderByDescending(x => x.N).ThenBy(x => x.Id)
            .Take(10)
            .Select(x => (x.Id, x.N)).ToList();

        List<(int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().Select(r => new { r.Id, N = ctx.Query<AChild>().Count(c => c.ParentId == r.Id) })
                .Where(x => x.N >= 2)
                .OrderByDescending(x => x.N).ThenBy(x => x.Id)
                .Take(10)
                .AsEnumerable().Select(x => (x.Id, x.N)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"projection-subquery→where→order→take mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 14. Contains(list-with-null) — positive and negated, over nullable col ─
    [Fact]
    public void Contains_List_With_Null_Positive_And_Negated()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var ids = new int?[] { 1, null, 2 };

        var expPos = Rows.Where(r => ids.Contains(r.NullableInt)).Select(r => r.Id).OrderBy(x => x).ToList();
        var expNeg = Rows.Where(r => !ids.Contains(r.NullableInt)).Select(r => r.Id).OrderBy(x => x).ToList();

        List<int> actPos, actNeg;
        try
        {
            actPos = ctx.Query<ARow>().Where(r => ids.Contains(r.NullableInt)).AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
            actNeg = ctx.Query<ARow>().Where(r => !ids.Contains(r.NullableInt)).AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expPos.SequenceEqual(actPos),
            $"positive Contains(list-with-null) mismatch\nexpected [{string.Join(",", expPos)}]\nactual   [{string.Join(",", actPos)}]");
        Assert.True(expNeg.SequenceEqual(actNeg),
            $"negated Contains(list-with-null) mismatch\nexpected [{string.Join(",", expNeg)}]\nactual   [{string.Join(",", actNeg)}]");
    }

    // ── 15. GroupBy with a FILTERED aggregate inside the projection ───────────
    [Fact]
    public void GroupBy_FilteredAggregate_In_Select()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.Cat)
            .Select(g => new { g.Key, PosCount = g.Count(x => x.IntVal > 0), FlagSum = g.Sum(x => x.Flag ? x.IntVal : 0) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.PosCount, x.FlagSum)).ToList();

        List<(int, int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.Cat)
                .Select(g => new { g.Key, PosCount = g.Count(x => x.IntVal > 0), FlagSum = g.Sum(x => x.Flag ? x.IntVal : 0) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.PosCount, x.FlagSum)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"GroupBy filtered-aggregate mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 16. Nullable Sum/Max over an EMPTY and an ALL-NULL top-level set ──────
    [Fact]
    public void Nullable_Sum_Max_Over_Empty_And_AllNull()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        // Empty set
        var expEmptyMax = Rows.Where(r => r.Id < 0).Max(r => (int?)r.IntVal);
        var expEmptySum = Rows.Where(r => r.Id < 0).Sum(r => (int?)r.IntVal);
        // All-null set (Cat 3 rows have NullableInt == null)
        var expAllNullMax = Rows.Where(r => r.Cat == 3).Max(r => r.NullableInt);
        var expAllNullSum = Rows.Where(r => r.Cat == 3).Sum(r => r.NullableInt);

        int? actEmptyMax, actEmptySum, actAllNullMax, actAllNullSum;
        try
        {
            actEmptyMax = ctx.Query<ARow>().Where(r => r.Id < 0).Max(r => (int?)r.IntVal);
            actEmptySum = ctx.Query<ARow>().Where(r => r.Id < 0).Sum(r => (int?)r.IntVal);
            actAllNullMax = ctx.Query<ARow>().Where(r => r.Cat == 3).Max(r => r.NullableInt);
            actAllNullSum = ctx.Query<ARow>().Where(r => r.Cat == 3).Sum(r => r.NullableInt);
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expEmptyMax == actEmptyMax, $"empty Max(int?) mismatch: expected {Fmt(expEmptyMax)} actual {Fmt(actEmptyMax)}");
        Assert.True(expEmptySum == actEmptySum, $"empty Sum(int?) mismatch: expected {Fmt(expEmptySum)} actual {Fmt(actEmptySum)}");
        Assert.True(expAllNullMax == actAllNullMax, $"all-null Max(int?) mismatch: expected {Fmt(expAllNullMax)} actual {Fmt(actAllNullMax)}");
        Assert.True(expAllNullSum == actAllNullSum, $"all-null Sum(int?) mismatch: expected {Fmt(expAllNullSum)} actual {Fmt(actAllNullSum)}");
    }

    private static string Fmt(int? v) => v.HasValue ? v.Value.ToString() : "null";

    // ── 17. Take then Where: filter must apply AFTER the paging window ────────
    [Fact]
    public void Take_Then_Where_Filters_After_Window()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.OrderBy(r => r.Id).Take(12).Where(r => r.Flag).Select(r => r.Id).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().OrderBy(r => r.Id).Take(12).Where(r => r.Flag)
                .AsEnumerable().Select(r => r.Id).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"Take-then-Where mismatch (filter must apply after the window)\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── 18. Skip/Take then Count: Count must respect the paging window ────────
    [Fact]
    public void Paging_Then_Count_Respects_Window()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expectedA = Rows.OrderBy(r => r.Id).Skip(5).Take(10).Count();
        var expectedB = Rows.Where(r => r.IntVal >= 0).OrderBy(r => r.Id).Skip(3).Take(4).Count();

        int actualA, actualB;
        try
        {
            actualA = ctx.Query<ARow>().OrderBy(r => r.Id).Skip(5).Take(10).Count();
            actualB = ctx.Query<ARow>().Where(r => r.IntVal >= 0).OrderBy(r => r.Id).Skip(3).Take(4).Count();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expectedA == actualA, $"Count after Skip/Take mismatch (A): expected {expectedA} actual {actualA}");
        Assert.True(expectedB == actualB, $"Count after Where/Skip/Take mismatch (B): expected {expectedB} actual {actualB}");
    }

    // ── 19. HAVING on Sum (not just Count) ────────────────────────────────────
    [Fact]
    public void GroupBy_Having_On_Sum()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.Cat)
            .Where(g => g.Sum(x => x.IntVal) > 0)
            .Select(g => new { g.Key, S = g.Sum(x => x.IntVal) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.S)).ToList();

        List<(int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.Cat)
                .Where(g => g.Sum(x => x.IntVal) > 0)
                .Select(g => new { g.Key, S = g.Sum(x => x.IntVal) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.S)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"HAVING-on-Sum mismatch\nexpected [{string.Join(" | ", expected)}]\nactual   [{string.Join(" | ", actual)}]");
    }

    // ── 20. NOT IN / IN over a non-null column (fuzzer never emits List.Contains) ─
    [Fact]
    public void NotIn_And_In_Over_IntColumn()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var vals = new[] { -1, 0, 2, 2, 3 };   // duplicates on purpose

        var expIn = Rows.Where(r => vals.Contains(r.IntVal)).Select(r => r.Id).OrderBy(x => x).ToList();
        var expNotIn = Rows.Where(r => !vals.Contains(r.IntVal)).Select(r => r.Id).OrderBy(x => x).ToList();

        List<int> actIn, actNotIn;
        try
        {
            actIn = ctx.Query<ARow>().Where(r => vals.Contains(r.IntVal)).AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
            actNotIn = ctx.Query<ARow>().Where(r => !vals.Contains(r.IntVal)).AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expIn.SequenceEqual(actIn),
            $"IN over int column mismatch\nexpected [{string.Join(",", expIn)}]\nactual   [{string.Join(",", actIn)}]");
        Assert.True(expNotIn.SequenceEqual(actNotIn),
            $"NOT IN over int column mismatch\nexpected [{string.Join(",", expNotIn)}]\nactual   [{string.Join(",", actNotIn)}]");
    }

    // ── 21. Nested ternary in projection with a NULL branch ───────────────────
    [Fact]
    public void Nested_Ternary_Projection_With_Null_Branch()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.OrderBy(r => r.Id)
            .Select(r => (r.Id, V: r.Flag ? (r.NullableInt ?? -1) : (r.IntVal > 0 ? r.IntVal : -2)))
            .ToList();

        List<(int, int)> actual;
        try
        {
            actual = ctx.Query<ARow>().OrderBy(r => r.Id)
                .Select(r => new { r.Id, V = r.Flag ? (r.NullableInt ?? -1) : (r.IntVal > 0 ? r.IntVal : -2) })
                .AsEnumerable().Select(x => (x.Id, x.V)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"nested-ternary projection mismatch\nexpected [{string.Join(" | ", expected.Take(12))}]\nactual   [{string.Join(" | ", actual.Take(12))}]");
    }

    // ── 22. Average(decimal) precision over groups ────────────────────────────
    [Fact]
    public void GroupBy_Average_Decimal_Precision()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.GroupBy(r => r.Cat)
            .Select(g => new { g.Key, A = g.Average(x => x.Amount) })
            .OrderBy(x => x.Key)
            .Select(x => (x.Key, x.A)).ToList();

        List<(int Key, decimal A)> actual;
        try
        {
            actual = ctx.Query<ARow>().GroupBy(r => r.Cat)
                .Select(g => new { g.Key, A = g.Average(x => x.Amount) })
                .OrderBy(x => x.Key)
                .AsEnumerable().Select(x => (x.Key, x.A)).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.True(expected[i].Key == actual[i].Key && Math.Abs(expected[i].A - actual[i].A) < 0.0000001m,
                $"Average(decimal) mismatch at {i}: expected {expected[i]} actual {actual[i]}");
        }
    }

    // ── 23. Correlated OR-EXISTS with a nullable predicate inside ─────────────
    [Fact]
    public void Correlated_Or_Exists_With_Nullable()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;

        var expected = Rows.Where(r =>
                Children.Any(c => c.ParentId == r.Id && c.ChildVal > 0)
                || (r.NullableInt != null && r.NullableInt > 1))
            .Select(r => r.Id).OrderBy(x => x).ToList();

        List<int> actual;
        try
        {
            actual = ctx.Query<ARow>().Where(r =>
                    ctx.Query<AChild>().Any(c => c.ParentId == r.Id && c.ChildVal > 0)
                    || (r.NullableInt != null && r.NullableInt > 1))
                .AsEnumerable().Select(r => r.Id).OrderBy(x => x).ToList();
        }
        catch (Exception ex) when (IsDeclined(ex)) { return; }

        Assert.True(expected.SequenceEqual(actual),
            $"correlated OR-EXISTS mismatch\nexpected [{string.Join(",", expected)}]\nactual   [{string.Join(",", actual)}]");
    }

    // ── Diagnostic: report whether each nORM shape executed or was declined ───
    // (Always fails so the report prints; disabled by default via Skip.)
    [Fact(Skip = "diagnostic — flip on to print an executed/declined coverage report")]
    public void _Diagnostic_Coverage_Report()
    {
        var (ctx, cn) = NewContext();
        using var _ = cn;
        using var __ = ctx;
        var ids = new int?[] { 1, null, 2 };
        var report = new List<string>();

        void Probe(string name, Func<object> run)
        {
            try { var r = run(); report.Add($"{name}: EXECUTED -> {Describe(r)}"); }
            catch (NormUnsupportedFeatureException e) { report.Add($"{name}: DECLINED ({e.GetType().Name}: {e.Message})"); }
            catch (Exception e) { report.Add($"{name}: THREW ({e.GetType().Name}: {e.Message})"); }
        }

        Probe("1 GroupBy_Having", () => ctx.Query<ARow>().GroupBy(r => r.Cat).Where(g => g.Count() >= 3)
            .Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.IntVal) })
            .OrderByDescending(x => x.S).ThenBy(x => x.Key).Skip(1).Take(3).AsEnumerable().ToList());
        Probe("2 GroupBy_Avg_Min_Max", () => ctx.Query<ARow>().GroupBy(r => r.IntVal % 3)
            .Select(g => new { Key = g.Key, C = g.Count(), Mn = g.Min(x => x.IntVal), Mx = g.Max(x => x.IntVal), A = g.Average(x => x.IntVal) })
            .OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("3 GroupBy_NullableAgg", () => ctx.Query<ARow>().GroupBy(r => r.Cat)
            .Select(g => new { g.Key, Mx = g.Max(x => (int?)x.NullableInt), Mn = g.Min(x => (int?)x.NullableInt) })
            .OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("4 Distinct_Order_Skip_Take", () => ctx.Query<ARow>().Select(r => r.IntVal).Distinct().OrderBy(x => x).Skip(1).Take(4).AsEnumerable().ToList());
        Probe("5 Distinct_MultiCol_Count", () => ctx.Query<ARow>().Select(r => new { r.IntVal, r.Flag }).Distinct().Count());
        Probe("6 Union_Except_chain", () => ctx.Query<ARow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(ctx.Query<ARow>().Where(r => r.IntVal < 0).Select(r => r.IntVal))
            .Except(ctx.Query<ARow>().Where(r => r.IntVal == 0).Select(r => r.IntVal)).AsEnumerable().ToList());
        Probe("7 Concat_chain", () => ctx.Query<ARow>().Where(r => r.IntVal >= 1).Select(r => r.IntVal)
            .Concat(ctx.Query<ARow>().Where(r => r.IntVal <= -1).Select(r => r.IntVal))
            .Concat(ctx.Query<ARow>().Where(r => r.Flag).Select(r => r.IntVal)).AsEnumerable().ToList());
        Probe("8 SetOp_Order_Page", () => ctx.Query<ARow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
            .Union(ctx.Query<ARow>().Where(r => r.Flag).Select(r => r.IntVal))
            .OrderByDescending(x => x).Skip(1).Take(3).AsEnumerable().ToList());
        Probe("9 Join_GroupBy", () => ctx.Query<ARow>().Join(ctx.Query<AChild>(), r => r.Id, c => c.ParentId, (r, c) => new { r.IntVal, c.ChildVal })
            .GroupBy(x => x.IntVal).Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal) })
            .OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("10 OrderBy_NullableKey", () => ctx.Query<ARow>().OrderBy(r => r.NullableInt).ThenBy(r => r.Id).Skip(2).Take(12).AsEnumerable().Select(r => r.Id).ToList());
        Probe("11 Ternary_Where_Nullable", () => ctx.Query<ARow>().Where(r => (r.Flag ? r.NullableInt : (int?)r.IntVal) > 0).AsEnumerable().Select(r => r.Id).ToList());
        Probe("12 OrderBy_CorrelatedCount", () => ctx.Query<ARow>().OrderBy(r => ctx.Query<AChild>().Count(c => c.ParentId == r.Id)).ThenBy(r => r.Id).Take(18).AsEnumerable().Select(r => r.Id).ToList());
        Probe("13 Projection_Subquery_Where_Order", () => ctx.Query<ARow>().Select(r => new { r.Id, N = ctx.Query<AChild>().Count(c => c.ParentId == r.Id) })
            .Where(x => x.N >= 2).OrderByDescending(x => x.N).ThenBy(x => x.Id).Take(10).AsEnumerable().ToList());
        Probe("14a Contains_null_pos", () => ctx.Query<ARow>().Where(r => ids.Contains(r.NullableInt)).AsEnumerable().Select(r => r.Id).ToList());
        Probe("14b Contains_null_neg", () => ctx.Query<ARow>().Where(r => !ids.Contains(r.NullableInt)).AsEnumerable().Select(r => r.Id).ToList());
        Probe("15 GroupBy_FilteredAgg", () => ctx.Query<ARow>().GroupBy(r => r.Cat)
            .Select(g => new { g.Key, PosCount = g.Count(x => x.IntVal > 0), FlagSum = g.Sum(x => x.Flag ? x.IntVal : 0) })
            .OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("16a Empty_MaxNullable", () => (object?)ctx.Query<ARow>().Where(r => r.Id < 0).Max(r => (int?)r.IntVal) ?? "null");
        Probe("16b Empty_SumNullable", () => (object?)ctx.Query<ARow>().Where(r => r.Id < 0).Sum(r => (int?)r.IntVal) ?? "null");
        Probe("16c AllNull_MaxNullable", () => (object?)ctx.Query<ARow>().Where(r => r.Cat == 3).Max(r => r.NullableInt) ?? "null");
        Probe("16d AllNull_SumNullable", () => (object?)ctx.Query<ARow>().Where(r => r.Cat == 3).Sum(r => r.NullableInt) ?? "null");
        Probe("17 Take_Then_Where", () => ctx.Query<ARow>().OrderBy(r => r.Id).Take(12).Where(r => r.Flag).AsEnumerable().Select(r => r.Id).ToList());
        Probe("18a Paging_Then_Count", () => ctx.Query<ARow>().OrderBy(r => r.Id).Skip(5).Take(10).Count());
        Probe("18b Where_Paging_Then_Count", () => ctx.Query<ARow>().Where(r => r.IntVal >= 0).OrderBy(r => r.Id).Skip(3).Take(4).Count());
        Probe("19 Having_On_Sum", () => ctx.Query<ARow>().GroupBy(r => r.Cat).Where(g => g.Sum(x => x.IntVal) > 0)
            .Select(g => new { g.Key, S = g.Sum(x => x.IntVal) }).OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("20a IN_int", () => ctx.Query<ARow>().Where(r => new[] { -1, 0, 2, 2, 3 }.Contains(r.IntVal)).AsEnumerable().Select(r => r.Id).ToList());
        Probe("20b NOTIN_int", () => ctx.Query<ARow>().Where(r => !new[] { -1, 0, 2, 2, 3 }.Contains(r.IntVal)).AsEnumerable().Select(r => r.Id).ToList());
        Probe("21 Nested_Ternary", () => ctx.Query<ARow>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, V = r.Flag ? (r.NullableInt ?? -1) : (r.IntVal > 0 ? r.IntVal : -2) }).AsEnumerable().ToList());
        Probe("22 Average_Decimal", () => ctx.Query<ARow>().GroupBy(r => r.Cat).Select(g => new { g.Key, A = g.Average(x => x.Amount) }).OrderBy(x => x.Key).AsEnumerable().ToList());
        Probe("23 OrExists_Nullable", () => ctx.Query<ARow>().Where(r => ctx.Query<AChild>().Any(c => c.ParentId == r.Id && c.ChildVal > 0)
            || (r.NullableInt != null && r.NullableInt > 1)).AsEnumerable().Select(r => r.Id).ToList());

        Assert.Fail("COVERAGE REPORT:\n" + string.Join("\n", report));
    }

    private static string Describe(object r)
    {
        if (r is System.Collections.ICollection c) return $"{c.Count} rows";
        return r?.ToString() ?? "null";
    }
}
