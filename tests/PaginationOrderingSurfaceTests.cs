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
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pagination/ordering correctness. Each probe builds a query via nORM AND the SAME query via LINQ-to-Objects
/// over an identical in-memory seed, then asserts the row SEQUENCES are equal (order matters). Deliberate TIES
/// on sort keys + distinguishable ids expose silent drops/dups/misorders across page boundaries.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PaginationOrderingSurfaceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    public PaginationOrderingSurfaceTests(ITestOutputHelper o) => _out = o;

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    // Seed: Id unique PK 1..12 (= insertion order = rowid order).
    // K has deliberate ties. Cat is a group key. Name for text ordering.
    // Id:  1  2  3  4  5  6  7  8  9 10 11 12
    // K :  5  5  5  2  2  8  8  8  8  1  9  9
    // Cat: 1  1  2  2  1  2  1  2  1  2  1  2
    private static readonly (int Id, int K, int Cat, string Name)[] Seed = new[]
    {
        (1, 5, 1, "e"),
        (2, 5, 1, "b"),
        (3, 5, 2, "h"),
        (4, 2, 2, "a"),
        (5, 2, 1, "f"),
        (6, 8, 2, "d"),
        (7, 8, 1, "g"),
        (8, 8, 2, "c"),
        (9, 8, 1, "i"),
        (10, 1, 2, "j"),
        (11, 9, 1, "k"),
        (12, 9, 2, "l"),
    };

    private List<HuntRow> _oracle = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        var values = string.Join(",", Seed.Select(r => $"({r.Id},{r.K},{r.Cat},'{r.Name}')"));
        cmd.CommandText =
            "CREATE TABLE HuntRow (Id INTEGER PRIMARY KEY, K INTEGER NOT NULL, Cat INTEGER NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO HuntRow (Id,K,Cat,Name) VALUES " + values + ";";
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
        _oracle = Seed.Select(r => new HuntRow { Id = r.Id, K = r.K, Cat = r.Cat, Name = r.Name }).ToList();
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    private IQueryable<HuntRow> Q => _ctx.Query<HuntRow>();
    private IEnumerable<HuntRow> O => _oracle;

    private static int[] Ids(IEnumerable<HuntRow> rows) => rows.Select(r => r.Id).ToArray();

    private void AssertSeqEqual(string label, IEnumerable<HuntRow> expected, IEnumerable<HuntRow> actual)
    {
        var e = Ids(expected);
        var a = Ids(actual);
        _out.WriteLine($"{label}\n  oracle: [{string.Join(",", e)}]\n  norm  : [{string.Join(",", a)}]");
        Assert.Equal(e, a);
    }

    private void AssertMultisetEqual(string label, IEnumerable<HuntRow> expected, IEnumerable<HuntRow> actual)
    {
        var e = Ids(expected).OrderBy(x => x).ToArray();
        var a = Ids(actual).OrderBy(x => x).ToArray();
        _out.WriteLine($"{label}\n  oracle(set): [{string.Join(",", e)}]\n  norm  (set): [{string.Join(",", a)}]");
        Assert.Equal(e, a);
    }

    // ---- Surface 1: basic Skip/Take on a unique key -------------------------
    [Fact]
    public void S1_OrderBy_Skip_Take_unique_key()
    {
        AssertSeqEqual("S1",
            O.OrderBy(r => r.Id).Skip(2).Take(3),
            Q.OrderBy(r => r.Id).Skip(2).Take(3).ToList());
    }

    // ---- Surface 2: paging across a TIE boundary ----------------------------
    // Cross-page: Page1 ++ Page2 must equal the first 2N rows, no overlap, no gap.
    [Fact]
    public void S2_tie_key_cross_page_no_overlap_no_gap()
    {
        const int N = 3;
        // nORM pages (two separate executions).
        var p1 = Q.OrderBy(r => r.K).Skip(0).Take(N).ToList();
        var p2 = Q.OrderBy(r => r.K).Skip(N).Take(N).ToList();
        var combined = p1.Concat(p2).ToList();
        // The union of the first two pages must equal SOME first-2N window of an
        // ORDER BY K sequence: no id may repeat, and it must be a contiguous prefix.
        Assert.Equal(2 * N, combined.Select(r => r.Id).Distinct().Count()); // no overlap/dup
        // Contiguity: combined must be a first-2N slice of a valid ORDER BY K order.
        // Every combined row's K must be <= every non-combined row's K (prefix property).
        var combinedIds = combined.Select(r => r.Id).ToHashSet();
        var maxKIn = combined.Max(r => r.K);
        var minKOut = _oracle.Where(r => !combinedIds.Contains(r.Id)).Min(r => r.K);
        // prefix property: no row outside the first 2N has a strictly smaller K than one inside
        // (ties on the boundary K are allowed to fall either way, but a clean prefix means
        // minKOut >= every K in combined except possibly the boundary tie value).
        _out.WriteLine($"S2 p1=[{string.Join(",", Ids(p1))}] p2=[{string.Join(",", Ids(p2))}] maxKIn={maxKIn} minKOut={minKOut}");
        Assert.True(minKOut >= combined.OrderBy(r => r.K).Skip(2 * N - 1).First().K,
            $"page prefix broken: minKOut={minKOut}");
    }

    // Full sweep across ALL pages must reconstruct the whole table exactly once.
    [Fact]
    public void S2b_tie_key_full_paging_sweep_covers_every_row_once()
    {
        const int N = 3;
        var all = new List<HuntRow>();
        for (int page = 0; page * N < Seed.Length; page++)
            all.AddRange(Q.OrderBy(r => r.K).Skip(page * N).Take(N).ToList());
        AssertMultisetEqual("S2b full-sweep", O, all);
    }

    // Exact-sequence when the sort key is made unique by a tiebreaker: MUST match oracle.
    [Fact]
    public void S2c_tie_key_with_id_tiebreaker_exact_sequence()
    {
        AssertSeqEqual("S2c",
            O.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(3).Take(4),
            Q.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(3).Take(4).ToList());
    }

    // ---- Surface 3: Take/Skip without OrderBy (multiset only) ---------------
    [Fact]
    public void S3_Take_without_orderby_returns_subset()
    {
        var rows = Q.Take(5).ToList();
        Assert.Equal(5, rows.Count);
        Assert.All(rows, r => Assert.Contains(r.Id, Seed.Select(s => s.Id)));
        Assert.Equal(5, rows.Select(r => r.Id).Distinct().Count());
    }

    // ---- Surface 4: Distinct combinations -----------------------------------
    [Fact]
    public void S4a_OrderBy_then_Distinct_scalar_projection()
    {
        // Distinct K values, ordered.
        AssertSeqEqualScalar("S4a",
            O.Select(r => r.K).Distinct().OrderBy(k => k),
            Q.Select(r => r.K).Distinct().OrderBy(k => k).ToList());
    }

    [Fact]
    public void S4b_Distinct_scalar_then_Skip_Take()
    {
        AssertSeqEqualScalar("S4b",
            O.Select(r => r.K).Distinct().OrderBy(k => k).Skip(1).Take(2),
            Q.Select(r => r.K).Distinct().OrderBy(k => k).Skip(1).Take(2).ToList());
    }

    [Fact]
    public void S4c_Distinct_entity_OrderBy_Skip_Take()
    {
        AssertSeqEqual("S4c",
            O.Distinct().OrderBy(r => r.Id).Skip(4).Take(4),
            Q.Distinct().OrderBy(r => r.Id).Skip(4).Take(4).ToList());
    }

    // ---- Surface 5: OrderByDescending + multi-key ThenBy --------------------
    [Fact]
    public void S5a_OrderByDescending_Skip_Take()
    {
        AssertSeqEqual("S5a",
            O.OrderByDescending(r => r.Id).Skip(2).Take(3),
            Q.OrderByDescending(r => r.Id).Skip(2).Take(3).ToList());
    }

    [Fact]
    public void S5b_multi_key_KthenId_paged_exact()
    {
        AssertSeqEqual("S5b",
            O.OrderBy(r => r.K).ThenByDescending(r => r.Id).Skip(2).Take(5),
            Q.OrderBy(r => r.K).ThenByDescending(r => r.Id).Skip(2).Take(5).ToList());
    }

    [Fact]
    public void S5c_CatThenKThenId_paged_exact()
    {
        AssertSeqEqual("S5c",
            O.OrderBy(r => r.Cat).ThenBy(r => r.K).ThenBy(r => r.Id).Skip(3).Take(6),
            Q.OrderBy(r => r.Cat).ThenBy(r => r.K).ThenBy(r => r.Id).Skip(3).Take(6).ToList());
    }

    [Fact]
    public void S5d_OrderByDesc_ThenBy_Name_ties_exact()
    {
        // K desc then Name asc then Id - fully deterministic
        AssertSeqEqual("S5d",
            O.OrderByDescending(r => r.K).ThenBy(r => r.Name).ThenBy(r => r.Id).Skip(1).Take(7),
            Q.OrderByDescending(r => r.K).ThenBy(r => r.Name).ThenBy(r => r.Id).Skip(1).Take(7).ToList());
    }

    // ---- Surface 6: OrderBy computed column then page -----------------------
    [Fact]
    public void S6a_OrderBy_computed_expr_paged()
    {
        AssertSeqEqual("S6a",
            O.OrderBy(r => r.K * 100 + r.Id).Skip(2).Take(4),
            Q.OrderBy(r => r.K * 100 + r.Id).Skip(2).Take(4).ToList());
    }

    [Fact]
    public void S6b_OrderBy_name_length_then_id_paged()
    {
        AssertSeqEqual("S6b",
            O.OrderBy(r => r.Name.Length).ThenBy(r => r.Id).Skip(1).Take(5),
            Q.OrderBy(r => r.Name.Length).ThenBy(r => r.Id).Skip(1).Take(5).ToList());
    }

    // ---- Surface 7: boundary conditions -------------------------------------
    [Fact]
    public void S7a_Skip0_Take0_beyond()
    {
        AssertSeqEqual("S7a-skip0", O.OrderBy(r => r.Id).Skip(0).Take(3), Q.OrderBy(r => r.Id).Skip(0).Take(3).ToList());
        Assert.Empty(Q.OrderBy(r => r.Id).Take(0).ToList());
        AssertSeqEqual("S7a-takebeyond", O.OrderBy(r => r.Id).Take(100), Q.OrderBy(r => r.Id).Take(100).ToList());
        Assert.Empty(Q.OrderBy(r => r.Id).Skip(100).ToList());
    }

    [Fact]
    public void S7b_take_exactly_at_boundary()
    {
        AssertSeqEqual("S7b", O.OrderBy(r => r.Id).Skip(9).Take(3), Q.OrderBy(r => r.Id).Skip(9).Take(3).ToList());
        // exactly count
        AssertSeqEqual("S7b-exact", O.OrderBy(r => r.Id).Take(12), Q.OrderBy(r => r.Id).Take(12).ToList());
    }

    // ---- Surface 8: paging after GroupBy / Join / Union ---------------------
    [Fact]
    public void S8a_GroupBy_project_then_Skip_Take()
    {
        // group by Cat, count, order by Cat, page.
        var oracle = O.GroupBy(r => r.Cat).Select(g => new { Cat = g.Key, Cnt = g.Count() }).OrderBy(x => x.Cat).Skip(0).Take(2).ToList();
        var norm = Q.GroupBy(r => r.Cat).Select(g => new { Cat = g.Key, Cnt = g.Count() }).OrderBy(x => x.Cat).Skip(0).Take(2).ToList();
        _out.WriteLine("S8a oracle: " + string.Join(",", oracle.Select(x => $"{x.Cat}:{x.Cnt}")));
        _out.WriteLine("S8a norm  : " + string.Join(",", norm.Select(x => $"{x.Cat}:{x.Cnt}")));
        Assert.Equal(oracle.Count, norm.Count);
        for (int i = 0; i < oracle.Count; i++)
        {
            Assert.Equal(oracle[i].Cat, norm[i].Cat);
            Assert.Equal(oracle[i].Cnt, norm[i].Cnt);
        }
    }

    [Fact]
    public void S8b_Join_then_OrderBy_Skip_Take()
    {
        // self-join on Cat is cartesian; instead join HuntRow to a small key set.
        var oracle = O.Join(O, a => a.Id, b => b.Id, (a, b) => new { a.Id, a.K })
            .OrderBy(x => x.K).ThenBy(x => x.Id).Skip(3).Take(4).ToList();
        var norm = Q.Join(Q, a => a.Id, b => b.Id, (a, b) => new { a.Id, a.K })
            .OrderBy(x => x.K).ThenBy(x => x.Id).Skip(3).Take(4).ToList();
        _out.WriteLine("S8b oracle: " + string.Join(",", oracle.Select(x => x.Id)));
        _out.WriteLine("S8b norm  : " + string.Join(",", norm.Select(x => x.Id)));
        Assert.Equal(oracle.Select(x => x.Id), norm.Select(x => x.Id));
    }

    [Fact]
    public void S8c_Concat_then_OrderBy_Skip_Take()
    {
        var lowIds = new[] { 1, 2, 3, 4 };
        var oracle = O.Where(r => r.Id <= 4).Select(r => r.Id)
            .Concat(O.Where(r => r.Id >= 9).Select(r => r.Id))
            .OrderBy(x => x).Skip(1).Take(4).ToList();
        var norm = Q.Where(r => r.Id <= 4).Select(r => r.Id)
            .Concat(Q.Where(r => r.Id >= 9).Select(r => r.Id))
            .OrderBy(x => x).Skip(1).Take(4).ToList();
        _out.WriteLine("S8c oracle: " + string.Join(",", oracle));
        _out.WriteLine("S8c norm  : " + string.Join(",", norm));
        Assert.Equal(oracle, norm);
    }

    // ---- Surface 9: First/Single/Last/ElementAt with OrderBy ----------------
    [Fact]
    public void S9a_First_after_OrderBy()
    {
        Assert.Equal(O.OrderBy(r => r.K).ThenBy(r => r.Id).First().Id,
                     Q.OrderBy(r => r.K).ThenBy(r => r.Id).First().Id);
    }

    [Fact]
    public void S9b_Last_after_OrderBy()
    {
        Assert.Equal(O.OrderBy(r => r.K).ThenBy(r => r.Id).Last().Id,
                     Q.OrderBy(r => r.K).ThenBy(r => r.Id).Last().Id);
    }

    [Fact]
    public void S9c_ElementAt_after_OrderBy()
    {
        for (int i = 0; i < Seed.Length; i++)
        {
            var oid = O.OrderBy(r => r.K).ThenBy(r => r.Id).ElementAt(i).Id;
            var nid = Q.OrderBy(r => r.K).ThenBy(r => r.Id).ElementAt(i).Id;
            Assert.Equal(oid, nid);
        }
    }

    [Fact]
    public void S9d_First_after_Skip()
    {
        Assert.Equal(O.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(5).First().Id,
                     Q.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(5).First().Id);
    }

    // ---- Surface 10: reverse / stability ------------------------------------
    [Fact]
    public void S10a_reverse_of_asc_equals_desc_paged()
    {
        AssertSeqEqual("S10a",
            O.OrderByDescending(r => r.K).ThenByDescending(r => r.Id).Skip(2).Take(4),
            Q.OrderBy(r => r.K).ThenBy(r => r.Id).Reverse().Skip(2).Take(4).ToList());
    }

    // ---- Surface 11: large offset -------------------------------------------
    [Fact]
    public void S11_large_offset_exact_window()
    {
        // 12 rows; Skip(8).Take(3) over unique order.
        AssertSeqEqual("S11",
            O.OrderBy(r => r.Id).Skip(8).Take(3),
            Q.OrderBy(r => r.Id).Skip(8).Take(3).ToList());
    }

    // ---- Surface 12: keyset / seek pagination -------------------------------
    [Fact]
    public void S12_keyset_seek_pagination()
    {
        // page by (K, Id) keyset. Grab first page then seek past the last.
        var page1 = Q.OrderBy(r => r.K).ThenBy(r => r.Id).Take(4).ToList();
        var last = page1[^1];
        var page2 = Q.Where(r => r.K > last.K || (r.K == last.K && r.Id > last.Id))
            .OrderBy(r => r.K).ThenBy(r => r.Id).Take(4).ToList();
        var oracle = O.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(4).Take(4).ToList();
        _out.WriteLine("S12 page2 : " + string.Join(",", Ids(page2)));
        _out.WriteLine("S12 oracle: " + string.Join(",", Ids(oracle)));
        Assert.Equal(Ids(oracle), Ids(page2));
    }

    // ---- Extra: Take(n).Skip(m) composite algebra ---------------------------
    [Fact]
    public void EX1_Take_then_Skip_composite()
    {
        AssertSeqEqual("EX1",
            O.OrderBy(r => r.Id).Take(8).Skip(3),
            Q.OrderBy(r => r.Id).Take(8).Skip(3).ToList());
    }

    [Fact]
    public void EX2_Skip_Take_Skip_composite()
    {
        AssertSeqEqual("EX2",
            O.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2),
            Q.OrderBy(r => r.Id).Skip(2).Take(6).Skip(2).ToList());
    }

    [Fact]
    public void EX3_OrderBy_Take_window_then_resort_with_ties()
    {
        // Take top-6 by (K,Id) [deterministic], then resort by Name.
        AssertSeqEqual("EX3",
            O.OrderBy(r => r.K).ThenBy(r => r.Id).Take(6).OrderBy(r => r.Name).ThenBy(r => r.Id),
            Q.OrderBy(r => r.K).ThenBy(r => r.Id).Take(6).OrderBy(r => r.Name).ThenBy(r => r.Id).ToList());
    }

    [Fact]
    public void EX4_OrderBy_Skip_Take_window_then_resort()
    {
        AssertSeqEqual("EX4",
            O.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(2).Take(6).OrderByDescending(r => r.Id),
            Q.OrderBy(r => r.K).ThenBy(r => r.Id).Skip(2).Take(6).OrderByDescending(r => r.Id).ToList());
    }

    [Fact]
    public void EX5_Where_then_OrderBy_Skip_Take()
    {
        AssertSeqEqual("EX5",
            O.Where(r => r.Cat == 1).OrderBy(r => r.K).ThenBy(r => r.Id).Skip(1).Take(3),
            Q.Where(r => r.Cat == 1).OrderBy(r => r.K).ThenBy(r => r.Id).Skip(1).Take(3).ToList());
    }

    [Fact]
    public void EX6_captured_page_variables()
    {
        int pageSize = 4;
        for (int pageIndex = 0; pageIndex < 3; pageIndex++)
        {
            var oracle = O.OrderBy(r => r.Id).Skip(pageSize * pageIndex).Take(pageSize).ToList();
            var norm = Q.OrderBy(r => r.Id).Skip(pageSize * pageIndex).Take(pageSize).ToList();
            AssertSeqEqual($"EX6 page{pageIndex}", oracle, norm);
        }
    }

    private void AssertSeqEqualScalar(string label, IEnumerable<int> expected, IEnumerable<int> actual)
    {
        var e = expected.ToArray();
        var a = actual.ToArray();
        _out.WriteLine($"{label}\n  oracle: [{string.Join(",", e)}]\n  norm  : [{string.Join(",", a)}]");
        Assert.Equal(e, a);
    }

    [Table("HuntRow")]
    public sealed class HuntRow : IEquatable<HuntRow>
    {
        [Key] public int Id { get; set; }
        public int K { get; set; }
        public int Cat { get; set; }
        public string Name { get; set; } = string.Empty;

        public bool Equals(HuntRow? other) => other != null && other.Id == Id && other.K == K && other.Cat == Cat && other.Name == Name;
        public override bool Equals(object? obj) => Equals(obj as HuntRow);
        public override int GetHashCode() => HashCode.Combine(Id, K, Cat, Name);
    }
}
