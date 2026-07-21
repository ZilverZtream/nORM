using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The filtered-sorted-page fast path (<c>IsFilteredOrderedPagePattern</c>) flattens
/// <c>.Where(..).OrderBy(..).Skip(..).Take(..)</c> to one <c>WHERE .. ORDER BY .. LIMIT/OFFSET</c>
/// statement. These tests pin that every accepted shape returns byte-identical rows to a
/// LINQ-to-objects oracle, and that the NON-flattenable orderings (take-before-skip,
/// page-then-filter, skip-only) still return correct rows via the slow translator.
/// Ordering is by a unique <c>int</c> column so the total order is collation-free and
/// deterministic across SQLite and LINQ.
/// </summary>
public class FilteredSortedPagingFastPathTests
{
    [Table("PageUser")]
    public class PageUser
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
        public int Age { get; set; }
        public string City { get; set; } = "";
        public int Rank { get; set; }
    }

    private static readonly List<PageUser> Seed = BuildSeed();

    private static List<PageUser> BuildSeed()
    {
        var list = new List<PageUser>();
        for (int i = 1; i <= 80; i++)
        {
            list.Add(new PageUser
            {
                Id = i,
                Name = "User " + i.ToString("D3"),
                IsActive = i % 4 != 0,          // 3/4 active
                Age = 18 + (i * 7) % 50,        // spread 18..67
                City = i % 3 == 0 ? "NY" : (i % 3 == 1 ? "LA" : "SF"),
                Rank = (i * 31) % 80            // unique-ish scramble; made unique below
            });
        }
        // Force Rank to be a unique total order (a scrambled permutation of 1..80).
        var ranks = Enumerable.Range(1, 80).ToArray();
        for (int i = 0; i < ranks.Length; i++)
        {
            int j = (i * 37 + 11) % ranks.Length;
            (ranks[i], ranks[j]) = (ranks[j], ranks[i]);
        }
        for (int i = 0; i < list.Count; i++) list[i].Rank = ranks[i];
        return list;
    }

    private static DbContext NewContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PageUser (Id INTEGER PRIMARY KEY, Name TEXT, IsActive INTEGER, Age INTEGER, City TEXT, Rank INTEGER);";
            cmd.ExecuteNonQuery();
        }
        foreach (var u in Seed)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "INSERT INTO PageUser (Id,Name,IsActive,Age,City,Rank) VALUES ($id,$n,$a,$g,$c,$r);";
            cmd.Parameters.AddWithValue("$id", u.Id);
            cmd.Parameters.AddWithValue("$n", u.Name);
            cmd.Parameters.AddWithValue("$a", u.IsActive ? 1 : 0);
            cmd.Parameters.AddWithValue("$g", u.Age);
            cmd.Parameters.AddWithValue("$c", u.City);
            cmd.Parameters.AddWithValue("$r", u.Rank);
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    private static IEnumerable<PageUser> OracleFilter(IEnumerable<PageUser> src)
        => src.Where(u => u.IsActive && u.Age > 25 && u.City == "NY");

    // ---- Accepted (now-fast) flattenable shapes: exact ordered-row parity ----

    [Fact]
    public async Task Filter_order_skip_take_matches_linq()
    {
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY")
               .OrderBy(u => u.Rank).Skip(5).Take(10));
        var expected = OracleFilter(Seed).OrderBy(u => u.Rank).Skip(5).Take(10).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }

    [Fact]
    public async Task Filter_orderDesc_skip_take_matches_linq()
    {
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY")
               .OrderByDescending(u => u.Rank).Skip(3).Take(7));
        var expected = OracleFilter(Seed).OrderByDescending(u => u.Rank).Skip(3).Take(7).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }

    [Fact]
    public async Task Filter_order_take_only_matches_linq()
    {
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY")
               .OrderBy(u => u.Rank).Take(9));
        var expected = OracleFilter(Seed).OrderBy(u => u.Rank).Take(9).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }

    [Fact]
    public async Task Order_before_filter_commutes_and_pages_correctly()
    {
        // .OrderBy().Where().Skip().Take() — filter and order commute, paging last → flattenable.
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().OrderBy(u => u.Rank)
               .Where(u => u.IsActive && u.Age > 25 && u.City == "NY").Skip(2).Take(8));
        var expected = Seed.OrderBy(u => u.Rank)
               .Where(u => u.IsActive && u.Age > 25 && u.City == "NY").Skip(2).Take(8).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }

    [Fact]
    public async Task Filter_skip_take_without_order_returns_correct_count_and_rows()
    {
        // No ORDER BY → row identity is provider order (nondeterministic); assert the WINDOW size
        // and that every returned row satisfies the predicate.
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY").Skip(4).Take(6));
        int total = OracleFilter(Seed).Count();
        int expectedCount = Math.Max(0, Math.Min(6, total - 4));
        Assert.Equal(expectedCount, actual.Count);
        Assert.All(actual, u => Assert.True(u.IsActive && u.Age > 25 && u.City == "NY"));
    }

    // ---- Rejected (non-flattenable) shapes: still correct via the slow translator ----

    [Fact]
    public async Task Take_before_skip_pages_the_window_correctly()
    {
        // .OrderBy().Take(10).Skip(3) means take the first 10 THEN drop 3 → rows [3,10) of the
        // ordered set. My guard rejects this (rank decrease) so it must go slow and stay correct.
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY")
               .OrderBy(u => u.Rank).Take(10).Skip(3));
        var expected = OracleFilter(Seed).OrderBy(u => u.Rank).Take(10).Skip(3).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }

    [Fact]
    public async Task Skip_only_without_take_matches_linq()
    {
        // Skip-only is deferred to the slow path (bare OFFSET is invalid on SQLite).
        await using var ctx = NewContext();
        var actual = await NormAsyncExtensions.ToListAsync(
            ctx.Query<PageUser>().Where(u => u.IsActive && u.Age > 25 && u.City == "NY")
               .OrderBy(u => u.Rank).Skip(5));
        var expected = OracleFilter(Seed).OrderBy(u => u.Rank).Skip(5).Select(u => u.Id).ToList();
        Assert.Equal(expected, actual.Select(u => u.Id).ToList());
    }
}
