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

namespace nORM.Tests;

/// <summary>
/// Covers the client-tail sequence operators Chunk, Append, Prepend, and Zip:
/// the source query runs server-side and the operator reshapes the materialized
/// rows, matching LINQ-to-Objects semantics including ordering, empty sources,
/// remainder chunks, and shorter-sequence Zip truncation.
/// </summary>
[Trait("Category", "Fast")]
public class LinqSequenceTailOperatorTests
{
    [Table("SeqTailItem")]
    private class SeqTailItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(int rowCount)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE SeqTailItem (
                    Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name  TEXT    NOT NULL,
                    Value INTEGER NOT NULL
                )";
            cmd.ExecuteNonQuery();
        }
        for (var i = 1; i <= rowCount; i++)
        {
            using var insert = cn.CreateCommand();
            insert.CommandText = "INSERT INTO SeqTailItem (Name, Value) VALUES (@n, @v)";
            insert.Parameters.AddWithValue("@n", $"item{i}");
            insert.Parameters.AddWithValue("@v", i * 10);
            insert.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Chunk ────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(7, 3)] // remainder chunk
    [InlineData(6, 3)] // exact multiple
    [InlineData(2, 5)] // size larger than count
    [InlineData(5, 1)] // size one
    [InlineData(0, 3)] // empty source
    public async Task Chunk_matches_linq_to_objects(int rowCount, int size)
    {
        var (cn, ctx) = CreateContext(rowCount);
        using var _cn = cn;
        using var _ctx = ctx;

        var chunks = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(size).ToListAsync();
        var expected = Enumerable.Range(1, rowCount).Select(i => i * 10).Chunk(size).ToList();

        Assert.Equal(expected.Count, chunks.Count);
        for (var i = 0; i < expected.Count; i++)
            Assert.Equal(expected[i], chunks[i].Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Chunk_after_where_chunks_only_matching_rows()
    {
        var (cn, ctx) = CreateContext(10);
        using var _cn = cn;
        using var _ctx = ctx;

        var chunks = await ctx.Query<SeqTailItem>()
            .Where(x => x.Value > 30)
            .OrderBy(x => x.Id)
            .Chunk(4)
            .ToListAsync();

        Assert.Equal(2, chunks.Count);
        Assert.Equal(new[] { 40, 50, 60, 70 }, chunks[0].Select(x => x.Value).ToArray());
        Assert.Equal(new[] { 80, 90, 100 }, chunks[1].Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Chunk_size_below_one_throws_argument_out_of_range()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => ctx.Query<SeqTailItem>().Chunk(0).ToListAsync());
    }

    [Fact]
    public async Task Chunk_with_different_captured_sizes_does_not_reuse_a_stale_plan()
    {
        var (cn, ctx) = CreateContext(6);
        using var _cn = cn;
        using var _ctx = ctx;

        var sizeTwo = 2;
        var sizeThree = 3;
        var twoChunks = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(sizeTwo).ToListAsync();
        var threeChunks = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(sizeThree).ToListAsync();

        Assert.Equal(3, twoChunks.Count);
        Assert.Equal(2, threeChunks.Count);
    }

    // ── Append / Prepend ─────────────────────────────────────────────────────

    [Fact]
    public async Task Append_adds_the_captured_entity_after_the_query_rows()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Id = 99, Name = "extra", Value = 999 };
        var rows = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { 10, 20, 30, 999 }, rows.Select(x => x.Value).ToArray());
        Assert.Same(extra, rows[^1]);
    }

    [Fact]
    public async Task Prepend_adds_the_captured_entity_before_the_query_rows()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Id = 99, Name = "extra", Value = 999 };
        var rows = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra).ToListAsync();

        Assert.Equal(4, rows.Count);
        Assert.Equal(new[] { 999, 10, 20, 30 }, rows.Select(x => x.Value).ToArray());
        Assert.Same(extra, rows[0]);
    }

    [Fact]
    public void Append_on_projected_scalars_matches_linq_to_objects()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var values = ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Select(x => x.Value)
            .Append(999)
            .ToList();

        Assert.Equal(new[] { 10, 20, 30, 999 }, values);
    }

    [Fact]
    public async Task Append_on_empty_source_yields_only_the_element()
    {
        var (cn, ctx) = CreateContext(0);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "only", Value = 1 };
        var rows = await ctx.Query<SeqTailItem>().Append(extra).ToListAsync();

        Assert.Single(rows);
        Assert.Same(extra, rows[0]);
    }

    [Fact]
    public async Task Append_then_prepend_compose_in_order()
    {
        var (cn, ctx) = CreateContext(2);
        using var _cn = cn;
        using var _ctx = ctx;

        var first = new SeqTailItem { Name = "first", Value = 1 };
        var last = new SeqTailItem { Name = "last", Value = 2 };
        var rows = await ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Append(last)
            .Prepend(first)
            .ToListAsync();

        Assert.Equal(new[] { 1, 10, 20, 2 }, rows.Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Append_with_different_captured_elements_does_not_reuse_a_stale_plan()
    {
        var (cn, ctx) = CreateContext(1);
        using var _cn = cn;
        using var _ctx = ctx;

        var firstExtra = new SeqTailItem { Name = "a", Value = 111 };
        var rowsA = await ctx.Query<SeqTailItem>().Append(firstExtra).ToListAsync();

        var secondExtra = new SeqTailItem { Name = "b", Value = 222 };
        var rowsB = await ctx.Query<SeqTailItem>().Append(secondExtra).ToListAsync();

        Assert.Equal(111, rowsA[^1].Value);
        Assert.Equal(222, rowsB[^1].Value);
    }

    // ── Zip ──────────────────────────────────────────────────────────────────

    [Fact]
    public void Zip_with_local_collection_pairs_rows_in_order()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var labels = new[] { "a", "b", "c" };
        var pairs = ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Zip(labels)
            .ToList();

        Assert.Equal(3, pairs.Count);
        Assert.Equal(10, pairs[0].First.Value);
        Assert.Equal("a", pairs[0].Second);
        Assert.Equal(30, pairs[2].First.Value);
        Assert.Equal("c", pairs[2].Second);
    }

    [Fact]
    public void Zip_truncates_to_the_shorter_sequence_on_both_sides()
    {
        var (cn, ctx) = CreateContext(4);
        using var _cn = cn;
        using var _ctx = ctx;

        var shortLabels = new[] { "a", "b" };
        var shortSide = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Zip(shortLabels).ToList();
        Assert.Equal(2, shortSide.Count);

        var longLabels = new[] { "a", "b", "c", "d", "e", "f" };
        var longSide = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Zip(longLabels).ToList();
        Assert.Equal(4, longSide.Count);
        Assert.Equal("d", longSide[3].Second);
    }

    [Fact]
    public void Zip_with_result_selector_projects_pairs()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var bonuses = new[] { 1, 2, 3 };
        var totals = ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Zip(bonuses, (item, bonus) => item.Value + bonus)
            .ToList();

        Assert.Equal(new[] { 11, 22, 33 }, totals);
    }

    [Fact]
    public void Zip_of_two_ordered_database_queries_pairs_positionally()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var second = ctx.Query<SeqTailItem>().OrderByDescending(x => x.Id);
        var pairs = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Zip(second).ToList();

        Assert.Equal(3, pairs.Count);
        Assert.Equal(new[] { (10, 30), (20, 20), (30, 10) },
            pairs.Select(p => (p.First.Value, p.Second.Value)).ToArray());
    }

    [Fact]
    public void Zip_of_two_ordered_database_queries_with_selector_truncates_to_the_shorter_side()
    {
        var (cn, ctx) = CreateContext(4); // values 10, 20, 30, 40
        using var _cn = cn;
        using var _ctx = ctx;

        var shorter = ctx.Query<SeqTailItem>().Where(x => x.Value > 20).OrderBy(x => x.Id);
        var sums = ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Zip(shorter, (a, b) => a.Value + b.Value)
            .ToList();

        Assert.Equal(new[] { 10 + 30, 20 + 40 }, sums.ToArray());
    }

    [Fact]
    public void Zip_of_two_database_queries_requires_explicit_ordering_on_both_sides()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var ordered = ctx.Query<SeqTailItem>().OrderBy(x => x.Id);
        var unordered = ctx.Query<SeqTailItem>();

        Assert.Throws<NormUnsupportedFeatureException>(
            () => unordered.Zip(ordered).ToList());
        Assert.Throws<NormUnsupportedFeatureException>(
            () => ordered.Zip(unordered).ToList());
    }

    // ── Terminal operators after a client-tail reshape must not lie ─────────

    [Fact]
    public async Task Count_after_append_reflects_the_appended_element()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        Assert.Equal(4, await ctx.Query<SeqTailItem>().Append(extra).CountAsync());
    }

    [Fact]
    public async Task Count_after_chunk_reflects_chunk_cardinality()
    {
        var (cn, ctx) = CreateContext(5);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(3, await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(2).CountAsync());
    }

    [Fact]
    public async Task Count_with_predicate_after_append_filters_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var count = await ctx.Query<SeqTailItem>().Append(extra).CountAsync(x => x.Value > 15);
        Assert.Equal(3, count); // rows 20, 30 plus the appended 999

        var sync = ctx.Query<SeqTailItem>().Append(extra).Count(x => x.Value > 15);
        Assert.Equal(3, sync);
    }

    [Fact]
    public async Task LongCount_after_prepend_counts_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(2);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 1 };
        Assert.Equal(3L, await ctx.Query<SeqTailItem>().Prepend(extra).LongCountAsync());
    }

    [Fact]
    public async Task Count_after_default_if_empty_with_value_counts_the_default_row()
    {
        var (cn, ctx) = CreateContext(0);
        using var _cn = cn;
        using var _ctx = ctx;

        var fallback = new SeqTailItem { Name = "fallback", Value = 0 };
        Assert.Equal(1, await ctx.Query<SeqTailItem>().DefaultIfEmpty(fallback).CountAsync());
    }

    [Fact]
    public async Task First_after_prepend_returns_the_prepended_element()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var first = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra).FirstAsync();
        Assert.Equal(999, first.Value);
    }

    // ── Positional terminals after a client-tail reshape evaluate in memory ──

    [Fact]
    public async Task First_after_chunk_returns_the_complete_first_chunk()
    {
        // A server LIMIT 1 shortcut would truncate the rows BEFORE chunking,
        // yielding a one-element chunk instead of the full first chunk.
        var (cn, ctx) = CreateContext(5); // values 10..50
        using var _cn = cn;
        using var _ctx = ctx;

        var first = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(2).FirstAsync();

        Assert.Equal(new[] { 10, 20 }, first.Select(x => x.Value).ToArray());
    }

    [Fact]
    public void Last_with_predicate_after_append_can_return_the_appended_element()
    {
        // The reversed-ORDER-BY LIMIT 1 shortcut picks the last SERVER row matching
        // the predicate; the appended element comes after every server row.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 5 };
        var last = ctx.Query<SeqTailItem>().Append(extra).Last(x => x.Value < 25);

        Assert.Equal(5, last.Value);
    }

    [Fact]
    public async Task Last_after_append_returns_the_appended_element()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var last = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).LastAsync();

        Assert.Equal(999, last.Value);
    }

    [Fact]
    public async Task Element_at_after_prepend_accounts_for_the_shifted_index()
    {
        // Prepend shifts every index by one; a server OFFSET would be off by one.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var query = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra);

        Assert.Equal(999, (await query.ElementAtAsync(0)).Value);
        Assert.Equal(10, (await query.ElementAtAsync(1)).Value);
        Assert.Equal(30, (await query.ElementAtAsync(3)).Value);
    }

    [Fact]
    public void Min_by_and_max_by_after_reshape_consider_the_reshaped_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var high = new SeqTailItem { Name = "high", Value = 999 };
        Assert.Equal(999, ctx.Query<SeqTailItem>().Append(high).MaxBy(x => x.Value)!.Value);

        var low = new SeqTailItem { Name = "low", Value = -5 };
        Assert.Equal(-5, ctx.Query<SeqTailItem>().Prepend(low).MinBy(x => x.Value)!.Value);
    }

    [Fact]
    public async Task Single_after_append_applies_linq_cardinality_rules_to_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(1);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };

        // One server row plus the appended element is two elements.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<SeqTailItem>().Append(extra).SingleAsync());

        // An empty source plus the appended element is exactly one.
        var (cn2, ctx2) = CreateContext(0);
        using var _cn2 = cn2;
        using var _ctx2 = ctx2;
        var single = await ctx2.Query<SeqTailItem>().Append(extra).SingleAsync();
        Assert.Equal(999, single.Value);
    }

    // ── Sequence operators after a client-tail reshape evaluate in memory ────

    [Fact]
    public async Task Take_and_skip_after_reshape_page_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };

        // A server OFFSET would drop a real row and keep the prepended element.
        var skipped = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra).Skip(1).ToListAsync();
        Assert.Equal(new[] { 10, 20, 30 }, skipped.Select(x => x.Value).ToArray());

        // A captured (non-literal) count must also page client-side.
        var count = 2;
        var taken = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).Take(count).ToListAsync();
        Assert.Equal(new[] { 10, 20 }, taken.Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Take_last_and_skip_last_after_append_count_from_the_reshaped_end()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };

        var lastTwo = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).TakeLast(2).ToListAsync();
        Assert.Equal(new[] { 30, 999 }, lastTwo.Select(x => x.Value).ToArray());

        var withoutLast = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).SkipLast(1).ToListAsync();
        Assert.Equal(new[] { 10, 20, 30 }, withoutLast.Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Take_while_and_skip_while_after_prepend_start_from_the_prepended_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 5 };
        var query = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra);

        var head = await query.TakeWhile(x => x.Value < 15).ToListAsync();
        Assert.Equal(new[] { 5, 10 }, head.Select(x => x.Value).ToArray());

        var tail = await query.SkipWhile(x => x.Value < 15).ToListAsync();
        Assert.Equal(new[] { 20, 30 }, tail.Select(x => x.Value).ToArray());
    }

    [Fact]
    public void Distinct_after_append_dedups_against_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var values = ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Select(x => x.Value)
            .Append(10)
            .Distinct()
            .ToList();

        Assert.Equal(3, values.Count);
        Assert.Equal(new[] { 10, 20, 30 }, values.OrderBy(v => v).ToArray());
    }

    [Fact]
    public async Task Reverse_after_prepend_moves_the_prepended_element_to_the_end()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var reversed = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra).Reverse().ToListAsync();

        Assert.Equal(new[] { 30, 20, 10, 999 }, reversed.Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Aggregates_after_take_after_reshape_compose_client_side()
    {
        // The windowed COUNT/SUM derived-table wraps must divert: the Take after the
        // reshape pages the reshaped rows client-side, so no LIMIT reaches the SQL.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var query = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).Take(2);

        Assert.Equal(2, await query.CountAsync());
        Assert.Equal(30, await query.SumAsync(x => x.Value));
    }

    // ── Scalar aggregates after a client-tail reshape evaluate in memory ─────

    [Fact]
    public async Task Sum_after_append_includes_the_appended_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var sum = await ctx.Query<SeqTailItem>().Append(extra).SumAsync(x => x.Value);

        Assert.Equal(10 + 20 + 30 + 999, sum);
    }

    [Fact]
    public async Task Min_and_max_after_prepend_see_the_prepended_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var low = new SeqTailItem { Name = "low", Value = -5 };
        Assert.Equal(-5, await ctx.Query<SeqTailItem>().Prepend(low).MinAsync(x => x.Value));

        var high = new SeqTailItem { Name = "high", Value = 999 };
        Assert.Equal(999, await ctx.Query<SeqTailItem>().Prepend(high).MaxAsync(x => x.Value));
    }

    [Fact]
    public async Task Average_after_append_matches_linq_to_objects()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 40 };
        var average = await ctx.Query<SeqTailItem>().Append(extra).AverageAsync(x => x.Value);

        Assert.Equal(25.0, average);
    }

    [Fact]
    public async Task Min_after_default_if_empty_returns_the_default_value_on_empty_source()
    {
        var (cn, ctx) = CreateContext(0);
        using var _cn = cn;
        using var _ctx = ctx;

        var fallback = new SeqTailItem { Name = "fallback", Value = 42 };
        var min = await ctx.Query<SeqTailItem>().DefaultIfEmpty(fallback).MinAsync(x => x.Value);

        Assert.Equal(42, min);
    }

    [Fact]
    public async Task Any_after_append_sees_only_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };

        // The appended element is the only one matching the predicate.
        Assert.True(await ctx.Query<SeqTailItem>().Append(extra).AnyAsync(x => x.Value > 100));
        Assert.False(await ctx.Query<SeqTailItem>().Append(extra).AnyAsync(x => x.Value > 1000));

        // An empty source with an appended element is non-empty.
        var (cn2, ctx2) = CreateContext(0);
        using var _cn2 = cn2;
        using var _ctx2 = ctx2;
        Assert.True(await ctx2.Query<SeqTailItem>().Append(extra).AnyAsync());
    }

    [Fact]
    public async Task All_after_prepend_is_falsified_by_the_prepended_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var outlier = new SeqTailItem { Name = "outlier", Value = -1 };
        Assert.False(await ctx.Query<SeqTailItem>().Prepend(outlier).AllAsync(x => x.Value > 0));
        Assert.True(await ctx.Query<SeqTailItem>().Prepend(outlier).AllAsync(x => x.Value > -10));
    }

    [Fact]
    public async Task Any_after_chunk_reflects_chunk_cardinality()
    {
        var (cn, ctx) = CreateContext(5);
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.True(await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(2).AnyAsync());

        var (cn2, ctx2) = CreateContext(0);
        using var _cn2 = cn2;
        using var _ctx2 = ctx2;
        Assert.False(await ctx2.Query<SeqTailItem>().OrderBy(x => x.Id).Chunk(2).AnyAsync());
    }

    [Fact]
    public async Task Where_after_append_filters_the_reshaped_sequence_client_side()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };

        // The appended element must pass through the same predicate as the rows.
        var kept = await ctx.Query<SeqTailItem>().Append(extra).Where(x => x.Value > 15).ToListAsync();
        Assert.Equal(new[] { 20, 30, 999 }, kept.Select(x => x.Value).ToArray());

        var dropped = await ctx.Query<SeqTailItem>().Append(extra).Where(x => x.Value < 100).ToListAsync();
        Assert.Equal(new[] { 10, 20, 30 }, dropped.Select(x => x.Value).ToArray());
    }

    [Fact]
    public async Task Default_if_empty_after_append_sees_the_already_reshaped_sequence()
    {
        // Reshape transforms compose in operator order: Append runs first, so the
        // sequence DefaultIfEmpty inspects is non-empty and no default is inserted.
        var (cn, ctx) = CreateContext(0);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var rows = await ctx.Query<SeqTailItem>().Append(extra).DefaultIfEmpty().ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(999, row!.Value);
    }

    [Fact]
    public async Task As_async_enumerable_after_append_streams_the_reshaped_sequence()
    {
        // Streaming must not silently yield the pre-reshape server rows.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var seen = new List<int>();
        await foreach (var item in ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra).AsAsyncEnumerable())
            seen.Add(item.Value);

        Assert.Equal(new[] { 10, 20, 30, 999 }, seen.ToArray());
    }

    [Fact]
    public async Task As_async_enumerable_after_take_last_preserves_original_order()
    {
        // TakeLast plans flip the server ORDER BY and mark the plan for a final
        // in-memory reverse; streaming must apply that reverse, not yield the
        // reversed server order.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var seen = new List<int>();
        await foreach (var item in ctx.Query<SeqTailItem>().OrderBy(x => x.Id).TakeLast(2).AsAsyncEnumerable())
            seen.Add(item.Value);

        Assert.Equal(new[] { 20, 30 }, seen.ToArray());
    }

    [Fact]
    public async Task Container_terminals_after_append_include_the_reshaped_element()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var query = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Append(extra);

        var array = await query.ToArrayAsync();
        Assert.Equal(new[] { 10, 20, 30, 999 }, array.Select(x => x.Value).ToArray());

        var set = await query.ToHashSetAsync();
        Assert.Equal(4, set.Count);
        Assert.Contains(set, x => x.Value == 999);

        var dict = await query.ToDictionaryAsync(x => x.Value);
        Assert.Equal(4, dict.Count);
        Assert.Equal("extra", dict[999].Name);
    }

    [Fact]
    public async Task Group_by_after_append_groups_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 10 };
        var groups = await ctx.Query<SeqTailItem>()
            .OrderBy(x => x.Id)
            .Append(extra)
            .GroupBy(x => x.Value)
            .ToListAsync();

        // The appended duplicate value must land in the same group as the row.
        Assert.Equal(3, groups.Count);
        Assert.Equal(2, groups.Single(g => g.Key == 10).Count());
    }

    private static readonly Func<DbContext, int, Task<List<SeqTailItem>>> _compiledListAfterAppend =
        Norm.CompileQuery((DbContext c, int minValue) =>
            c.Query<SeqTailItem>()
                .Where(x => x.Value >= minValue)
                .OrderBy(x => x.Id)
                .Append(new SeqTailItem { Name = "baked", Value = 999 }));

    [Fact]
    public async Task Compiled_query_with_reshape_tail_applies_the_transform()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var rows = await _compiledListAfterAppend(ctx, 15);
        Assert.Equal(new[] { 20, 30, 999 }, rows.Select(x => x.Value).ToArray());
    }

    [Fact]
    public void Set_operations_after_reshape_do_not_silently_drop_the_reshaped_element()
    {
        // A SQL set-op over the server rows cannot see the appended element; the
        // composition must either include it correctly or fail closed — silently
        // losing it is never acceptable.
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        var second = ctx.Query<SeqTailItem>().Where(x => x.Value > 20);

        try
        {
            var combined = ctx.Query<SeqTailItem>().Append(extra).Concat(second).ToList();
            Assert.Contains(combined, x => x.Value == 999);
            Assert.Equal(5, combined.Count); // 3 rows + appended + 1 concat row
        }
        catch (NormUnsupportedFeatureException)
        {
            // Failing closed is acceptable; silently dropping the element is not.
        }
    }

    [Fact]
    public async Task Order_by_after_prepend_sorts_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 25 };
        var query = ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra);

        // The prepended element must participate in the ordering.
        var sorted = await query.OrderByDescending(x => x.Value).ToListAsync();
        Assert.Equal(new[] { 30, 25, 20, 10 }, sorted.Select(x => x.Value).ToArray());

        // Ordering then a positional terminal composes over the reshaped rows.
        var top = await query.OrderByDescending(x => x.Value).FirstAsync();
        Assert.Equal(30, top.Value);
    }

    [Fact]
    public async Task Cacheable_reshaped_queries_with_different_captured_elements_do_not_share_results()
    {
        // Result-cache keys derive from SQL + parameters; a reshape's captured
        // element reaches neither, so two Appends differing only in the element
        // would replay each other's cached list. Transform-carrying plans must
        // never be result-cached.
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE SeqTailItem (
                    Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name  TEXT    NOT NULL,
                    Value INTEGER NOT NULL
                );
                INSERT INTO SeqTailItem (Name, Value) VALUES ('item1', 10);";
            cmd.ExecuteNonQuery();
        }
        var opts = new nORM.Configuration.DbContextOptions().UseInMemoryCache();
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var first = new SeqTailItem { Name = "a", Value = 111 };
        var second = new SeqTailItem { Name = "b", Value = 222 };

        var withFirst = await ctx.Query<SeqTailItem>()
            .Append(first).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(new[] { 10, 111 }, withFirst.Select(x => x.Value).ToArray());

        var withSecond = await ctx.Query<SeqTailItem>()
            .Append(second).Cacheable(TimeSpan.FromMinutes(5)).ToListAsync();
        Assert.Equal(new[] { 10, 222 }, withSecond.Select(x => x.Value).ToArray());
    }

    [Fact]
    public void Sync_aggregates_after_append_use_the_reshaped_sequence()
    {
        var (cn, ctx) = CreateContext(3); // values 10, 20, 30
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 40 };
        var query = ctx.Query<SeqTailItem>().Append(extra);

        Assert.Equal(100, query.Sum(x => x.Value));
        Assert.Equal(40, query.Max(x => x.Value));
        Assert.Equal(25.0, query.Average(x => x.Value));
        Assert.True(query.Any(x => x.Value == 40));
        Assert.False(query.All(x => x.Value < 40));
    }
}
