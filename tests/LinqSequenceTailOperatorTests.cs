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
    public void Zip_of_two_database_queries_throws_a_deterministic_unsupported_exception()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var second = ctx.Query<SeqTailItem>().OrderBy(x => x.Id);
        Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Zip(second).ToList());
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
    public async Task First_after_prepend_returns_the_prepended_element_or_fails_closed()
    {
        var (cn, ctx) = CreateContext(3);
        using var _cn = cn;
        using var _ctx = ctx;

        var extra = new SeqTailItem { Name = "extra", Value = 999 };
        try
        {
            var first = await ctx.Query<SeqTailItem>().OrderBy(x => x.Id).Prepend(extra).FirstAsync();
            Assert.Equal(999, first.Value);
        }
        catch (NormUnsupportedFeatureException)
        {
            // Failing closed is acceptable; silently returning the first row is not.
        }
    }
}
