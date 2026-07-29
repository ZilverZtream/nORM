using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Async streaming (AsAsyncEnumerable) and cancellation correctness. Every test asserts the streamed result
/// equals the same query's ToListAsync / LINQ-to-Objects oracle over the SAME seed, that the streaming path
/// honours the context tracking DEFAULT (not just an explicit AsNoTracking), and that partial / cancelled
/// enumeration leaves the context usable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsyncStreamingCorrectnessTests
{
    private readonly ITestOutputHelper _out;
    public AsyncStreamingCorrectnessTests(ITestOutputHelper o) => _out = o;

    // Order-preserving offset converter: model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => Convert.ToInt32(value) - 1000;
    }

    [Table("H58Row")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
        public int Score { get; set; }       // value-converter column (+1000)
        public decimal Amount { get; set; }
        public int Val { get; set; }
    }

    private static DbContext NewCtx(out SqliteConnection cn, int rows = 5,
        QueryTrackingBehavior tracking = QueryTrackingBehavior.TrackAll)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE H58Row (Id INTEGER PRIMARY KEY, Name TEXT NULL, Score INTEGER NOT NULL, Amount TEXT NOT NULL, Val INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            var sb = new System.Text.StringBuilder("INSERT INTO H58Row (Id, Name, Score, Amount, Val) VALUES ");
            for (int i = 1; i <= rows; i++)
            {
                if (i > 1) sb.Append(',');
                // Name null on every 3rd row; Score stored as model(i)+1000.
                var name = (i % 3 == 0) ? "NULL" : $"'name{i}'";
                sb.Append($"({i}, {name}, {i + 1000}, '{i}.25', {i * 10})");
            }
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            DefaultTrackingBehavior = tracking,
            OnModelCreating = mb =>
            {
                mb.Entity<Row>().HasKey(r => r.Id);
                mb.Entity<Row>().Property<int>(r => r.Score).HasConversion(new OffsetConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task<List<T>> Drain<T>(IAsyncEnumerable<T> src)
    {
        var list = new List<T>();
        await foreach (var x in src) list.Add(x);
        return list;
    }

    // ── Surface 1: full drain parity (order/projection/filter/Include-free) ──────

    [Fact]
    public async Task S1_FullDrain_OrderBy_matches_ToListAsync()
    {
        await using var ctx = NewCtx(out var cn); using var _cn = cn;
        var oracle = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        var streamed = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        Assert.Equal(oracle.Select(r => r.Id), streamed.Select(r => r.Id));
        Assert.Equal(oracle.Select(r => r.Name), streamed.Select(r => r.Name));
        Assert.Equal(oracle.Select(r => r.Score), streamed.Select(r => r.Score));
        Assert.Equal(oracle.Select(r => r.Amount), streamed.Select(r => r.Amount));
    }

    [Fact]
    public async Task S1_FullDrain_Filter_and_Projection_matches_ToListAsync()
    {
        await using var ctx = NewCtx(out var cn); using var _cn = cn;
        var oracle = await ctx.Query<Row>().Where(r => r.Val > 20).OrderByDescending(r => r.Id)
            .Select(r => new { r.Id, r.Name }).ToListAsync();
        var streamed = await Drain(ctx.Query<Row>().Where(r => r.Val > 20).OrderByDescending(r => r.Id)
            .Select(r => new { r.Id, r.Name }).AsAsyncEnumerable());
        Assert.Equal(oracle.Select(x => x.Id), streamed.Select(x => x.Id));
        Assert.Equal(oracle.Select(x => x.Name), streamed.Select(x => x.Name));
    }

    // ── Surface 2: partial enumeration then break → follow-up query works ────────

    [Fact]
    public async Task S2_PartialBreak_then_followup_query_correct()
    {
        await using var ctx = NewCtx(out var cn, rows: 8); using var _cn = cn;
        int n = 0;
        var seen = new List<int>();
        await foreach (var r in ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable())
        {
            seen.Add(r.Id);
            if (++n == 3) break;
        }
        Assert.Equal(new[] { 1, 2, 3 }, seen);

        // Follow-up on the SAME context must return the full correct set.
        var full = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(Enumerable.Range(1, 8), full.Select(r => r.Id));

        // And a second stream must fully drain again.
        var again = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        Assert.Equal(Enumerable.Range(1, 8), again.Select(r => r.Id));
    }

    // ── Surface 3: cancellation mid-stream → clean throw + context usable ────────

    [Fact]
    public async Task S3_CancelMidStream_throws_and_context_usable()
    {
        await using var ctx = NewCtx(out var cn, rows: 20); using var _cn = cn;
        using var cts = new CancellationTokenSource();
        int n = 0;
        var ex = await Record.ExceptionAsync(async () =>
        {
            await foreach (var r in ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable().WithCancellation(cts.Token))
            {
                if (++n == 5) cts.Cancel();
            }
        });
        Assert.True(ex is OperationCanceledException, $"expected OCE, got {ex?.GetType().Name}");

        // Context must remain usable after mid-stream cancellation.
        var full = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(20, full.Count);
        Assert.Equal(Enumerable.Range(1, 20), full.Select(r => r.Id));
    }

    // ── Surface 4: re-enumeration yields full set again ─────────────────────────

    [Fact]
    public async Task S4_Reenumeration_yields_full_set_each_time()
    {
        await using var ctx = NewCtx(out var cn, rows: 6); using var _cn = cn;
        var q = ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable();
        var first = await Drain(q);
        var second = await Drain(q);
        Assert.Equal(Enumerable.Range(1, 6), first.Select(r => r.Id));
        Assert.Equal(Enumerable.Range(1, 6), second.Select(r => r.Id));
    }

    // ── Surface 5: value-converter column parity on the async path ──────────────

    [Fact]
    public async Task S5_Converter_column_values_match_between_stream_and_ToList()
    {
        await using var ctx = NewCtx(out var cn, rows: 5); using var _cn = cn;
        var oracle = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        var streamed = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        // Model Score must be i (converter subtracts 1000), NOT the stored i+1000.
        Assert.Equal(Enumerable.Range(1, 5), streamed.Select(r => r.Score));
        Assert.Equal(oracle.Select(r => r.Score), streamed.Select(r => r.Score));
        Assert.Equal(oracle.Select(r => r.Amount), streamed.Select(r => r.Amount));
    }

    [Fact]
    public async Task S5_Converter_scalar_projection_parity()
    {
        await using var ctx = NewCtx(out var cn, rows: 5); using var _cn = cn;
        // Public AsAsyncEnumerable requires T : class, so wrap the converter scalar in an anon type.
        var oracle = await ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Score }).ToListAsync();
        var streamed = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).Select(r => new { r.Score }).AsAsyncEnumerable());
        Assert.Equal(oracle.Select(x => x.Score), streamed.Select(x => x.Score));
        Assert.Equal(Enumerable.Range(1, 5), streamed.Select(x => x.Score));
    }

    // ── Surface 6: large-ish result, exact count, no boundary drop/dup ──────────

    [Fact]
    public async Task S6_LargeResult_exact_count_and_no_dupes()
    {
        await using var ctx = NewCtx(out var cn, rows: 500); using var _cn = cn;
        var streamed = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        Assert.Equal(500, streamed.Count);
        Assert.Equal(Enumerable.Range(1, 500), streamed.Select(r => r.Id));
        Assert.Equal(500, streamed.Select(r => r.Id).Distinct().Count());
    }

    // ── Surface 7: nested/concurrent enumeration on same context ────────────────

    [Fact]
    public async Task S7_ConcurrentEnumeration_no_corruption_after()
    {
        await using var ctx = NewCtx(out var cn, rows: 10); using var _cn = cn;

        // Start enumerator A, pump partially (holds the SQLite serialization gate).
        var eA = ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable().GetAsyncEnumerator();
        Assert.True(await eA.MoveNextAsync());
        Assert.True(await eA.MoveNextAsync());
        Assert.Equal(2, eA.Current.Id);

        // Start enumerator B while A is still mid-stream. On SQLite the gate serializes, so B's
        // first MoveNext blocks until A releases. Kick it off, THEN dispose A (releasing the gate)
        // so B can proceed — this avoids a deadlock while still exercising the overlap.
        var eB = ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable().GetAsyncEnumerator();
        var bMoved = eB.MoveNextAsync().AsTask();

        // Give B a moment to reach the gate wait, then release A.
        var raced = await Task.WhenAny(bMoved, Task.Delay(300));
        _out.WriteLine(raced == bMoved ? "B advanced before A released (multi-reader)" : "B waiting on gate");

        await eA.DisposeAsync(); // release the gate held by A

        // B must now fully and correctly drain (no rows lost/duplicated from the overlap).
        var bRows = new List<int>();
        try
        {
            if (await bMoved) bRows.Add(eB.Current.Id);
            while (await eB.MoveNextAsync()) bRows.Add(eB.Current.Id);
        }
        finally
        {
            await eB.DisposeAsync();
        }
        Assert.Equal(Enumerable.Range(1, 10), bRows);

        // And a follow-up query is still correct.
        var full = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(Enumerable.Range(1, 10), full.Select(r => r.Id));
    }

    // ── Surface 8: empty / single-row / null-heavy last row ─────────────────────

    [Fact]
    public async Task S8_Empty_single_and_nullheavy_streams()
    {
        await using var ctx = NewCtx(out var cn, rows: 3); using var _cn = cn;

        var empty = await Drain(ctx.Query<Row>().Where(r => r.Id > 1000).AsAsyncEnumerable());
        Assert.Empty(empty);

        var single = await Drain(ctx.Query<Row>().Where(r => r.Id == 1).AsAsyncEnumerable());
        Assert.Single(single);
        Assert.Equal("name1", single[0].Name);

        // Row 3 has Name = NULL. Make it the last row.
        var all = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        var oracle = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(oracle.Select(r => r.Name), all.Select(r => r.Name));
        Assert.Null(all.Last().Name);
    }

    // ── Surface 9: rejected shapes still fail loud (not silent truncation) ──────

    [Fact]
    public async Task S9_CollectionProjection_still_fails_loud()
    {
        await using var ctx = NewCtx(out var cn, rows: 3); using var _cn = cn;
        var yielded = new List<int>();
        var ex = await Record.ExceptionAsync(async () =>
        {
            await foreach (var row in ctx.Query<Row>()
                .Select(r => new { r.Id, Vals = ctx.Query<Row>().Where(x => x.Id == r.Id).Select(x => x.Val).ToList() })
                .AsAsyncEnumerable())
            {
                yielded.Add(row.Vals.Count);
            }
        });
        _out.WriteLine($"S9 ex: {ex?.GetType().Name}: {ex?.Message}");
        // Must FAIL LOUD (not silently stream roots with empty child collections).
        Assert.NotNull(ex);
        // And it must not have silently yielded any rows with EMPTY collections before throwing.
        Assert.DoesNotContain(0, yielded);
    }

    // ── Surface 10: aggregate/terminal-async parity with sync ───────────────────

    [Fact]
    public async Task S10_Terminal_async_parity()
    {
        await using var ctx = NewCtx(out var cn, rows: 7); using var _cn = cn;

        Assert.Equal(7, await ctx.Query<Row>().CountAsync());
        Assert.Equal(1, (await ctx.Query<Row>().OrderBy(r => r.Id).FirstAsync()).Id);
        Assert.Equal(7, (await ctx.Query<Row>().OrderBy(r => r.Id).LastAsync()).Id);
        Assert.Equal(4, (await ctx.Query<Row>().Where(r => r.Id == 4).SingleAsync()).Id);

        var list = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(Enumerable.Range(1, 7), list.Select(r => r.Id));
    }

    // ── Focus: DefaultTrackingBehavior parity between stream and ToList ──────────
    // Buffered path gates tracking on IsReadOnlyQuery() (context default). Streaming path
    // only checks plan.NoTracking (explicit AsNoTracking). Hypothesis: with a NoTracking
    // context default, ToListAsync leaves entities UNTRACKED but AsAsyncEnumerable TRACKS them.

    [Fact]
    public async Task Focus_NoTrackingDefault_stream_matches_ToList_tracking()
    {
        await using var ctx = NewCtx(out var cn, rows: 4, tracking: QueryTrackingBehavior.NoTracking);
        using var _cn = cn;

        // Buffered oracle: with NoTracking default, ToListAsync must NOT track.
        var listed = await ctx.Query<Row>().OrderBy(r => r.Id).ToListAsync();
        var trackedAfterList = ctx.ChangeTracker.Entries.Count(e => e.Entity is Row);
        _out.WriteLine($"tracked after ToListAsync (NoTracking default): {trackedAfterList}");
        Assert.Equal(0, trackedAfterList);

        // Streaming path under the SAME NoTracking default must ALSO leave entities untracked.
        var streamed = await Drain(ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable());
        var trackedAfterStream = ctx.ChangeTracker.Entries.Count(e => e.Entity is Row);
        _out.WriteLine($"tracked after AsAsyncEnumerable (NoTracking default): {trackedAfterStream}");

        Assert.Equal(4, streamed.Count);
        // The divergence assertion: streaming must match the buffered NoTracking contract.
        Assert.Equal(0, trackedAfterStream);
    }

    [Fact]
    public async Task Focus_NoTrackingDefault_streamed_edit_does_not_persist_on_SaveChanges()
    {
        // Under a NoTracking default, a streamed entity edited then SaveChanges'd must NOT persist
        // (parity with ToList, which returns untracked entities). If streaming silently tracks, the
        // edit would be written — an unintended data mutation.
        await using var ctx = NewCtx(out var cn, rows: 3, tracking: QueryTrackingBehavior.NoTracking);
        using var _cn = cn;

        Row? streamed = null;
        await foreach (var r in ctx.Query<Row>().OrderBy(r => r.Id).AsAsyncEnumerable())
            if (r.Id == 2) streamed = r;
        Assert.NotNull(streamed);

        streamed!.Val = 99999;
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Val FROM H58Row WHERE Id = 2";
        var persisted = Convert.ToInt32(check.ExecuteScalar());
        _out.WriteLine($"persisted Val after streamed edit under NoTracking default: {persisted} (seed was 20)");
        // NoTracking contract: the edit must NOT have persisted.
        Assert.Equal(20, persisted);
    }
}
