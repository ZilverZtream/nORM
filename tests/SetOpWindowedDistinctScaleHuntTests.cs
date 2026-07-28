using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: DISTINCT applied AFTER a Take/Skip window over a decimal or TEXT-stored temporal
/// column. The flat DISTINCT path canonicalizes decimal/DateTime/TimeOnly/TimeSpan for by-value dedup,
/// but the windowed-distinct branch (DistinctTranslator.TranslateAfterTakeSkipWindow) only canonicalizes
/// string (CI providers) and DateTimeOffset — NeedsDistinctKeyTreatment omits decimal/DateTime/TimeOnly/
/// TimeSpan. So `Take(N).Select(x => x.Decimal).Distinct()` dedups by raw TEXT: '10.5' and '10.50' survive
/// as two rows though .NET decimal.Distinct treats them equal (one). Diffed vs a LINQ-to-Objects oracle.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpWindowedDistinctScaleHuntTests
{
    [Table("SwdRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public decimal DVal { get; set; }
        public DateTime Dt { get; set; }
        public TimeSpan Ts { get; set; }
        [Column("T_o")] public TimeOnly To { get; set; }
        public DateTimeOffset Dto { get; set; }
    }

    // Rows 1 & 2 are the SAME value in a different TEXT scale/offset (external raw writes);
    // rows 3 & 4 are genuinely distinct. All written via raw INSERT to force provider-scale variance.
    private const string SeedSql =
        "CREATE TABLE SwdRow (Id INTEGER PRIMARY KEY, DVal TEXT NOT NULL, Dt TEXT NOT NULL, " +
        "Ts TEXT NOT NULL, T_o TEXT NOT NULL, Dto TEXT NOT NULL);" +
        "INSERT INTO SwdRow (Id, DVal, Dt, Ts, T_o, Dto) VALUES " +
        "(1, '10.5',  '2026-05-25 12:00:00',         '1.00:00:00',         '12:00:00',         '2026-05-25 12:00:00+00:00')," +
        "(2, '10.50', '2026-05-25 12:00:00.0000000', '1.00:00:00.0000000', '12:00:00.0000000', '2026-05-25 14:00:00+02:00')," +
        "(3, '20',    '2026-05-25 13:00:00',         '2.00:00:00',         '13:00:00',         '2026-05-25 13:00:00+00:00')," +
        "(4, '20.0',  '2026-05-25 13:00:00.0000000', '2.00:00:00.0000000', '13:00:00.0000000', '2026-05-25 15:00:00+02:00');";

    private static readonly Row[] Reference =
    {
        new() { Id = 1, DVal = 10.5m,  Dt = new DateTime(2026,5,25,12,0,0), Ts = TimeSpan.FromDays(1), To = new TimeOnly(12,0,0), Dto = new DateTimeOffset(2026,5,25,12,0,0,TimeSpan.Zero) },
        new() { Id = 2, DVal = 10.50m, Dt = new DateTime(2026,5,25,12,0,0), Ts = TimeSpan.FromDays(1), To = new TimeOnly(12,0,0), Dto = new DateTimeOffset(2026,5,25,14,0,0,TimeSpan.FromHours(2)) },
        new() { Id = 3, DVal = 20m,    Dt = new DateTime(2026,5,25,13,0,0), Ts = TimeSpan.FromDays(2), To = new TimeOnly(13,0,0), Dto = new DateTimeOffset(2026,5,25,13,0,0,TimeSpan.Zero) },
        new() { Id = 4, DVal = 20.0m,  Dt = new DateTime(2026,5,25,13,0,0), Ts = TimeSpan.FromDays(2), To = new TimeOnly(13,0,0), Dto = new DateTimeOffset(2026,5,25,15,0,0,TimeSpan.FromHours(2)) },
    };

    private sealed class SqlCapture : BaseDbCommandInterceptor
    {
        public ConcurrentQueue<string> Sql { get; } = new();
        public SqlCapture() : base(NullLogger.Instance) { }
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        { Sql.Enqueue(command.CommandText); return base.ReaderExecuting(command, context); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { Sql.Enqueue(command.CommandText); return base.ReaderExecutingAsync(command, context, ct); }
    }

    private static DbContext NewCtx(out SqlCapture capture)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand()) { c.CommandText = SeedSql; c.ExecuteNonQuery(); }
        capture = new SqlCapture();
        var opts = new DbContextOptions();
        opts.CommandInterceptors.Add(capture);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ── CONTROL: flat scalar decimal Distinct dedups by value (known-good path) ──────────────────
    [Fact]
    public void Flat_decimal_distinct_dedups_by_value_control()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Select(x => x.DVal).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Select(r => r.DVal).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);          // {10.5, 20}
        Assert.Equal(2, norm.Count);
    }

    // ── HUNT: windowed decimal Distinct loses by-value dedup ─────────────────────────────────────
    [Fact]
    public void Windowed_decimal_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out var cap);
        var norm = ctx.Query<Row>().Take(100).Select(x => x.DVal).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => r.DVal).Distinct().OrderBy(x => x).ToList();
        // Emitted SQL: SELECT DISTINCT * FROM (SELECT "DVal" FROM "SwdRow" LIMIT 100) AS "__wdis0"
        // The inner sub-plan projects the raw TEXT decimal with no ExactKeySql canonicalization, so the
        // outer DISTINCT dedups '10.5'/'10.50'/'20'/'20.0' lexically -> 4 rows instead of 2.
        _ = cap;
        Assert.Equal(oracle.Count, norm.Count); // oracle 2 ({10.5,20}); raw-text dedup gives 4 -> FAIL
        Assert.Equal(oracle, norm);
    }

    // ── HUNT: windowed DateTime Distinct loses by-value dedup ────────────────────────────────────
    [Fact]
    public void Windowed_datetime_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Take(100).Select(x => x.Dt).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => r.Dt).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count); // oracle 2 distinct instants; raw text gives 4
        Assert.Equal(oracle, norm);
    }

    // ── HUNT: windowed TimeSpan Distinct loses by-value dedup ────────────────────────────────────
    [Fact]
    public void Windowed_timespan_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Take(100).Select(x => x.Ts).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => r.Ts).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count);
        Assert.Equal(oracle, norm);
    }

    // ── HUNT: windowed TimeOnly Distinct loses by-value dedup ────────────────────────────────────
    [Fact]
    public void Windowed_timeonly_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Take(100).Select(x => x.To).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => r.To).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count);
        Assert.Equal(oracle, norm);
    }

    // ── HUNT breadth: Skip window has the same gap as Take ───────────────────────────────────────
    [Fact]
    public void Skip_windowed_decimal_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().OrderBy(x => x.Id).Skip(0).Select(x => x.DVal).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.OrderBy(r => r.Id).Skip(0).Select(r => r.DVal).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count);
        Assert.Equal(oracle, norm);
    }

    // ── HUNT breadth: anonymous-type member falls through NeedsDistinctKeyTreatment too ──────────
    [Fact]
    public void Windowed_anon_decimal_member_distinct_dedups_by_value()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Take(100).Select(x => new { x.DVal }).Distinct().ToList()
            .Select(a => a.DVal).OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => new { r.DVal }).Distinct()
            .Select(a => a.DVal).OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count);
        Assert.Equal(oracle, norm);
    }

    // ── CLEAN BILL: Concat(...).Distinct() over scale-different decimals dedups by value (PASS) ───
    // The set-op path sets _exactDecimalProjectionKeys, so each arm emits canonical decimal TEXT and the
    // outer SELECT DISTINCT dedups by value. Confirms the windowed gap is specific to the Take/Skip wrap.
    [Fact]
    public void Concat_then_distinct_decimal_dedups_by_value_cleanbill()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Where(x => x.Id <= 2).Select(x => x.DVal)
            .Concat(ctx.Query<Row>().Where(x => x.Id >= 2).Select(x => x.DVal))
            .Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Where(r => r.Id <= 2).Select(r => r.DVal)
            .Concat(Reference.Where(r => r.Id >= 2).Select(r => r.DVal))
            .Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle, norm);          // {10.5, 20}
    }

    // ── CONTRAST: windowed DateTimeOffset Distinct IS handled (should PASS) ───────────────────────
    [Fact]
    public void Windowed_datetimeoffset_distinct_dedups_by_instant_contrast()
    {
        using var ctx = NewCtx(out _);
        var norm = ctx.Query<Row>().Take(100).Select(x => x.Dto).Distinct().ToList().OrderBy(x => x).ToList();
        var oracle = Reference.Take(100).Select(r => r.Dto).Distinct().OrderBy(x => x).ToList();
        Assert.Equal(oracle.Count, norm.Count); // 2 instants
    }
}
