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
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// T1 — 4-provider parameter round-trip matrix
//
// Audit finding: ParameterBindingParityTests and SourceGenRuntimeParityTests
// are SQLite-only. Non-SQLite providers could diverge undetected in parameter
// metadata, driver coercion, and source-generated execution.
//
// All four provider kinds run against a SQLite in-memory database using the
// FakeProvider pattern (provider SQL generator + SqliteParameterFactory).
// SQLite accepts backtick, double-quote, and bracket quoting, and @param
// prefixes, so the generated SQL is executable for all provider kinds.
//
// Explicit integer PKs (no AUTOINCREMENT) avoid provider-specific identity
// retrieval SQL (LAST_INSERT_ID, OUTPUT INSERTED) that SQLite cannot execute.
//
// Covers:
//   RTP-1  null column values — read back as null
//   RTP-2  enum values — stored as int, decoded by CLR
//   RTP-3  Guid values — stored as TEXT
//   RTP-4  DateOnly values — stored as TEXT
//   RTP-5  TimeOnly values — stored as TEXT
//   RTP-6  binary (byte[]) values — stored as BLOB
//   RTP-7  long string (>4 000 chars) — stored as TEXT
//   RTP-8  compiled query matches runtime LINQ across all providers
//   RTP-9  pre-cancelled token throws OCE on query path
//   RTP-10 pre-cancelled token throws OCE on save path
//   RTP-11 IS NULL / IS NOT NULL filter
//   RTP-12 enum WHERE filter
// ══════════════════════════════════════════════════════════════════════════════

public class ProviderParameterRoundTripMatrixTests
{
    // ── Entity ────────────────────────────────────────────────────────────────

    public enum RtpStatus { Draft = 0, Active = 1, Archived = 2 }

    [Table("RtpRow")]
    public class RtpRow
    {
        // Explicit key — no AUTOINCREMENT — so INSERT SQL is provider-agnostic.
        [Key]
        public int Id { get; set; }
        public string? NullableText { get; set; }
        public RtpStatus Status { get; set; }
        public Guid Uid { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly Moment { get; set; }
        public byte[]? Payload { get; set; }
        public string? LongText { get; set; }
    }

    // ── Provider factory ──────────────────────────────────────────────────────

    private const string Ddl =
        "CREATE TABLE RtpRow (" +
        "  Id INTEGER PRIMARY KEY," +
        "  NullableText TEXT," +
        "  Status INTEGER NOT NULL," +
        "  Uid TEXT NOT NULL," +
        "  Day TEXT NOT NULL," +
        "  Moment TEXT NOT NULL," +
        "  Payload BLOB," +
        "  LongText TEXT" +
        ")";

    private static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        "sqlserver" => new SqlServerProvider(),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static (SqliteConnection Cn, DbContext Ctx) CreateDb(string kind, DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = Ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(kind), opts ?? new DbContextOptions()));
    }

    private static RtpRow BaseRow(int id) => new RtpRow
    {
        Id     = id,
        Status = RtpStatus.Draft,
        Uid    = Guid.Empty,
        Day    = DateOnly.MinValue,
        Moment = TimeOnly.MinValue
    };

    // ── RTP-1: null column values ─────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task NullColumnValues_InsertAndQuery_ReturnsNull(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var row = BaseRow(1);
        row.NullableText = null;
        row.Payload      = null;
        row.LongText     = null;
        await ctx.InsertAsync(row);

        var list = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(list);
        Assert.Null(list[0].NullableText);
        Assert.Null(list[0].Payload);
        Assert.Null(list[0].LongText);
    }

    // ── RTP-2: enum round-trip ────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task EnumValue_InsertAndQuery_PreservesValue(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var row = BaseRow(1);
        row.Status = RtpStatus.Archived;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(RtpStatus.Archived, result[0].Status);
    }

    // ── RTP-3: Guid round-trip ────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task GuidValue_InsertAndQuery_PreservesValue(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var expected = Guid.NewGuid();
        var row = BaseRow(1);
        row.Uid = expected;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(expected, result[0].Uid);
    }

    // ── RTP-4: DateOnly round-trip ────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task DateOnlyValue_InsertAndQuery_PreservesValue(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var expected = new DateOnly(2025, 12, 31);
        var row = BaseRow(1);
        row.Day = expected;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(expected, result[0].Day);
    }

    // ── RTP-5: TimeOnly round-trip ────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task TimeOnlyValue_InsertAndQuery_PreservesValue(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var expected = new TimeOnly(14, 30, 59);
        var row = BaseRow(1);
        row.Moment = expected;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(expected, result[0].Moment);
    }

    // ── RTP-6: binary round-trip ──────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task BinaryPayload_InsertAndQuery_PreservesBytes(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var expected = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0xFF };
        var row = BaseRow(1);
        row.Payload = expected;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(expected, result[0].Payload);
    }

    // ── RTP-7: long string round-trip ─────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task LongString_InsertAndQuery_PreservesFullText(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var expected = new string('A', 4_000) + new string('Z', 4_000); // 8 000 chars
        var row = BaseRow(1);
        row.LongText = expected;
        await ctx.InsertAsync(row);

        var result = await ctx.Query<RtpRow>().ToListAsync();
        Assert.Single(result);
        Assert.Equal(expected, result[0].LongText);
    }

    // ── RTP-8: compiled query matches runtime LINQ ────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task CompiledQuery_MatchesRuntimeQuery_AllProviders(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
        {
            var row = BaseRow(i);
            row.Status = (RtpStatus)((i - 1) % 3);
            row.Uid    = Guid.NewGuid();
            row.Day    = new DateOnly(2024, 1, i);
            row.Moment = new TimeOnly(i - 1, 0, 0);
            await ctx.InsertAsync(row);
        }

        var compiled = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<RtpRow>().Where(x => x.Id >= minId).OrderBy(x => x.Id));

        var compiledResult = await compiled(ctx, 1);
        var runtimeResult  = await ctx.Query<RtpRow>().OrderBy(x => x.Id).ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id,    compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Status, compiledResult[i].Status);
            Assert.Equal(runtimeResult[i].Uid,    compiledResult[i].Uid);
            Assert.Equal(runtimeResult[i].Day,    compiledResult[i].Day);
            Assert.Equal(runtimeResult[i].Moment, compiledResult[i].Moment);
        }
    }

    // ── RTP-9: pre-cancelled token on query path ──────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PreCancelledToken_OnQuery_ThrowsOCE(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<RtpRow>().ToListAsync(cts.Token));
    }

    // ── RTP-10: pre-cancelled token on save path ──────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task PreCancelledToken_OnSaveChanges_ThrowsOCE(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(BaseRow(1));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.SaveChangesAsync(cts.Token));
    }

    // ── RTP-11: IS NULL / IS NOT NULL filter ──────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task NullFilter_IsNullAndIsNotNull_CorrectCounts(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        var r1 = BaseRow(1); r1.NullableText = null;
        var r2 = BaseRow(2); r2.NullableText = "present";
        await ctx.InsertAsync(r1);
        await ctx.InsertAsync(r2);

        var nullRows    = await ctx.Query<RtpRow>().Where(r => r.NullableText == null).ToListAsync();
        var nonNullRows = await ctx.Query<RtpRow>().Where(r => r.NullableText != null).ToListAsync();

        Assert.Single(nullRows);
        Assert.Single(nonNullRows);
    }

    // ── RTP-12: enum WHERE filter ─────────────────────────────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task EnumFilter_WhereStatus_ReturnsMatchingRowsOnly(string kind)
    {
        var (cn, ctx) = CreateDb(kind);
        await using var _ = ctx; using var __ = cn;

        for (int i = 0; i < 3; i++)
        {
            var row = BaseRow(i + 1);
            row.Status = (RtpStatus)i;
            await ctx.InsertAsync(row);
        }

        var active = await ctx.Query<RtpRow>()
            .Where(r => r.Status == RtpStatus.Active)
            .ToListAsync();

        Assert.Single(active);
        Assert.Equal(RtpStatus.Active, active[0].Status);
    }
}
