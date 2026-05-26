using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for SCV additions that previously only had
/// SQLite probes. Each test runs against every configured provider
/// (skips gracefully when a provider's env var is missing). The point
/// is to confirm the per-provider hooks actually behave correctly on
/// real engines — provider-shape equivalence is not the same as
/// runtime parity.
///
/// Tracked in docs/live-provider-linq-parity.md — flips 🚧 rows to ✅.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderRecentScvParityTests
{
    // ------------------------------------------------------------------ helpers

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string DtoCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "DATETIMEOFFSET",
        ProviderKind.Postgres  => "TIMESTAMPTZ",
        ProviderKind.MySql     => "DATETIME(6)",
        ProviderKind.Sqlite    => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string IdCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "INT",
        ProviderKind.Postgres  => "INT",
        ProviderKind.MySql     => "INT",
        ProviderKind.Sqlite    => "INTEGER",
        _ => throw new NotSupportedException()
    };

    private static string VarCharCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        ProviderKind.Postgres  => $"VARCHAR({len})",
        ProviderKind.MySql     => $"VARCHAR({len})",
        ProviderKind.Sqlite    => "TEXT",
        _ => throw new NotSupportedException()
    };

    private static string TimeSpanCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "TIME(7)",
        ProviderKind.Postgres  => "INTERVAL",
        ProviderKind.MySql     => "TIME(6)",
        ProviderKind.Sqlite    => "TEXT",
        _ => throw new NotSupportedException()
    };

    // Drop-if-exists prefix used in DDL per provider.
    private static string DropTable(ProviderKind kind, string rawName, string escaped) => kind switch
    {
        ProviderKind.SqlServer => $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escaped};",
        _ => $"DROP TABLE IF EXISTS {escaped};"
    };

    // Escape a column name consistently with the provider.
    private static string EscapeCol(ProviderKind kind, string name) => kind switch
    {
        ProviderKind.MySql    => $"`{name}`",
        ProviderKind.Postgres => $"\"{name}\"",
        _ => $"[{name}]"  // SqlServer / SQLite
    };

    private static async Task Teardown(DbContext ctx, string rawName)
    {
        try
        {
            var esc = ctx.Provider.Escape(rawName);
            var kind = ctx.Provider switch
            {
                MySqlProvider    => ProviderKind.MySql,
                PostgresProvider => ProviderKind.Postgres,
                SqlServerProvider=> ProviderKind.SqlServer,
                _                => ProviderKind.Sqlite
            };
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {esc}"
                : $"DROP TABLE IF EXISTS {esc}";
            await ExecuteAsync(ctx, drop);
        }
        catch { /* best-effort */ }
    }

    // ------------------------------------------------------------------ test 1: DTO == DateTime literal

    private const string DtoTable = "DtoParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_column_equals_UTC_DateTime_literal_matches_by_UTC_instant_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupDtoTableAsync(ctx, kind);
            try
            {
                var literal = new DateTime(2026, 5, 25, 12, 30, 45, DateTimeKind.Utc);
                var rows = await ctx.Query<DtoParityRow>()
                    .Where(r => r.Dto == literal)
                    .OrderBy(r => r.Id)
                    .ToListAsync();
                // Rows 1+2 share the same UTC instant stored in different offsets; row 3 is 1s later.
                Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());

                var notRows = await ctx.Query<DtoParityRow>()
                    .Where(r => r.Dto != literal)
                    .OrderBy(r => r.Id)
                    .ToListAsync();
                Assert.Equal(new[] { 3 }, notRows.Select(r => r.Id).ToArray());
            }
            finally { await Teardown(ctx, DtoTable); }
        }
    }

    private static async Task SetupDtoTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t = ctx.Provider.Escape(DtoTable);
        var id = EscapeCol(kind, "Id");
        var dto = EscapeCol(kind, "Dto");
        var ddl = DropTable(kind, DtoTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {dto} {DtoCol(kind)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        string insert = kind switch
        {
            ProviderKind.SqlServer =>
                $"INSERT INTO {t} ({id},{dto}) VALUES " +
                "(1,CAST('2026-05-25 12:30:45 +00:00' AS DATETIMEOFFSET))," +
                "(2,CAST('2026-05-25 14:30:45 +02:00' AS DATETIMEOFFSET))," +
                "(3,CAST('2026-05-25 12:30:46 +00:00' AS DATETIMEOFFSET))",
            ProviderKind.Postgres =>
                $"INSERT INTO {t} ({id},{dto}) VALUES " +
                "(1,'2026-05-25 12:30:45+00'::timestamptz)," +
                "(2,'2026-05-25 14:30:45+02'::timestamptz)," +
                "(3,'2026-05-25 12:30:46+00'::timestamptz)",
            ProviderKind.MySql =>
                $"INSERT INTO {t} ({id},{dto}) VALUES " +
                "(1,'2026-05-25 12:30:45.000000')," +
                "(2,'2026-05-25 12:30:45.000000')," +
                "(3,'2026-05-25 12:30:46.000000')",
            ProviderKind.Sqlite =>
                $"INSERT INTO {t} ({id},{dto}) VALUES " +
                "(1,'2026-05-25 12:30:45+00:00')," +
                "(2,'2026-05-25 14:30:45+02:00')," +
                "(3,'2026-05-25 12:30:46+00:00')",
            _ => throw new NotSupportedException()
        };
        await ExecuteAsync(ctx, insert);
    }

    [Table(DtoTable)]
    public sealed class DtoParityRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }

    // ------------------------------------------------------------------ test 2: DTO - DTO → TimeSpan

    private const string DsubTable = "DsubParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_column_minus_DateTimeOffset_column_returns_UTC_instant_difference(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupDsubTableAsync(ctx, kind);
            try
            {
                var rows = (await ctx.Query<DsubParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Diff = r.A - r.B })
                    .ToListAsync())
                    .ToArray();

                Assert.Equal(3, rows.Length);
                Assert.Equal(TimeSpan.FromSeconds(15), rows[0].Diff);  // 12:30:45Z - 12:30:30Z
                Assert.Equal(TimeSpan.Zero,            rows[1].Diff);  // 12:00:00Z - 12:00:00Z (different stored offsets)
                Assert.Equal(TimeSpan.FromHours(-1),   rows[2].Diff);  // 12:00:00Z - 13:00:00Z
            }
            finally { await Teardown(ctx, DsubTable); }
        }
    }

    private static async Task SetupDsubTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t = ctx.Provider.Escape(DsubTable);
        var id = EscapeCol(kind, "Id");
        var a  = EscapeCol(kind, "A");
        var b  = EscapeCol(kind, "B");
        var ddl = DropTable(kind, DsubTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {a} {DtoCol(kind)} NOT NULL, {b} {DtoCol(kind)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        string insert = kind switch
        {
            ProviderKind.SqlServer =>
                $"INSERT INTO {t} ({id},{a},{b}) VALUES " +
                "(1,CAST('2026-05-25 12:30:45 +00:00' AS DATETIMEOFFSET),CAST('2026-05-25 12:30:30 +00:00' AS DATETIMEOFFSET))," +
                "(2,CAST('2026-05-25 14:00:00 +02:00' AS DATETIMEOFFSET),CAST('2026-05-25 11:00:00 -01:00' AS DATETIMEOFFSET))," +
                "(3,CAST('2026-05-25 12:00:00 +00:00' AS DATETIMEOFFSET),CAST('2026-05-25 13:00:00 +00:00' AS DATETIMEOFFSET))",
            ProviderKind.Postgres =>
                $"INSERT INTO {t} ({id},{a},{b}) VALUES " +
                "(1,'2026-05-25 12:30:45+00'::timestamptz,'2026-05-25 12:30:30+00'::timestamptz)," +
                "(2,'2026-05-25 14:00:00+02'::timestamptz,'2026-05-25 11:00:00-01'::timestamptz)," +
                "(3,'2026-05-25 12:00:00+00'::timestamptz,'2026-05-25 13:00:00+00'::timestamptz)",
            // MySQL stores as UTC DATETIME(6); both +02:00 and -01:00 rows become 12:00:00 UTC.
            ProviderKind.MySql =>
                $"INSERT INTO {t} ({id},{a},{b}) VALUES " +
                "(1,'2026-05-25 12:30:45.000000','2026-05-25 12:30:30.000000')," +
                "(2,'2026-05-25 12:00:00.000000','2026-05-25 12:00:00.000000')," +
                "(3,'2026-05-25 12:00:00.000000','2026-05-25 13:00:00.000000')",
            ProviderKind.Sqlite =>
                $"INSERT INTO {t} ({id},{a},{b}) VALUES " +
                "(1,'2026-05-25 12:30:45+00:00','2026-05-25 12:30:30+00:00')," +
                "(2,'2026-05-25 14:00:00+02:00','2026-05-25 11:00:00-01:00')," +
                "(3,'2026-05-25 12:00:00+00:00','2026-05-25 13:00:00+00:00')",
            _ => throw new NotSupportedException()
        };
        await ExecuteAsync(ctx, insert);
    }

    [Table(DsubTable)]
    public sealed class DsubParityRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset A { get; set; }
        public DateTimeOffset B { get; set; }
    }

    // ------------------------------------------------------------------ test 3: DTO +/- TimeSpan col

    private const string DtsTable = "DtsParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_column_plus_TimeSpan_column_shifts_instant_by_column_value(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupDtsTableAsync(ctx, kind);
            try
            {
                var rows = (await ctx.Query<DtsParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Shifted = r.Dto + r.Span })
                    .ToListAsync())
                    .ToArray();

                // Row 1: 12:00:00Z + 01:30:00 → 13:30:00Z
                Assert.Equal(new DateTime(2026, 5, 25, 13, 30,  0, DateTimeKind.Utc), rows[0].Shifted.UtcDateTime);
                // Row 2: 10:00:00Z (12:00+02:00 stored as UTC on MySQL) + 00:00:30 → 10:00:30Z
                Assert.Equal(new DateTime(2026, 5, 25, 10,  0, 30, DateTimeKind.Utc), rows[1].Shifted.UtcDateTime);
                // Row 3: 14:00:00Z + 00:45:00 → 14:45:00Z
                Assert.Equal(new DateTime(2026, 5, 25, 14, 45,  0, DateTimeKind.Utc), rows[2].Shifted.UtcDateTime);
            }
            finally { await Teardown(ctx, DtsTable); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_column_minus_TimeSpan_column_shifts_instant_backward(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupDtsTableAsync(ctx, kind);
            try
            {
                var rows = (await ctx.Query<DtsParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Shifted = r.Dto - r.Span })
                    .ToListAsync())
                    .ToArray();

                // Row 1: 12:00:00Z - 01:30:00 → 10:30:00Z
                Assert.Equal(new DateTime(2026, 5, 25, 10, 30,  0, DateTimeKind.Utc), rows[0].Shifted.UtcDateTime);
                // Row 2: 10:00:00Z - 00:00:30 → 09:59:30Z
                Assert.Equal(new DateTime(2026, 5, 25,  9, 59, 30, DateTimeKind.Utc), rows[1].Shifted.UtcDateTime);
                // Row 3: 14:00:00Z - 00:45:00 → 13:15:00Z
                Assert.Equal(new DateTime(2026, 5, 25, 13, 15,  0, DateTimeKind.Utc), rows[2].Shifted.UtcDateTime);
            }
            finally { await Teardown(ctx, DtsTable); }
        }
    }

    private static async Task SetupDtsTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t = ctx.Provider.Escape(DtsTable);
        var id   = EscapeCol(kind, "Id");
        var dto  = EscapeCol(kind, "Dto");
        var span = EscapeCol(kind, "Span");
        var ddl = DropTable(kind, DtsTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {dto} {DtoCol(kind)} NOT NULL, {span} {TimeSpanCol(kind)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        // Row 2 on MySQL: stored as UTC 10:00:00 (12:00:00+02:00 = 10:00Z).
        // Row 3 uses a positive span (SQL Server TIME is unsigned, no negative values).
        string insert = kind switch
        {
            ProviderKind.SqlServer =>
                $"INSERT INTO {t} ({id},{dto},{span}) VALUES " +
                "(1,CAST('2026-05-25 12:00:00 +00:00' AS DATETIMEOFFSET),CAST('01:30:00' AS TIME))," +
                "(2,CAST('2026-05-25 12:00:00 +02:00' AS DATETIMEOFFSET),CAST('00:00:30' AS TIME))," +
                "(3,CAST('2026-05-25 14:00:00 +00:00' AS DATETIMEOFFSET),CAST('00:45:00' AS TIME))",
            ProviderKind.Postgres =>
                $"INSERT INTO {t} ({id},{dto},{span}) VALUES " +
                "(1,'2026-05-25 12:00:00+00'::timestamptz,INTERVAL '1 hour 30 minutes')," +
                "(2,'2026-05-25 12:00:00+02'::timestamptz,INTERVAL '30 seconds')," +
                "(3,'2026-05-25 14:00:00+00'::timestamptz,INTERVAL '45 minutes')",
            ProviderKind.MySql =>
                $"INSERT INTO {t} ({id},{dto},{span}) VALUES " +
                "(1,'2026-05-25 12:00:00.000000','01:30:00')," +
                "(2,'2026-05-25 10:00:00.000000','00:00:30')," +
                "(3,'2026-05-25 14:00:00.000000','00:45:00')",
            ProviderKind.Sqlite =>
                $"INSERT INTO {t} ({id},{dto},{span}) VALUES " +
                "(1,'2026-05-25 12:00:00+00:00','01:30:00')," +
                "(2,'2026-05-25 12:00:00+02:00','00:00:30')," +
                "(3,'2026-05-25 14:00:00+00:00','00:45:00')",
            _ => throw new NotSupportedException()
        };
        await ExecuteAsync(ctx, insert);
    }

    [Table(DtsTable)]
    public sealed class DtsParityRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Span { get; set; }
    }

    // ------------------------------------------------------------------ test 4: DTO.LocalDateTime projection

    private const string LocalDtoTable = "LocalDtoParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_LocalDateTime_projection_returns_wall_clock_at_snapshot_local_offset(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupLocalDtoTableAsync(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LocalDtoParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Local = r.Dto.LocalDateTime })
                    .ToListAsync())
                    .ToArray();

                // LocalDateTime uses the local offset snapshotted at query-build time.
                // Compute the expected value the same way nORM does so the assertion
                // is TZ-agnostic (works whether the test machine is UTC, UTC+2, etc.).
                var localOffset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow);
                var utcBase = new DateTime(2026, 5, 25, 12, 0, 0, DateTimeKind.Utc);
                var expected = utcBase.Add(localOffset);

                var row = Assert.Single(rows);
                // ⚠️ Second-resolution: allow ±1s for sub-second rounding.
                Assert.InRange(row.Local, expected.AddSeconds(-1), expected.AddSeconds(1));
            }
            finally { await Teardown(ctx, LocalDtoTable); }
        }
    }

    private static async Task SetupLocalDtoTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t = ctx.Provider.Escape(LocalDtoTable);
        var id  = EscapeCol(kind, "Id");
        var dto = EscapeCol(kind, "Dto");
        var ddl = DropTable(kind, LocalDtoTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {dto} {DtoCol(kind)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        string insert = kind switch
        {
            ProviderKind.SqlServer =>
                $"INSERT INTO {t} ({id},{dto}) VALUES (1,CAST('2026-05-25 12:00:00 +00:00' AS DATETIMEOFFSET))",
            ProviderKind.Postgres =>
                $"INSERT INTO {t} ({id},{dto}) VALUES (1,'2026-05-25 12:00:00+00'::timestamptz)",
            ProviderKind.MySql =>
                $"INSERT INTO {t} ({id},{dto}) VALUES (1,'2026-05-25 12:00:00.000000')",
            ProviderKind.Sqlite =>
                $"INSERT INTO {t} ({id},{dto}) VALUES (1,'2026-05-25 12:00:00+00:00')",
            _ => throw new NotSupportedException()
        };
        await ExecuteAsync(ctx, insert);
    }

    [Table(LocalDtoTable)]
    public sealed class LocalDtoParityRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }

    // ------------------------------------------------------------------ test 5: Enum.TryParse<T>(col, out _)

    private const string EtpTable = "EtpParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Enum_TryParse_predicate_filters_valid_enum_names_server_side(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupEtpTableAsync(ctx, kind);
            try
            {
                // Expression trees can't use `out _`; must name the sink variable.
                EtpStatus sink;
                var rows = await ctx.Query<EtpParityRow>()
                    .Where(r => Enum.TryParse<EtpStatus>(r.StatusText, out sink))
                    .OrderBy(r => r.Id)
                    .ToListAsync();

                // Active (1), Inactive (2), Pending (4) are valid names; NotAStatus (3) is not.
                Assert.Equal(new[] { 1, 2, 4 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await Teardown(ctx, EtpTable); }
        }
    }

    private static async Task SetupEtpTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t   = ctx.Provider.Escape(EtpTable);
        var id  = EscapeCol(kind, "Id");
        var txt = EscapeCol(kind, "StatusText");
        var ddl = DropTable(kind, EtpTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {txt} {VarCharCol(kind, 50)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        string insert = kind switch
        {
            ProviderKind.MySql    => $"INSERT INTO {t} ({id},{txt}) VALUES (1,'Active'),(2,'Inactive'),(3,'NotAStatus'),(4,'Pending')",
            ProviderKind.Postgres => $"INSERT INTO {t} ({id},{txt}) VALUES (1,'Active'),(2,'Inactive'),(3,'NotAStatus'),(4,'Pending')",
            _                     => $"INSERT INTO {t} ({id},{txt}) VALUES (1,'Active'),(2,'Inactive'),(3,'NotAStatus'),(4,'Pending')"
        };
        await ExecuteAsync(ctx, insert);
    }

    public enum EtpStatus { Active, Inactive, Pending }

    [Table(EtpTable)]
    public sealed class EtpParityRow
    {
        [Key] public int Id { get; set; }
        public string StatusText { get; set; } = "";
    }

    // ------------------------------------------------------------------ test 6: Convert.ChangeType(col, typeof(T))

    private const string CctTable = "CctParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Convert_ChangeType_int_to_string_and_string_to_int_emit_server_side_CAST(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupCctTableAsync(ctx, kind);
            try
            {
                var intToStr = (await ctx.Query<CctParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, S = (string)Convert.ChangeType(r.IntVal, typeof(string)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal("42", intToStr[0].S);
                Assert.Equal("7",  intToStr[1].S);

                var strToInt = (await ctx.Query<CctParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, N = (int)Convert.ChangeType(r.TextVal, typeof(int)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(99, strToInt[0].N);
                Assert.Equal(13, strToInt[1].N);

                // bool cast: nonzero → true, zero → false (server-side via provider CAST hook)
                var intToBool = (await ctx.Query<CctParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, B = (bool)Convert.ChangeType(r.IntVal, typeof(bool)) })
                    .ToListAsync())
                    .ToArray();
                Assert.True(intToBool[0].B);   // 42 → true
                Assert.True(intToBool[1].B);   // 7  → true
                Assert.False(intToBool[2].B);  // 0  → false
            }
            finally { await Teardown(ctx, CctTable); }
        }
    }

    private static async Task SetupCctTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t    = ctx.Provider.Escape(CctTable);
        var id   = EscapeCol(kind, "Id");
        var ival = EscapeCol(kind, "IntVal");
        var tval = EscapeCol(kind, "TextVal");
        var ddl = DropTable(kind, CctTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {ival} INT NOT NULL, {tval} {VarCharCol(kind, 50)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);
        await ExecuteAsync(ctx, $"INSERT INTO {t} ({id},{ival},{tval}) VALUES (1,42,'99'),(2,7,'13'),(3,0,'0')");
    }

    [Table(CctTable)]
    public sealed class CctParityRow
    {
        [Key] public int Id { get; set; }
        public int IntVal { get; set; }
        public string TextVal { get; set; } = "";
    }

    // ------------------------------------------------------------------ test 7: Aggregate sum-fold

    private const string AggTable = "AggParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Aggregate_sum_fold_lowers_to_server_side_SUM(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAggTableAsync(ctx, kind);
            try
            {
                long total = ctx.Query<AggParityRow>()
                    .Select(r => (long)r.Score)
                    .Aggregate(0L, (acc, s) => acc + s);
                Assert.Equal(100L, total);

                long noSeed = ctx.Query<AggParityRow>()
                    .Select(r => (long)r.Score)
                    .Aggregate((acc, s) => acc + s);
                Assert.Equal(100L, noSeed);
            }
            finally { await Teardown(ctx, AggTable); }
        }
    }

    // ------------------------------------------------------------------ test 8: Aggregate min/max-fold

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Aggregate_max_and_min_fold_lower_to_server_side_MAX_MIN(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAggTableAsync(ctx, kind);
            try
            {
                int max = ctx.Query<AggParityRow>()
                    .Select(r => r.Score)
                    .Aggregate((acc, x) => x > acc ? x : acc);
                Assert.Equal(40, max);

                int mathMax = ctx.Query<AggParityRow>()
                    .Select(r => r.Score)
                    .Aggregate((acc, x) => Math.Max(acc, x));
                Assert.Equal(40, mathMax);

                int min = ctx.Query<AggParityRow>()
                    .Select(r => r.Score)
                    .Aggregate((acc, x) => x < acc ? x : acc);
                Assert.Equal(10, min);
            }
            finally { await Teardown(ctx, AggTable); }
        }
    }

    // ------------------------------------------------------------------ test 10: Aggregate conditional fold

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Aggregate_conditional_fold_lowers_to_server_side_SUM_CASE_WHEN(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAggTableAsync(ctx, kind);
            try
            {
                // Data: (10,20,30,40). Score > 15: rows with 20,30,40 → count = 3.
                int count = ctx.Query<AggParityRow>()
                    .Aggregate(0, (acc, x) => acc + (x.Score > 15 ? 1 : 0));
                Assert.Equal(3, count);

                // Custom weight: Score > 15 → 2, else → 1. Total = 1+2+2+2 = 7.
                int weighted = ctx.Query<AggParityRow>()
                    .Aggregate(0, (acc, x) => acc + (x.Score > 15 ? 2 : 1));
                Assert.Equal(7, weighted);
            }
            finally { await Teardown(ctx, AggTable); }
        }
    }

    private static async Task SetupAggTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t  = ctx.Provider.Escape(AggTable);
        var id = EscapeCol(kind, "Id");
        var sc = EscapeCol(kind, "Score");
        var ddl = DropTable(kind, AggTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {sc} INT NOT NULL)";
        await ExecuteAsync(ctx, ddl);
        // (10,20,30,40) → sum=100, max=40, min=10
        await ExecuteAsync(ctx, $"INSERT INTO {t} ({id},{sc}) VALUES (1,10),(2,20),(3,30),(4,40)");
    }

    [Table(AggTable)]
    public sealed class AggParityRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    // ------------------------------------------------------------------ test 9: Aggregate string-concat fold

    private const string CcTable = "CcParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Aggregate_string_concat_fold_lowers_to_server_side_STRING_AGG_or_GROUP_CONCAT(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupCcTableAsync(ctx, kind);
            try
            {
                // ORDER BY Id is preserved inside STRING_AGG / GROUP_CONCAT via the
                // provider's ordered aggregate overload — result must match insertion order.
                string joined = ctx.Query<CcParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => r.Name)
                    .Aggregate((acc, s) => acc + "|" + s);

                Assert.Equal("alpha|bravo|charlie", joined);
            }
            finally { await Teardown(ctx, CcTable); }
        }
    }

    private static async Task SetupCcTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t    = ctx.Provider.Escape(CcTable);
        var id   = EscapeCol(kind, "Id");
        var name = EscapeCol(kind, "Name");
        var ddl = DropTable(kind, CcTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {name} {VarCharCol(kind, 50)} NOT NULL)";
        await ExecuteAsync(ctx, ddl);
        await ExecuteAsync(ctx, $"INSERT INTO {t} ({id},{name}) VALUES (1,'alpha'),(2,'bravo'),(3,'charlie')");
    }

    [Table(CcTable)]
    public sealed class CcParityRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // ------------------------------------------------------------------ test 11: TimeSpan column < column

    private const string TsCmpTable = "TsCmpParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task TimeSpan_cross_column_comparison_uses_numeric_ordering_on_all_providers(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupTsCmpTableAsync(ctx, kind);
            try
            {
                // Rows 1 (10min < 20min) and, for providers supporting multi-day spans,
                // row 3 (1day < 2days). Row 4 is the lex-ordering trap: "10.00:00:00" <
                // "9.23:00:00" lexicographically, but 10 days > 9 days 23 hours numerically.
                var loLtHi = (await ctx.Query<TsCmpParityRow>()
                    .Where(r => r.Lo < r.Hi)
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                var loGtHi = (await ctx.Query<TsCmpParityRow>()
                    .Where(r => r.Lo > r.Hi)
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                var loEqHi = (await ctx.Query<TsCmpParityRow>()
                    .Where(r => r.Lo == r.Hi)
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                bool multiDay = kind != ProviderKind.SqlServer;

                Assert.Equal(multiDay ? new[] { 1, 3 } : new[] { 1 }, loLtHi);
                Assert.Equal(multiDay ? new[] { 2, 4 } : new[] { 2 }, loGtHi);
                Assert.Equal(new[] { 5 }, loEqHi);
            }
            finally { await Teardown(ctx, TsCmpTable); }
        }
    }

    private static string TimeSpanLiteralSql(ProviderKind kind, string hms) => kind switch
    {
        ProviderKind.SqlServer => $"CAST('{hms}' AS TIME)",
        ProviderKind.Postgres  => $"INTERVAL '{hms}'",
        ProviderKind.MySql     => $"'{hms}'",
        ProviderKind.Sqlite    => $"'{hms}'",
        _ => throw new NotSupportedException()
    };

    private static async Task SetupTsCmpTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t  = ctx.Provider.Escape(TsCmpTable);
        var id = EscapeCol(kind, "Id");
        var lo = EscapeCol(kind, "Lo");
        var hi = EscapeCol(kind, "Hi");
        var tsType = TimeSpanCol(kind);
        var ddl = DropTable(kind, TsCmpTable, t) +
                  $" CREATE TABLE {t} ({id} {IdCol(kind)} PRIMARY KEY, {lo} {tsType} NOT NULL, {hi} {tsType} NOT NULL)";
        await ExecuteAsync(ctx, ddl);

        // Rows 1-2 and row 5 (equal) work on all providers.
        // Rows 3-4 (multi-day) require INTERVAL/MySQL multi-hour TIME — skipped for SQL Server TIME.
        // Row 4 is the lex-ordering trap: '10.00:00:00' < '9.23:00:00' lexicographically
        // but 10 days > 9 days 23 hours numerically.
        string L(string hms) => TimeSpanLiteralSql(kind, hms);

        string baseRows = $"INSERT INTO {t} ({id},{lo},{hi}) VALUES " +
                          $"(1,{L("00:10:00")},{L("00:20:00")})," +   // Lo < Hi ✓
                          $"(2,{L("00:20:00")},{L("00:10:00")})," +   // Lo > Hi ✓
                          $"(5,{L("01:00:00")},{L("01:00:00")})";     // Lo == Hi ✓
        await ExecuteAsync(ctx, baseRows);

        if (kind != ProviderKind.SqlServer)
        {
            // Multi-day rows: SQL Server TIME cannot exceed 23:59:59.
            string multiDayLo3, multiDayHi3, multiDayLo4, multiDayHi4;
            if (kind == ProviderKind.Postgres)
            {
                multiDayLo3 = "1 day";      multiDayHi3 = "2 days";
                multiDayLo4 = "10 days";    multiDayHi4 = "9 days 23 hours";
            }
            else if (kind == ProviderKind.MySql)
            {
                multiDayLo3 = "24:00:00";   multiDayHi3 = "48:00:00";
                multiDayLo4 = "240:00:00";  multiDayHi4 = "239:00:00";
            }
            else // SQLite — canonical 'c' TEXT
            {
                multiDayLo3 = "1.00:00:00"; multiDayHi3 = "2.00:00:00";
                multiDayLo4 = "10.00:00:00"; multiDayHi4 = "9.23:00:00";
            }
            await ExecuteAsync(ctx,
                $"INSERT INTO {t} ({id},{lo},{hi}) VALUES " +
                $"(3,{TimeSpanLiteralSql(kind, multiDayLo3)},{TimeSpanLiteralSql(kind, multiDayHi3)})," +
                $"(4,{TimeSpanLiteralSql(kind, multiDayLo4)},{TimeSpanLiteralSql(kind, multiDayHi4)})");
        }
    }

    [Table(TsCmpTable)]
    public sealed class TsCmpParityRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Lo { get; set; }
        public TimeSpan Hi { get; set; }
    }
}
