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
/// is to confirm the per-provider hooks shipped in 560f24b, 1f06ac1,
/// f46ac85, dd83b32, 82e1dfa, 131a845, b3ca42b, f09f851, f8bd921,
/// 9b38245 actually behave correctly on real engines — provider-shape
/// equivalence is not the same as runtime parity.
///
/// Tracked in docs/live-provider-linq-parity.md — flips 🚧 rows to ✅.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderRecentScvParityTests
{
    private const string DtoTable = "DtoParity";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task DateTimeOffset_column_equals_UTC_DateTime_literal_matches_by_UTC_instant_on_live_provider(ProviderKind kind)
    {
        // 560f24b: DTO col == DateTime(Utc) literal lowers to UTC-instant
        // equality via GetDateTimeOffsetUtcEpochSecondsSql. Two rows that
        // store the SAME UTC instant in DIFFERENT offsets must both match;
        // a row one second later must not. This proves the per-provider
        // epoch hook (SqlServer DATEDIFF_BIG, Postgres EXTRACT EPOCH,
        // MySQL UNIX_TIMESTAMP+CONVERT_TZ, SQLite strftime('%s')) gives
        // the correct UTC instant.
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
                // Row 1 (12:30:45+00:00) and row 2 (14:30:45+02:00) are the same
                // UTC instant; row 3 is one second later. Equality must match
                // rows 1 and 2.
                Assert.Equal(new[] { 1, 2 }, rows.Select(r => r.Id).ToArray());

                var notRows = await ctx.Query<DtoParityRow>()
                    .Where(r => r.Dto != literal)
                    .OrderBy(r => r.Id)
                    .ToListAsync();
                Assert.Equal(new[] { 3 }, notRows.Select(r => r.Id).ToArray());
            }
            finally
            {
                await TeardownDtoTableAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupDtoTableAsync(DbContext ctx, ProviderKind kind)
    {
        var t = ctx.Provider.Escape(DtoTable);
        var ddl = kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'{DtoTable}', N'U') IS NOT NULL DROP TABLE {t}; CREATE TABLE {t} (Id INT PRIMARY KEY, Dto DATETIMEOFFSET NOT NULL)",
            ProviderKind.Postgres => $"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (\"Id\" INT PRIMARY KEY, \"Dto\" TIMESTAMPTZ NOT NULL)",
            // MySQL has no native DATETIMEOFFSET; nORM stores DTO as DATETIME(6)
            // pre-normalised to UTC. Insert UTC instants directly.
            ProviderKind.MySql => $"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (`Id` INT PRIMARY KEY, `Dto` DATETIME(6) NOT NULL)",
            ProviderKind.Sqlite => $"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (Id INTEGER PRIMARY KEY, Dto TEXT NOT NULL)",
            _ => throw new NotSupportedException()
        };
        await ExecuteAsync(ctx, ddl);

        // Provider-specific INSERT for the offset-bearing literals.
        // All three rows: 1 and 2 share UTC instant 12:30:45Z; row 3 is one second later.
        string insert;
        switch (kind)
        {
            case ProviderKind.SqlServer:
                insert = $"INSERT INTO {t} (Id, Dto) VALUES " +
                         "(1, CAST('2026-05-25 12:30:45 +00:00' AS DATETIMEOFFSET)), " +
                         "(2, CAST('2026-05-25 14:30:45 +02:00' AS DATETIMEOFFSET)), " +
                         "(3, CAST('2026-05-25 12:30:46 +00:00' AS DATETIMEOFFSET))";
                break;
            case ProviderKind.Postgres:
                insert = $"INSERT INTO {t} (\"Id\", \"Dto\") VALUES " +
                         "(1, '2026-05-25 12:30:45+00'::timestamptz), " +
                         "(2, '2026-05-25 14:30:45+02'::timestamptz), " +
                         "(3, '2026-05-25 12:30:46+00'::timestamptz)";
                break;
            case ProviderKind.MySql:
                // DATETIME(6) — store the UTC wall-clock directly (the +02 row
                // converts to 12:30:45 UTC).
                insert = $"INSERT INTO {t} (`Id`, `Dto`) VALUES " +
                         "(1, '2026-05-25 12:30:45.000000'), " +
                         "(2, '2026-05-25 12:30:45.000000'), " +
                         "(3, '2026-05-25 12:30:46.000000')";
                break;
            case ProviderKind.Sqlite:
                insert = $"INSERT INTO {t} (Id, Dto) VALUES " +
                         "(1, '2026-05-25 12:30:45+00:00'), " +
                         "(2, '2026-05-25 14:30:45+02:00'), " +
                         "(3, '2026-05-25 12:30:46+00:00')";
                break;
            default:
                throw new NotSupportedException();
        }
        await ExecuteAsync(ctx, insert);
    }

    private static async Task TeardownDtoTableAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, $"DROP TABLE IF EXISTS {ctx.Provider.Escape(DtoTable)}");
        }
        catch { /* best-effort */ }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table(DtoTable)]
    public sealed class DtoParityRow
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset Dto { get; set; }
    }
}
