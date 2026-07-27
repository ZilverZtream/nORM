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
/// .NET String.Length / Substring / PadLeft count every character including trailing spaces. T-SQL LEN()
/// EXCLUDES trailing spaces, so SQL Server's Length/Substring/Pad lowerings (which used LEN) silently
/// under-counted a value with trailing spaces — a predicate/projection over .Length returned the wrong
/// number, Substring dropped trailing spaces, and Pad over-padded. The other providers count trailing
/// spaces (SQLite/Postgres LENGTH, MySQL CHAR_LENGTH), so the result must match them and .NET.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderStringLengthTrailingSpaceParityTests
{
    private const string Table = "TrailSpaceRow";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc = ctx.Provider.Escape(Table);
        var eId = ctx.Provider.Escape("Id");
        var eCo = ctx.Provider.Escape("Code");
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var varT = kind == ProviderKind.SqlServer ? "NVARCHAR(20)" : "VARCHAR(20)";
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eCo} {varT} NOT NULL)");
        // "AB   " — two letters and three trailing spaces; .NET Length == 5.
        await ExecuteAsync(ctx, $"INSERT INTO {esc} ({eId},{eCo}) VALUES (1, 'AB   ')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class TrailSpaceRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_length_substring_and_pad_count_trailing_spaces(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                // Length == 5 (2 letters + 3 trailing spaces).
                var byLen = (await ctx.Query<TrailSpaceRow>()
                    .Where(r => r.Code.Length == 5)
                    .ToListAsync())
                    .Select(r => r.Id).ToArray();
                Assert.Equal(new[] { 1 }, byLen);   // BUG on SQL Server: LEN('AB   ')=2 != 5 -> empty

                var projected = (await ctx.Query<TrailSpaceRow>()
                    .Select(r => new { r.Id, Len = r.Code.Length, Sub = r.Code.Substring(1), Pad = r.Code.PadLeft(8) })
                    .ToListAsync())
                    .Single();

                Assert.Equal(5, projected.Len);
                Assert.Equal("B   ", projected.Sub);        // from index 1 to end, trailing spaces kept
                Assert.Equal("   AB   ", projected.Pad);     // pad to 8: 3 leading spaces + "AB   "
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
