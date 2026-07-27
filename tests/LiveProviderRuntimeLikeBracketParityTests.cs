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
/// Contains/StartsWith/EndsWith with a RUNTIME (parameterized) search term must match the term literally.
/// SQL Server treats '[' as a LIKE character-class opener; its constant-pattern path escapes '[', but the
/// runtime path (GetLikeEscapeSql) did not, so a term containing '[' silently became a wildcard class —
/// Contains("[0-9]") matched any row containing a digit instead of the literal substring. The other
/// providers treat '[' literally; the result must match them and .NET.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderRuntimeLikeBracketParityTests
{
    private const string Table = "LikeBracketRow";

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
        var eNa = ctx.Provider.Escape("Name");
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var varT = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eNa} {varT} NOT NULL)");
        // Row 1 contains the LITERAL substring "[0-9]"; row 2 contains a digit but not that substring.
        await ExecuteAsync(ctx, $"INSERT INTO {esc} ({eId},{eNa}) VALUES (1, 'x[0-9]y'), (2, 'digit5here')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class LikeBracketRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Contains_with_runtime_bracket_term_matches_literally(ProviderKind kind)
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
                var term = "[0-9]";   // runtime variable -> parameterized LIKE path
                var ids = (await ctx.Query<LikeBracketRow>()
                    .Where(r => r.Name.Contains(term))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                // Only the row containing the literal "[0-9]" — NOT the one that merely contains a digit.
                Assert.Equal(new[] { 1 }, ids);   // BUG on SQL Server: {1,2} — '[0-9]' became a digit class
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
