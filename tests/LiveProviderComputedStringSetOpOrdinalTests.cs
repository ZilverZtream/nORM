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
/// Parity guard: LINQ set operations compare strings ORDINALLY, and a set-op arm projecting a COMPUTED
/// string (x.First + x.Last, Substring, ToUpper, ...) must dedup/match case-sensitively on a case-
/// INSENSITIVE provider (MySQL, SQL Server) just like a mapped column does — "ABC" and "abc" stay distinct
/// under Union and don't cross-match under Intersect. The set-op dedup wraps the OUTPUT column in the
/// provider's ordinal collation, so this holds even though the inner projection emits the computed string
/// raw; this test pins that end-to-end behaviour across providers (a sweep flagged the inner-projection
/// emit as a gap — verified NOT a bug: the outer dedup wrap covers it).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderComputedStringSetOpOrdinalTests
{
    private const string Table = "CompStrRow";

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
        var eF = ctx.Provider.Escape("First");
        var eL = ctx.Provider.Escape("Last");
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var varT = kind == ProviderKind.SqlServer ? "NVARCHAR(20)" : "VARCHAR(20)";
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eF} {varT} NOT NULL, {eL} {varT} NOT NULL)");
        // Two rows whose concatenations differ only in CASE.
        await ExecuteAsync(ctx, $"INSERT INTO {esc} ({eId},{eF},{eL}) VALUES (1,'AB','C'), (2,'ab','c')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class CompStrRow
    {
        [Key] public int Id { get; set; }
        public string First { get; set; } = "";
        public string Last { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Union_of_computed_string_dedups_ordinally(ProviderKind kind)
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
                var arm1 = ctx.Query<CompStrRow>().Where(r => r.Id == 1).Select(r => r.First + r.Last); // "ABC"
                var arm2 = ctx.Query<CompStrRow>().Where(r => r.Id == 2).Select(r => r.First + r.Last); // "abc"

                var results = (await arm1.Union(arm2).ToListAsync())
                    .OrderBy(s => s, StringComparer.Ordinal)
                    .ToArray();

                // Ordinal Union keeps "ABC" and "abc" distinct. BUG on CI collation: they merge to one row.
                Assert.Equal(new[] { "ABC", "abc" }, results);

                // Intersect: ordinal has NO common value ("ABC" != "abc"); CI collation cross-matches -> 1 row.
                var inter = (await arm1.Intersect(arm2).ToListAsync()).Count;
                Assert.Equal(0, inter);

                // Computed string as an anonymous-shape MEMBER: same ordinal dedup required.
                var shaped1 = ctx.Query<CompStrRow>().Where(r => r.Id == 1).Select(r => new { Full = r.First + r.Last });
                var shaped2 = ctx.Query<CompStrRow>().Where(r => r.Id == 2).Select(r => new { Full = r.First + r.Last });
                var shapedCount = (await shaped1.Union(shaped2).ToListAsync()).Count;
                Assert.Equal(2, shapedCount);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
