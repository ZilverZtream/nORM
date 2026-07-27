using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for the case-sensitive char classification predicates
/// <c>char.IsUpper</c> / <c>char.IsLower</c>. These lowered to a string range
/// (<c>c BETWEEN 'A' AND 'Z'</c> / <c>'a' AND 'z'</c>), which is evaluated under the
/// column's collation — so on a case-INSENSITIVE default collation (SQL Server, MySQL)
/// 'a' compared within 'A'..'Z' and both predicates matched BOTH cases. The result must
/// instead be codepoint-based (immune to collation), matching .NET on every provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderCharCasePredicateParityTests
{
    private const string Table = "CharCaseRow";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string VarCol(ProviderKind kind) => kind == ProviderKind.SqlServer ? "NVARCHAR(1)" : "VARCHAR(1)";
    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc = ctx.Provider.Escape(Table);
        var eId = ctx.Provider.Escape("Id");
        var eC = ctx.Provider.Escape("C");
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} " +
            $"({eId} {IntCol(kind)} PRIMARY KEY, {eC} {VarCol(kind)} NOT NULL)");
        // Upper A/Z, lower a/z, and a digit (neither upper nor lower).
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eC}) VALUES (1,'A'),(2,'a'),(3,'Z'),(4,'z'),(5,'5')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class CharCaseRow
    {
        [Key] public int Id { get; set; }
        public string C { get; set; } = "";
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Char_IsUpper_matches_only_uppercase_regardless_of_collation(ProviderKind kind)
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
                var ids = (await ctx.Query<CharCaseRow>()
                    .Where(r => char.IsUpper(r.C[0]))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 1, 3 }, ids);   // BUG on CI collation: {1,2,3,4}
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Char_IsLower_matches_only_lowercase_regardless_of_collation(ProviderKind kind)
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
                var ids = (await ctx.Query<CharCaseRow>()
                    .Where(r => char.IsLower(r.C[0]))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 2, 4 }, ids);   // BUG on CI collation: {1,2,3,4}
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
