using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for char classification predicates
/// (<c>char.IsDigit</c>, <c>char.IsLetter</c>, <c>char.IsWhiteSpace</c>) and
/// string null/whitespace predicates (<c>string.IsNullOrEmpty</c>,
/// <c>string.IsNullOrWhiteSpace</c>) plus <c>string.Length</c> comparisons.
///
/// All of these lower to portable SQL (BETWEEN, IS NULL, LENGTH/LEN/CHAR_LENGTH)
/// and must produce the same row results on every configured live provider.
///
/// Tracked in docs/live-provider-linq-parity.md — Type-specific surfaces section.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderCharStringPredicateParityTests
{
    // ── table layout ──────────────────────────────────────────────────────────
    // StrPredRow (Id, Code, Tag)
    //   (1, '1',     'hello')   — Code starts with digit '1', length 1
    //   (2, 'ab',    'world')   — Code starts with letter 'a', length 2
    //   (3, '2cd',   NULL)      — Code starts with digit '2', length 3, Tag NULL
    //   (4, 'efg',   '')        — Code starts with letter 'e', length 3, Tag empty
    //   (5, '3efgh', '   ')     — Code starts with digit '3', length 5, Tag spaces
    //   (6, 'ijklm', 'test')    — Code all letters, length 5
    //
    // char.IsDigit(Code[0]) → {1,3,5}
    // char.IsLetter(Code[0]) → {2,4,6}
    // string.IsNullOrEmpty(Tag) → {3,4}
    // string.IsNullOrWhiteSpace(Tag) → {3,4,5}
    // Code.Length > 2 → {3,4,5,6}

    private const string Table = "StrPredRow";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string IntCol(ProviderKind kind) =>
        kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc  = ctx.Provider.Escape(Table);
        var eId  = ctx.Provider.Escape("Id");
        var eCo  = ctx.Provider.Escape("Code");
        var eTa  = ctx.Provider.Escape("Tag");
        var intT = IntCol(kind);
        var varT = VarCol(kind, 20);
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} " +
            $"({eId} {intT} PRIMARY KEY, {eCo} {varT} NOT NULL, {eTa} {varT} NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eCo},{eTa}) VALUES " +
            $"(1,'1','hello')," +
            $"(2,'ab','world')," +
            $"(3,'2cd',NULL)," +
            $"(4,'efg','')," +
            $"(5,'3efgh','   ')," +
            $"(6,'ijklm','test')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class StrPredRow
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public string? Tag { get; set; }
    }

    // ── 1: char.IsDigit ───────────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Char_IsDigit_on_first_character_of_column_filters_correctly(ProviderKind kind)
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
                var ids = (await ctx.Query<StrPredRow>()
                    .Where(r => char.IsDigit(r.Code[0]))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 1, 3, 5 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: char.IsLetter ─────────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Char_IsLetter_on_first_character_of_column_filters_correctly(ProviderKind kind)
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
                var ids = (await ctx.Query<StrPredRow>()
                    .Where(r => char.IsLetter(r.Code[0]))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 2, 4, 6 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: string.IsNullOrEmpty ───────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_IsNullOrEmpty_matches_null_and_empty_string_columns(ProviderKind kind)
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
                var ids = (await ctx.Query<StrPredRow>()
                    .Where(r => string.IsNullOrEmpty(r.Tag))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 3, 4 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: string.IsNullOrWhiteSpace ─────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_IsNullOrWhiteSpace_matches_null_empty_and_whitespace_columns(ProviderKind kind)
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
                var ids = (await ctx.Query<StrPredRow>()
                    .Where(r => string.IsNullOrWhiteSpace(r.Tag))
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 3, 4, 5 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: string.Length comparison ───────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_Length_comparison_on_column_filters_by_character_count(ProviderKind kind)
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
                // Code.Length > 2 → codes '2cd'(3),'efg'(3),'3efgh'(5),'ijklm'(5) → IDs {3,4,5,6}
                var ids = (await ctx.Query<StrPredRow>()
                    .Where(r => r.Code.Length > 2)
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 3, 4, 5, 6 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task String_format_and_interpolation_project_expected_values(ProviderKind kind)
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
                var rows = await ctx.Query<StrPredRow>()
                    .Where(r => r.Id <= 2)
                    .OrderBy(r => r.Id)
                    .Select(r => new
                    {
                        Format = string.Format("{0}:{1}", r.Code, r.Id),
                        Interpolated = $"{r.Code}-{r.Id}"
                    })
                    .ToListAsync();

                Assert.Equal(2, rows.Count);
                Assert.Equal("1:1", rows[0].Format);
                Assert.Equal("1-1", rows[0].Interpolated);
                Assert.Equal("ab:2", rows[1].Format);
                Assert.Equal("ab-2", rows[1].Interpolated);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
