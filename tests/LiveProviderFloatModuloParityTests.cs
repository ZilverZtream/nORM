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
/// The modulo operator on floating-point operands (double/float) must translate to C#'s remainder on every
/// provider. Emitting a bare `col % n` errors on SQL Server ("float and int are incompatible in the modulo
/// operator") and PostgreSQL ("operator does not exist: double precision % integer"), and on SQLite `%`
/// converts both operands to INTEGER (silently wrong). The result must equal C#'s double `%`.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderFloatModuloParityTests
{
    private const string Table = "FloatModRow";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static string DblType(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "FLOAT",
        ProviderKind.Postgres => "DOUBLE PRECISION",
        ProviderKind.MySql => "DOUBLE",
        _ => "REAL"
    };

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc = ctx.Provider.Escape(Table);
        var eId = ctx.Provider.Escape("Id");
        var eV = ctx.Provider.Escape("V");
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        await ExecuteAsync(ctx, $"{DropDdl(kind, esc)} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eV} {DblType(kind)} NOT NULL)");
        await ExecuteAsync(ctx, $"INSERT INTO {esc} ({eId},{eV}) VALUES (1, 20.5), (2, -7.5)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class FloatModRow
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Double_modulo_matches_dotnet(ProviderKind kind)
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
                var rows = (await ctx.Query<FloatModRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, M = r.V % 2 })
                    .ToListAsync())
                    .ToArray();

                // C#: 20.5 % 2 == 0.5 ; -7.5 % 2 == -1.5 (sign of the dividend).
                Assert.Equal(20.5 % 2, rows[0].M, 6);
                Assert.Equal(-7.5 % 2, rows[1].M, 6);

                // Predicate form exercises the WHERE-side translator too: only row 1's remainder (0.5) > 0.
                var ids = (await ctx.Query<FloatModRow>()
                    .Where(r => (r.V % 2) > 0)
                    .ToListAsync())
                    .Select(r => r.Id).OrderBy(i => i).ToArray();
                Assert.Equal(new[] { 1 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
