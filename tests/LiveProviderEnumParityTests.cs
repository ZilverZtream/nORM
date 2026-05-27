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
/// Live-provider proof for the enum row in docs/live-provider-linq-parity.md.
/// Keeps the related shapes together because they share the same provider
/// integer/text/CASE lowering risks.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderEnumParityTests
{
    private const string Table = "EnumParityRow";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Enum_comparison_ToString_Parse_IsDefined_and_HasFlag_match_linq_on_live_provider(ProviderKind kind)
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
                var activeIds = (await ctx.Query<EnumParityRow>()
                    .Where(r => r.Status == LiveStatus.Active)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 2, 5 }, activeIds.ToArray());

                var names = await ctx.Query<EnumParityRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Name = r.Status.ToString() })
                    .ToListAsync();
                Assert.Equal(new[] { "Pending", "Active", "Closed", "99", "Active" }, names.Select(r => r.Name).ToArray());

                var parsedIds = (await ctx.Query<EnumParityRow>()
                    .Where(r => Enum.Parse<LiveStatus>(r.StatusText) == LiveStatus.Closed)
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 3 }, parsedIds.ToArray());

                var definedIds = (await ctx.Query<EnumParityRow>()
                    .Where(r => Enum.IsDefined(typeof(LiveStatus), (int)r.Status))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 1, 2, 3, 5 }, definedIds.ToArray());

                var adminIds = (await ctx.Query<EnumParityRow>()
                    .Where(r => r.Perms.HasFlag(LivePerm.Admin))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 3, 4, 5 }, adminIds.ToArray());

                var readAdminIds = (await ctx.Query<EnumParityRow>()
                    .Where(r => r.Perms.HasFlag(LivePerm.Read | LivePerm.Admin))
                    .OrderBy(r => r.Id)
                    .ToListAsync())
                    .Select(r => r.Id)
                    .ToArray();
                Assert.Equal(new[] { 3, 4 }, readAdminIds.ToArray());
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var table = ctx.Provider.Escape(Table);
        var id = ctx.Provider.Escape(nameof(EnumParityRow.Id));
        var status = ctx.Provider.Escape(nameof(EnumParityRow.Status));
        var statusText = ctx.Provider.Escape(nameof(EnumParityRow.StatusText));
        var perms = ctx.Provider.Escape(nameof(EnumParityRow.Perms));
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var textType = kind == ProviderKind.SqlServer ? "NVARCHAR(40)" : "VARCHAR(40)";
        var drop = kind == ProviderKind.SqlServer
            ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table};"
            : $"DROP TABLE IF EXISTS {table};";

        await ExecuteAsync(ctx, drop);
        await ExecuteAsync(ctx, $"CREATE TABLE {table} ({id} {intType} PRIMARY KEY, {status} {intType} NOT NULL, {statusText} {textType} NOT NULL, {perms} {intType} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {table} ({id},{status},{statusText},{perms}) VALUES " +
            "(1,0,'Pending',1)," +
            "(2,1,'Active',3)," +
            "(3,2,'Closed',5)," +
            "(4,99,'Pending',7)," +
            "(5,1,'Active',4)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var table = ctx.Provider.Escape(Table);
            var drop = kind == ProviderKind.SqlServer
                ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {table}"
                : $"DROP TABLE IF EXISTS {table}";
            await ExecuteAsync(ctx, drop);
        }
        catch
        {
            // best-effort cleanup only
        }
    }

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public enum LiveStatus
    {
        Pending = 0,
        Active = 1,
        Closed = 2,
    }

    [Flags]
    public enum LivePerm
    {
        None = 0,
        Read = 1,
        Write = 2,
        Admin = 4,
    }

    [Table(Table)]
    private sealed class EnumParityRow
    {
        [Key] public int Id { get; set; }
        public LiveStatus Status { get; set; }
        public string StatusText { get; set; } = string.Empty;
        public LivePerm Perms { get; set; }
    }
}
