using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for ExecuteUpdate/Delete over composite-primary-key entities.
/// This pins the provider-specific SQL paths: row-tuple IN on SQLite/Postgres/MySQL and
/// JOIN-based target CUD on SQL Server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderCompositePkBulkCudParityTests
{
    private const string StoreTable = "LbcStore";
    private const string OrderTable = "LbcOrder";

    [Table(StoreTable)]
    private sealed class LbcStore
    {
        [Key] public int Id { get; set; }
        public int Active { get; set; }
    }

    [Table(OrderTable)]
    private sealed class LbcOrder
    {
        public int StoreId { get; set; }
        public int OrderId { get; set; }
        public string Status { get; set; } = "";
        public bool Archived { get; set; }
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string BoolCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "BIT",
        ProviderKind.Postgres => "BOOLEAN",
        ProviderKind.MySql => "BOOLEAN",
        _ => "INTEGER"
    };

    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string table, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb => mb.Entity<LbcOrder>().HasKey(o => new { o.StoreId, o.OrderId })
    };

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var es = ctx.Provider.Escape(StoreTable);
        var eo = ctx.Provider.Escape(OrderTable);
        await ExecuteAsync(ctx, DropDdl(kind, OrderTable, eo));
        await ExecuteAsync(ctx, DropDdl(kind, StoreTable, es));

        var intT = IntCol(kind);
        var boolT = BoolCol(kind);
        var varT = VarCol(kind, 20);
        await ExecuteAsync(ctx,
            $"CREATE TABLE {es} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, " +
            $"{ctx.Provider.Escape("Active")} {intT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {eo} ({ctx.Provider.Escape("StoreId")} {intT} NOT NULL, " +
            $"{ctx.Provider.Escape("OrderId")} {intT} NOT NULL, " +
            $"{ctx.Provider.Escape("Status")} {varT} NOT NULL, " +
            $"{ctx.Provider.Escape("Archived")} {boolT} NOT NULL, " +
            $"PRIMARY KEY ({ctx.Provider.Escape("StoreId")}, {ctx.Provider.Escape("OrderId")}))");

        await ExecuteAsync(ctx, $"INSERT INTO {es} ({ctx.Provider.Escape("Id")},{ctx.Provider.Escape("Active")}) VALUES (1,1),(2,0),(3,1)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {eo} ({ctx.Provider.Escape("StoreId")},{ctx.Provider.Escape("OrderId")},{ctx.Provider.Escape("Status")},{ctx.Provider.Escape("Archived")}) VALUES " +
            $"(1,1,'open',{ctx.Provider.BooleanFalseLiteral})," +
            $"(1,2,'closed',{ctx.Provider.BooleanFalseLiteral})," +
            $"(2,1,'open',{ctx.Provider.BooleanFalseLiteral})," +
            $"(2,2,'open',{ctx.Provider.BooleanFalseLiteral})," +
            $"(3,1,'closed',{ctx.Provider.BooleanFalseLiteral})");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropDdl(kind, OrderTable, ctx.Provider.Escape(OrderTable)));
            await ExecuteAsync(ctx, DropDdl(kind, StoreTable, ctx.Provider.Escape(StoreTable)));
        }
        catch { /* best-effort */ }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteUpdate_composite_pk_join_source_updates_only_matching_rows_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider, Options()))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await ctx.Query<LbcOrder>()
                    .Join(ctx.Query<LbcStore>().Where(s => s.Active == 0),
                          o => o.StoreId, s => s.Id,
                          (o, s) => o)
                    .ExecuteUpdateAsync(s => s.SetProperty(o => o.Archived, true));

                var orders = await ctx.Query<LbcOrder>().ToListAsync();
                Assert.All(orders.Where(o => o.StoreId == 2), o => Assert.True(o.Archived));
                Assert.All(orders.Where(o => o.StoreId != 2), o => Assert.False(o.Archived));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteDelete_composite_pk_join_source_deletes_only_matching_rows_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider, Options()))
        {
            await SetupAsync(ctx, kind);
            try
            {
                await ctx.Query<LbcOrder>()
                    .Join(ctx.Query<LbcStore>().Where(s => s.Active == 0),
                          o => o.StoreId, s => s.Id,
                          (o, s) => o)
                    .ExecuteDeleteAsync();

                var remaining = await ctx.Query<LbcOrder>().ToListAsync();
                Assert.Equal(3, remaining.Count);
                Assert.DoesNotContain(remaining, o => o.StoreId == 2);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
