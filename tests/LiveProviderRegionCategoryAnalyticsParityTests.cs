using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderRegionCategoryAnalyticsParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Region_category_window_style_analytics_runs_on_live_provider(ProviderKind kind)
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
                await AssertRegionCategoryAnalyticsAsync(ctx);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static async Task AssertRegionCategoryAnalyticsAsync(DbContext ctx)
    {
        var report =
            from cust in ctx.Query<RcaCustomer>()
            join ord in ctx.Query<RcaOrder>() on cust.Id equals ord.CustomerId
            join line in ctx.Query<RcaOrderLine>() on ord.Id equals line.OrderId
            join prod in ctx.Query<RcaProduct>() on line.ProductId equals prod.Id
            join cat in ctx.Query<RcaCategory>() on prod.CategoryId equals cat.Id
            let revenue = line.Qty * line.UnitPrice
            let margin = line.Qty * (line.UnitPrice - prod.UnitCost)
            group new { revenue, margin, line.Qty }
                by new { cust.Region, Category = cat.Name } into cell
            let agg = new
            {
                cell.Key.Region,
                cell.Key.Category,
                Revenue = cell.Sum(x => x.revenue),
                Margin = cell.Sum(x => x.margin),
                Units = cell.Sum(x => x.Qty),
            }
            group agg by agg.Region into region
            from c in region
            join tgt in ctx.Query<RcaRegionTarget>() on c.Region equals tgt.Region into tj
            from tgt in tj.DefaultIfEmpty()
            let regionRevenue = region.Sum(x => x.Revenue)
            let rank = region.Count(x => x.Revenue > c.Revenue) + 1
            let percentile = region.Count(x => x.Revenue <= c.Revenue) / (double)region.Count()
            let cumulative = region.Where(x => x.Revenue >= c.Revenue).Sum(x => x.Revenue)
            let target = tgt == null ? 0m : tgt.Target
            orderby c.Region, rank
            select new
            {
                c.Region,
                c.Category,
                c.Revenue,
                c.Margin,
                MarginPct = c.Revenue == 0 ? 0 : c.Margin / c.Revenue,
                ShareOfRegion = regionRevenue == 0 ? 0 : c.Revenue / regionRevenue,
                RankInRegion = rank,
                Percentile = percentile,
                CumulativeShare = regionRevenue == 0 ? 0 : cumulative / regionRevenue,
                Attainment = target == 0 ? (decimal?)null : regionRevenue / target,
                c.Units,
            };

        var rows = (await report.ToListAsync()).ToArray();
        Assert.Equal(8, rows.Length);
        Assert.Equal("North", rows[0].Region);
        Assert.Equal("Audio", rows[0].Category);
        Assert.Equal(335m, rows[0].Revenue);
        Assert.Equal(1, rows[0].RankInRegion);
        Assert.Equal("West", rows[^1].Region);
        Assert.Null(rows.Single(r => r.Region == "West" && r.Category == "Audio").Attainment);

        var monthly =
            from cust in ctx.Query<RcaCustomer>()
            join ord in ctx.Query<RcaOrder>() on cust.Id equals ord.CustomerId
            join line in ctx.Query<RcaOrderLine>() on ord.Id equals line.OrderId
            let month = new DateOnly(ord.Date.Year, ord.Date.Month, 1)
            let rev = line.Qty * line.UnitPrice
            group rev by new { cust.Region, Month = month } into mg
            select new { mg.Key.Region, mg.Key.Month, Revenue = mg.Sum() } into m
            group m by m.Region into rg
            from row in rg
            let cumulative = rg.Where(o => o.Month <= row.Month).Sum(o => o.Revenue)
            orderby row.Region, row.Month
            select new
            {
                row.Region,
                row.Month,
                row.Revenue,
                RunningTotal = cumulative,
                RunningShare = cumulative / rg.Sum(o => o.Revenue),
            };

        var months = (await monthly.ToListAsync()).ToArray();
        Assert.Equal(7, months.Length);
        Assert.Equal(new DateOnly(2025, 1, 1), months[0].Month);
        Assert.Equal(503m, months.Single(m => m.Region == "North" && m.Month == new DateOnly(2025, 1, 1)).Revenue);
        Assert.Equal(1138m, months.Single(m => m.Region == "South" && m.Month == new DateOnly(2025, 3, 1)).RunningTotal);
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<RcaCategory>().HasKey(x => x.Id);
            mb.Entity<RcaProduct>().HasKey(x => x.Id);
            mb.Entity<RcaCustomer>().HasKey(x => x.Id);
            mb.Entity<RcaOrder>().HasKey(x => x.Id);
            mb.Entity<RcaOrderLine>().HasKey(x => new { x.OrderId, x.ProductId });
            mb.Entity<RcaRegionTarget>().HasKey(x => x.Region);
        }
    };

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        await TeardownAsync(ctx, kind);

        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var stringType = kind == ProviderKind.Sqlite ? "TEXT" : kind == ProviderKind.SqlServer ? "NVARCHAR(64)" : "VARCHAR(64)";
        var dateType = kind == ProviderKind.Sqlite ? "TEXT" : "DATE";
        var decimalType = kind == ProviderKind.Sqlite ? "TEXT" : "DECIMAL(18,4)";

        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaCategory")} ({E(ctx, "Id")} {intType} PRIMARY KEY, {E(ctx, "Name")} {stringType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaProduct")} ({E(ctx, "Id")} {intType} PRIMARY KEY, {E(ctx, "Name")} {stringType} NOT NULL, {E(ctx, "CategoryId")} {intType} NOT NULL, {E(ctx, "UnitCost")} {decimalType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaCustomer")} ({E(ctx, "Id")} {intType} PRIMARY KEY, {E(ctx, "Name")} {stringType} NOT NULL, {E(ctx, "Region")} {stringType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaOrder")} ({E(ctx, "Id")} {intType} PRIMARY KEY, {E(ctx, "CustomerId")} {intType} NOT NULL, {E(ctx, "Date")} {dateType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaOrderLine")} ({E(ctx, "OrderId")} {intType} NOT NULL, {E(ctx, "ProductId")} {intType} NOT NULL, {E(ctx, "Qty")} {intType} NOT NULL, {E(ctx, "UnitPrice")} {decimalType} NOT NULL, PRIMARY KEY ({E(ctx, "OrderId")}, {E(ctx, "ProductId")}))");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "RcaRegionTarget")} ({E(ctx, "Region")} {stringType} PRIMARY KEY, {E(ctx, "Target")} {decimalType} NOT NULL)");

        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaCategory")} VALUES (1,'Audio'),(2,'Wearables'),(3,'Peripherals')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaProduct")} VALUES (10,'Headphones',1,'40'),(11,'Earbuds',1,'25'),(20,'Smartwatch',2,'90'),(21,'Fitness Band',2,'30'),(30,'Mechanical KB',3,'55'),(31,'Gaming Mouse',3,'20')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaCustomer")} VALUES (1,'Alice','North'),(2,'Bob','North'),(3,'Cleo','South'),(4,'Dmitri','South'),(5,'Esi','West')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaOrder")} VALUES (100,1,'2025-01-12'),(101,1,'2025-02-03'),(102,2,'2025-01-28'),(103,3,'2025-01-19'),(104,3,'2025-03-06'),(105,4,'2025-02-22'),(106,5,'2025-01-09'),(107,5,'2025-02-14')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaOrderLine")} VALUES (100,10,2,'79'),(100,31,1,'49'),(101,20,1,'199'),(102,11,3,'59'),(102,30,1,'119'),(103,21,4,'69'),(104,20,2,'199'),(104,10,1,'79'),(105,30,2,'119'),(105,31,3,'49'),(106,11,5,'59'),(107,21,2,'69'),(107,20,1,'199')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "RcaRegionTarget")} VALUES ('North','900'),('South','1500')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        foreach (var table in new[] { "RcaRegionTarget", "RcaOrderLine", "RcaOrder", "RcaCustomer", "RcaProduct", "RcaCategory" })
        {
            try
            {
                var sql = kind == ProviderKind.SqlServer
                    ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {E(ctx, table)}"
                    : $"DROP TABLE IF EXISTS {E(ctx, table)}";
                await ExecuteAsync(ctx, sql);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    private static string E(DbContext ctx, string identifier) => ctx.Provider.Escape(identifier);

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Table("RcaCategory")]
    public sealed class RcaCategory
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("RcaProduct")]
    public sealed class RcaProduct
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int CategoryId { get; set; }
        public decimal UnitCost { get; set; }
    }

    [Table("RcaCustomer")]
    public sealed class RcaCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Region { get; set; } = "";
    }

    [Table("RcaOrder")]
    public sealed class RcaOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public DateOnly Date { get; set; }
    }

    [Table("RcaOrderLine")]
    public sealed class RcaOrderLine
    {
        [Key] public int OrderId { get; set; }
        [Key] public int ProductId { get; set; }
        public int Qty { get; set; }
        public decimal UnitPrice { get; set; }
    }

    [Table("RcaRegionTarget")]
    public sealed class RcaRegionTarget
    {
        [Key] public string Region { get; set; } = "";
        public decimal Target { get; set; }
    }
}
