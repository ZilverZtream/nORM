using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;
using CrrAppointment = nORM.Tests.LinqCommercialRiskReportStressTests.CrrAppointment;
using CrrClinic = nORM.Tests.LinqCommercialRiskReportStressTests.CrrClinic;
using CrrCustomer = nORM.Tests.LinqCommercialRiskReportStressTests.CrrCustomer;
using CrrInsuranceClaim = nORM.Tests.LinqCommercialRiskReportStressTests.CrrInsuranceClaim;
using CrrInventoryItem = nORM.Tests.LinqCommercialRiskReportStressTests.CrrInventoryItem;
using CrrOrder = nORM.Tests.LinqCommercialRiskReportStressTests.CrrOrder;
using CrrOrderLine = nORM.Tests.LinqCommercialRiskReportStressTests.CrrOrderLine;
using CrrPrescription = nORM.Tests.LinqCommercialRiskReportStressTests.CrrPrescription;
using CrrProduct = nORM.Tests.LinqCommercialRiskReportStressTests.CrrProduct;
using CrrSupplierProduct = nORM.Tests.LinqCommercialRiskReportStressTests.CrrSupplierProduct;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderCommercialRiskReportParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Advanced_customer_commercial_risk_report_runs_on_live_provider(ProviderKind kind)
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
                await LinqCommercialRiskReportStressTests.AssertAdvancedCustomerCommercialRiskReportAsync(ctx);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }

    private static DbContextOptions Options() => new()
    {
        // The report query joins a dozen sources; its estimated cost exceeds the
        // memory-scaled admission ceiling on small machines, so pin explicit limits
        // to keep the test deterministic across environments.
        MaxQueryJoinDepth = 1_000,
        MaxQueryWhereConditions = 10_000,
        MaxQueryComplexityCost = 1_000_000,
        OnModelCreating = mb =>
        {
            mb.Entity<CrrCustomer>().HasKey(x => x.Id).HasMany(x => x.Orders).WithOne().HasForeignKey(x => x.CustomerId, x => x.Id);
            mb.Entity<CrrCustomer>().HasMany(x => x.Appointments).WithOne().HasForeignKey(x => x.CustomerId, x => x.Id);
            mb.Entity<CrrCustomer>().HasMany(x => x.Prescriptions).WithOne().HasForeignKey(x => x.CustomerId, x => x.Id);
            mb.Entity<CrrOrder>().HasKey(x => x.Id).HasMany(x => x.Lines).WithOne().HasForeignKey(x => x.OrderId, x => x.Id);
            mb.Entity<CrrProduct>().HasKey(x => x.Id);
            mb.Entity<CrrClinic>().HasKey(x => x.Id);
            mb.Entity<CrrInsuranceClaim>().HasKey(x => x.Id);
            mb.Entity<CrrInventoryItem>().HasKey(x => x.Id);
            mb.Entity<CrrSupplierProduct>().HasKey(x => x.Id);
        }
    };

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        await TeardownAsync(ctx, kind);

        var id = ctx.Provider.Escape("Id");
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        var stringType = kind == ProviderKind.Sqlite ? "TEXT" : kind == ProviderKind.SqlServer ? "NVARCHAR(128)" : "VARCHAR(128)";
        var dateType = kind == ProviderKind.Sqlite ? "TEXT" : kind == ProviderKind.SqlServer ? "DATETIME2" : kind == ProviderKind.MySql ? "DATETIME" : "TIMESTAMP";
        var decimalType = kind == ProviderKind.Sqlite ? "TEXT" : "DECIMAL(18,6)";
        var boolType = kind == ProviderKind.Sqlite ? "INTEGER" : kind == ProviderKind.SqlServer ? "BIT" : "BOOLEAN";
        var trueLiteral = kind == ProviderKind.Postgres || kind == ProviderKind.MySql ? "TRUE" : "1";
        var falseLiteral = kind == ProviderKind.Postgres || kind == ProviderKind.MySql ? "FALSE" : "0";

        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrCustomer")} ({id} {intType} PRIMARY KEY, {E(ctx, "CustomerNo")} {stringType} NOT NULL, {E(ctx, "FirstName")} {stringType} NOT NULL, {E(ctx, "LastName")} {stringType} NOT NULL, {E(ctx, "Email")} {stringType} NOT NULL, {E(ctx, "City")} {stringType} NOT NULL, {E(ctx, "Country")} {stringType} NOT NULL, {E(ctx, "RegisteredAt")} {dateType} NOT NULL, {E(ctx, "IsVip")} {boolType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrClinic")} ({id} {intType} PRIMARY KEY, {E(ctx, "Name")} {stringType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrProduct")} ({id} {intType} PRIMARY KEY, {E(ctx, "Sku")} {stringType} NOT NULL, {E(ctx, "Name")} {stringType} NOT NULL, {E(ctx, "Category")} {stringType} NOT NULL, {E(ctx, "Brand")} {stringType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrOrder")} ({id} {intType} PRIMARY KEY, {E(ctx, "CustomerId")} {intType} NOT NULL, {E(ctx, "ClinicId")} {intType} NOT NULL, {E(ctx, "Status")} {stringType} NOT NULL, {E(ctx, "CreatedAt")} {dateType} NOT NULL, {E(ctx, "DiscountAmount")} {decimalType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrOrderLine")} ({id} {intType} PRIMARY KEY, {E(ctx, "OrderId")} {intType} NOT NULL, {E(ctx, "ProductId")} {intType} NOT NULL, {E(ctx, "UnitPrice")} {decimalType} NOT NULL, {E(ctx, "UnitCost")} {decimalType} NOT NULL, {E(ctx, "Quantity")} {intType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrAppointment")} ({id} {intType} PRIMARY KEY, {E(ctx, "CustomerId")} {intType} NOT NULL, {E(ctx, "Status")} {stringType} NOT NULL, {E(ctx, "Type")} {stringType} NOT NULL, {E(ctx, "StartsAt")} {dateType} NOT NULL, {E(ctx, "EndsAt")} {dateType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrPrescription")} ({id} {intType} PRIMARY KEY, {E(ctx, "CustomerId")} {intType} NOT NULL, {E(ctx, "IssuedAt")} {dateType} NOT NULL, {E(ctx, "OdSphere")} {decimalType} NOT NULL, {E(ctx, "OsSphere")} {decimalType} NOT NULL, {E(ctx, "OdCylinder")} {decimalType} NOT NULL, {E(ctx, "OsCylinder")} {decimalType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrInsuranceClaim")} ({id} {intType} PRIMARY KEY, {E(ctx, "OrderId")} {intType} NOT NULL, {E(ctx, "Status")} {stringType} NOT NULL, {E(ctx, "ClaimedAmount")} {decimalType} NOT NULL, {E(ctx, "ApprovedAmount")} {decimalType} NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrInventoryItem")} ({id} {intType} PRIMARY KEY, {E(ctx, "ProductId")} {intType} NOT NULL, {E(ctx, "QuantityOnHand")} {intType} NOT NULL, {E(ctx, "ReservedQuantity")} {intType} NOT NULL, {E(ctx, "ReorderPoint")} {intType} NOT NULL)");
        await ExecuteAsync(ctx, $"CREATE TABLE {E(ctx, "CrrSupplierProduct")} ({id} {intType} PRIMARY KEY, {E(ctx, "ProductId")} {intType} NOT NULL, {E(ctx, "LeadTimeDays")} {intType} NOT NULL, {E(ctx, "SupplierPrice")} {decimalType} NOT NULL)");

        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrCustomer")} VALUES (1,'C001','Ada','Lovelace','ada@example.test','London','UK','2024-01-01 00:00:00',{trueLiteral}),(2,'C002','Bob','Stone','bob@example.test','London','UK','2024-02-01 00:00:00',{falseLiteral})");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrClinic")} VALUES (1,'Central Clinic')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrProduct")} VALUES (1,'FR-1','Frame One','Frames','Acme'),(2,'LN-1','Lens One','Lenses','Acme'),(3,'CL-1','Contacts One','ContactLenses','ClearCo'),(4,'AC-1','Case One','Accessories','Acme')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrOrder")} VALUES (1,1,1,'Paid','2026-01-05 10:00:00','100.00'),(2,1,1,'Shipped','2026-02-10 10:00:00','50.00'),(3,1,1,'Cancelled','2026-03-01 10:00:00','0.00'),(4,1,1,'Refunded','2026-04-01 10:00:00','0.00'),(5,2,1,'Paid','2026-02-01 10:00:00','0.00')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrOrderLine")} VALUES (1,1,1,'1000.00','400.00',1),(2,1,2,'500.00','100.00',2),(3,2,3,'300.00','80.00',1),(4,2,4,'100.00','30.00',1),(5,5,2,'100.00','20.00',1)");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrAppointment")} VALUES (1,1,'Completed','Eye Exam','2026-01-01 09:00:00','2026-01-01 10:00:00'),(2,1,'NoShow','Eye Exam','2026-01-20 09:00:00','2026-01-20 10:00:00'),(3,1,'Booked','Followup','2099-01-01 09:00:00','2099-01-01 10:00:00')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrPrescription")} VALUES (1,1,'2026-01-01 10:00:00','6.50','1.00','1.00','2.25')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrInsuranceClaim")} VALUES (1,1,'Rejected','700.00',NULL),(2,2,'Approved','200.00','150.00')");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrInventoryItem")} VALUES (1,1,5,4,2),(2,2,50,5,10)");
        await ExecuteAsync(ctx, $"INSERT INTO {E(ctx, "CrrSupplierProduct")} VALUES (1,1,20,'350.00'),(2,1,21,'500.00'),(3,2,10,'100.00'),(4,2,11,'110.00')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        foreach (var table in new[]
        {
            "CrrSupplierProduct", "CrrInventoryItem", "CrrInsuranceClaim", "CrrPrescription", "CrrAppointment",
            "CrrOrderLine", "CrrOrder", "CrrProduct", "CrrClinic", "CrrCustomer"
        })
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
}
