using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class LinqCommercialRiskReportStressTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CrrCustomer (
                Id INTEGER PRIMARY KEY, CustomerNo TEXT NOT NULL, FirstName TEXT NOT NULL, LastName TEXT NOT NULL,
                Email TEXT NOT NULL, City TEXT NOT NULL, Country TEXT NOT NULL, RegisteredAt TEXT NOT NULL, IsVip INTEGER NOT NULL);
            CREATE TABLE CrrClinic (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE CrrOrder (
                Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, ClinicId INTEGER NOT NULL, Status TEXT NOT NULL,
                CreatedAt TEXT NOT NULL, DiscountAmount TEXT NOT NULL);
            CREATE TABLE CrrOrderLine (
                Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, ProductId INTEGER NOT NULL,
                UnitPrice TEXT NOT NULL, UnitCost TEXT NOT NULL, Quantity INTEGER NOT NULL);
            CREATE TABLE CrrAppointment (
                Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Status TEXT NOT NULL, Type TEXT NOT NULL,
                StartsAt TEXT NOT NULL, EndsAt TEXT NOT NULL);
            CREATE TABLE CrrPrescription (
                Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, IssuedAt TEXT NOT NULL,
                OdSphere TEXT NOT NULL, OsSphere TEXT NOT NULL, OdCylinder TEXT NOT NULL, OsCylinder TEXT NOT NULL);
            CREATE TABLE CrrProduct (
                Id INTEGER PRIMARY KEY, Sku TEXT NOT NULL, Name TEXT NOT NULL, Category TEXT NOT NULL, Brand TEXT NOT NULL);
            CREATE TABLE CrrInsuranceClaim (
                Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Status TEXT NOT NULL, ClaimedAmount TEXT NOT NULL, ApprovedAmount TEXT NULL);
            CREATE TABLE CrrInventoryItem (
                Id INTEGER PRIMARY KEY, ProductId INTEGER NOT NULL, QuantityOnHand INTEGER NOT NULL, ReservedQuantity INTEGER NOT NULL, ReorderPoint INTEGER NOT NULL);
            CREATE TABLE CrrSupplierProduct (
                Id INTEGER PRIMARY KEY, ProductId INTEGER NOT NULL, LeadTimeDays INTEGER NOT NULL, SupplierPrice TEXT NOT NULL);

            INSERT INTO CrrCustomer VALUES
                (1, 'C001', 'Ada', 'Lovelace', 'ada@example.test', 'London', 'UK', '2024-01-01 00:00:00', 1),
                (2, 'C002', 'Bob', 'Stone', 'bob@example.test', 'London', 'UK', '2024-02-01 00:00:00', 0);
            INSERT INTO CrrClinic VALUES (1, 'Central Clinic');
            INSERT INTO CrrProduct VALUES
                (1, 'FR-1', 'Frame One', 'Frames', 'Acme'),
                (2, 'LN-1', 'Lens One', 'Lenses', 'Acme'),
                (3, 'CL-1', 'Contacts One', 'ContactLenses', 'ClearCo'),
                (4, 'AC-1', 'Case One', 'Accessories', 'Acme');
            INSERT INTO CrrOrder VALUES
                (1, 1, 1, 'Paid', '2026-01-05 10:00:00', '100.00'),
                (2, 1, 1, 'Shipped', '2026-02-10 10:00:00', '50.00'),
                (3, 1, 1, 'Cancelled', '2026-03-01 10:00:00', '0.00'),
                (4, 1, 1, 'Refunded', '2026-04-01 10:00:00', '0.00'),
                (5, 2, 1, 'Paid', '2026-02-01 10:00:00', '0.00');
            INSERT INTO CrrOrderLine VALUES
                (1, 1, 1, '1000.00', '400.00', 1),
                (2, 1, 2, '500.00', '100.00', 2),
                (3, 2, 3, '300.00', '80.00', 1),
                (4, 2, 4, '100.00', '30.00', 1),
                (5, 5, 2, '100.00', '20.00', 1);
            INSERT INTO CrrAppointment VALUES
                (1, 1, 'Completed', 'Eye Exam', '2026-01-01 09:00:00', '2026-01-01 10:00:00'),
                (2, 1, 'NoShow', 'Eye Exam', '2026-01-20 09:00:00', '2026-01-20 10:00:00'),
                (3, 1, 'Booked', 'Followup', '2099-01-01 09:00:00', '2099-01-01 10:00:00');
            INSERT INTO CrrPrescription VALUES
                (1, 1, '2026-01-01 10:00:00', '6.50', '1.00', '1.00', '2.25');
            INSERT INTO CrrInsuranceClaim VALUES
                (1, 1, 'Rejected', '700.00', NULL),
                (2, 2, 'Approved', '200.00', '150.00');
            INSERT INTO CrrInventoryItem VALUES (1, 1, 5, 4, 2), (2, 2, 50, 5, 10);
            INSERT INTO CrrSupplierProduct VALUES
                (1, 1, 20, '350.00'), (2, 1, 21, '500.00'),
                (3, 2, 10, '100.00'), (4, 2, 11, '110.00');
            """;
        await cmd.ExecuteNonQueryAsync();

        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
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
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Advanced_customer_commercial_risk_report_translates_and_executes()
    {
        await AssertAdvancedCustomerCommercialRiskReportAsync(_ctx);
    }

    internal static async Task AssertAdvancedCustomerCommercialRiskReportAsync(DbContext ctx)
    {
        var advancedCustomerCommercialRiskReport =
            ctx.Query<CrrCustomer>()
                .GroupJoin(
                    ctx.Query<CrrOrder>(),
                    customer => customer.Id,
                    order => order.CustomerId,
                    (customer, orders) => new { Customer = customer, Orders = orders })
                .GroupJoin(
                    ctx.Query<CrrAppointment>(),
                    x => x.Customer.Id,
                    appointment => appointment.CustomerId,
                    (x, appointments) => new { x.Customer, x.Orders, Appointments = appointments })
                .GroupJoin(
                    ctx.Query<CrrPrescription>(),
                    x => x.Customer.Id,
                    prescription => prescription.CustomerId,
                    (x, prescriptions) => new { x.Customer, x.Orders, x.Appointments, Prescriptions = prescriptions })
                .Select(x => new
                {
                    x.Customer,
                    CompletedOrders = x.Orders.Where(o => o.Status == "Paid" || o.Status == "Shipped"),
                    CancelledOrders = x.Orders.Where(o => o.Status == "Cancelled"),
                    RefundedOrders = x.Orders.Where(o => o.Status == "Refunded"),
                    CompletedAppointments = x.Appointments.Where(a => a.Status == "Completed"),
                    MissedAppointments = x.Appointments.Where(a => a.Status == "NoShow"),
                    FutureAppointments = x.Appointments.Where(a => a.Status == "Booked" && a.StartsAt >= DateTime.Today),
                    LatestPrescription = x.Prescriptions.OrderByDescending(p => p.IssuedAt).FirstOrDefault()
                })
                .Select(x => new
                {
                    x.Customer,
                    x.CompletedOrders,
                    x.CancelledOrders,
                    x.RefundedOrders,
                    x.CompletedAppointments,
                    x.MissedAppointments,
                    x.FutureAppointments,
                    x.LatestPrescription,
                    OrderCount = x.CompletedOrders.Count(),
                    CancelledOrderCount = x.CancelledOrders.Count(),
                    RefundedOrderCount = x.RefundedOrders.Count(),
                    TotalRevenue = x.CompletedOrders.SelectMany(o => o.Lines).Sum(l => l.UnitPrice * l.Quantity),
                    TotalCost = x.CompletedOrders.SelectMany(o => o.Lines).Sum(l => l.UnitCost * l.Quantity),
                    TotalDiscount = x.CompletedOrders.Sum(o => o.DiscountAmount),
                    TotalUnits = x.CompletedOrders.SelectMany(o => o.Lines).Sum(l => l.Quantity),
                    FirstOrderDate = x.CompletedOrders.Min(o => (DateTime?)o.CreatedAt),
                    LastOrderDate = x.CompletedOrders.Max(o => (DateTime?)o.CreatedAt),
                    AverageOrderValue = x.CompletedOrders.Any()
                        ? x.CompletedOrders.Average(o => o.Lines.Sum(l => l.UnitPrice * l.Quantity) - o.DiscountAmount)
                        : 0,
                    LargestSingleOrderValue = x.CompletedOrders.Any()
                        ? x.CompletedOrders.Max(o => o.Lines.Sum(l => l.UnitPrice * l.Quantity) - o.DiscountAmount)
                        : 0
                })
                .Select(x => new
                {
                    x.Customer,
                    x.CompletedOrders,
                    x.CompletedAppointments,
                    x.MissedAppointments,
                    x.FutureAppointments,
                    x.LatestPrescription,
                    x.OrderCount,
                    x.CancelledOrderCount,
                    x.RefundedOrderCount,
                    x.TotalRevenue,
                    x.TotalCost,
                    x.TotalDiscount,
                    x.TotalUnits,
                    x.FirstOrderDate,
                    x.LastOrderDate,
                    x.AverageOrderValue,
                    x.LargestSingleOrderValue,
                    NetRevenue = x.TotalRevenue - x.TotalDiscount,
                    GrossProfit = x.TotalRevenue - x.TotalCost - x.TotalDiscount,
                    MarginPercent = x.TotalRevenue == 0 ? 0 : Math.Round((x.TotalRevenue - x.TotalCost - x.TotalDiscount) / x.TotalRevenue * 100, 2),
                    CompletedAppointmentCount = x.CompletedAppointments.Count(),
                    MissedAppointmentCount = x.MissedAppointments.Count(),
                    NextAppointmentDate = x.FutureAppointments.Min(a => (DateTime?)a.StartsAt),
                    LatestPrescriptionDate = x.LatestPrescription == null ? null : (DateTime?)x.LatestPrescription.IssuedAt,
                    HasHighPrescription = x.LatestPrescription != null &&
                        (Math.Abs(x.LatestPrescription.OdSphere) >= 6 || Math.Abs(x.LatestPrescription.OsSphere) >= 6 ||
                         Math.Abs(x.LatestPrescription.OdCylinder) >= 2 || Math.Abs(x.LatestPrescription.OsCylinder) >= 2),
                    BoughtFrames = x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "Frames"),
                    BoughtLenses = x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "Lenses"),
                    BoughtContactLenses = x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "ContactLenses"),
                    BoughtAccessories = x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "Accessories"),
                    DistinctBrandCount = x.CompletedOrders.SelectMany(o => o.Lines).Select(l => l.Product.Brand).Distinct().Count(),
                    DistinctCategoryCount = x.CompletedOrders.SelectMany(o => o.Lines).Select(l => l.Product.Category).Distinct().Count(),
                    FavoriteBrand = x.CompletedOrders.SelectMany(o => o.Lines).GroupBy(l => l.Product.Brand).OrderByDescending(g => g.Sum(l => l.Quantity)).Select(g => g.Key).FirstOrDefault(),
                    FavoriteCategory = x.CompletedOrders.SelectMany(o => o.Lines).GroupBy(l => l.Product.Category).OrderByDescending(g => g.Sum(l => l.Quantity)).Select(g => g.Key).FirstOrDefault(),
                    LastClinicVisited = x.CompletedOrders.OrderByDescending(o => o.CreatedAt).Select(o => o.Clinic.Name).FirstOrDefault(),
                    InsuranceClaimCount = x.CompletedOrders.Count(o => o.InsuranceClaim != null),
                    RejectedInsuranceClaimCount = x.CompletedOrders.Count(o => o.InsuranceClaim != null && o.InsuranceClaim.Status == "Rejected"),
                    ClaimedInsuranceAmount = x.CompletedOrders.Where(o => o.InsuranceClaim != null).Sum(o => o.InsuranceClaim!.ClaimedAmount),
                    ApprovedInsuranceAmount = x.CompletedOrders.Where(o => o.InsuranceClaim != null).Sum(o => o.InsuranceClaim!.ApprovedAmount ?? 0)
                })
                .Select(x => new
                {
                    x.Customer.Id,
                    x.Customer.CustomerNo,
                    FullName = x.Customer.FirstName + " " + x.Customer.LastName,
                    x.Customer.Email,
                    x.Customer.City,
                    x.Customer.Country,
                    x.Customer.RegisteredAt,
                    x.Customer.IsVip,
                    x.OrderCount,
                    x.CancelledOrderCount,
                    x.RefundedOrderCount,
                    x.TotalRevenue,
                    x.TotalCost,
                    x.TotalDiscount,
                    x.NetRevenue,
                    x.GrossProfit,
                    x.MarginPercent,
                    x.TotalUnits,
                    x.AverageOrderValue,
                    x.LargestSingleOrderValue,
                    x.FirstOrderDate,
                    x.LastOrderDate,
                    x.CompletedAppointmentCount,
                    x.MissedAppointmentCount,
                    x.NextAppointmentDate,
                    x.LatestPrescriptionDate,
                    x.HasHighPrescription,
                    x.BoughtFrames,
                    x.BoughtLenses,
                    x.BoughtContactLenses,
                    x.BoughtAccessories,
                    x.DistinctBrandCount,
                    x.DistinctCategoryCount,
                    x.FavoriteBrand,
                    x.FavoriteCategory,
                    x.LastClinicVisited,
                    x.InsuranceClaimCount,
                    x.RejectedInsuranceClaimCount,
                    x.ClaimedInsuranceAmount,
                    x.ApprovedInsuranceAmount,
                    CityAverageRevenue = ctx.Query<CrrCustomer>()
                        .Where(other => other.City == x.Customer.City)
                        .Select(other => other.Orders.Where(o => o.Status == "Paid" || o.Status == "Shipped").SelectMany(o => o.Lines).Sum(l => l.UnitPrice * l.Quantity))
                        .Average(),
                    CountryAverageRevenue = ctx.Query<CrrCustomer>()
                        .Where(other => other.Country == x.Customer.Country)
                        .Select(other => other.Orders.Where(o => o.Status == "Paid" || o.Status == "Shipped").SelectMany(o => o.Lines).Sum(l => l.UnitPrice * l.Quantity))
                        .Average(),
                    HasCompleteOpticalJourney =
                        x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "Frames") &&
                        x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "Lenses") &&
                        x.CompletedOrders.SelectMany(o => o.Lines).Any(l => l.Product.Category == "ContactLenses"),
                    HasCompletedExamWithoutPurchase =
                        x.CompletedAppointments.Any(a => a.Type == "Eye Exam" &&
                            !x.CompletedOrders.Any(o => o.CreatedAt >= a.EndsAt && o.CreatedAt <= a.EndsAt.AddDays(30))),
                    BoughtProductsWithInventoryRisk = x.CompletedOrders
                        .SelectMany(o => o.Lines)
                        .Where(l => ctx.Query<CrrInventoryItem>().Any(i => i.ProductId == l.ProductId && i.QuantityOnHand - i.ReservedQuantity <= i.ReorderPoint))
                        .Select(l => new { l.ProductId, l.Product.Sku, l.Product.Name, l.Product.Category })
                        .Distinct()
                        .Count(),
                    BoughtProductsWithSupplierDelayRisk = x.CompletedOrders
                        .SelectMany(o => o.Lines)
                        .Where(l => ctx.Query<CrrSupplierProduct>().Where(sp => sp.ProductId == l.ProductId).Any() &&
                            ctx.Query<CrrSupplierProduct>().Where(sp => sp.ProductId == l.ProductId).All(sp => sp.LeadTimeDays >= 14))
                        .Select(l => l.ProductId)
                        .Distinct()
                        .Count(),
                    BoughtProductsWithSupplierPriceSpread = x.CompletedOrders
                        .SelectMany(o => o.Lines)
                        .Where(l => ctx.Query<CrrSupplierProduct>().Where(sp => sp.ProductId == l.ProductId).Count() >= 2 &&
                            ctx.Query<CrrSupplierProduct>().Where(sp => sp.ProductId == l.ProductId).Max(sp => sp.SupplierPrice) >
                            ctx.Query<CrrSupplierProduct>().Where(sp => sp.ProductId == l.ProductId).Min(sp => sp.SupplierPrice) * 1.25m)
                        .Select(l => l.ProductId)
                        .Distinct()
                        .Count()
                })
                .Select(x => new
                {
                    x.Id,
                    x.CustomerNo,
                    x.FullName,
                    x.Email,
                    x.City,
                    x.Country,
                    x.RegisteredAt,
                    x.IsVip,
                    x.OrderCount,
                    x.CancelledOrderCount,
                    x.RefundedOrderCount,
                    x.TotalRevenue,
                    x.TotalCost,
                    x.TotalDiscount,
                    x.NetRevenue,
                    x.GrossProfit,
                    x.MarginPercent,
                    x.TotalUnits,
                    x.AverageOrderValue,
                    x.LargestSingleOrderValue,
                    x.FirstOrderDate,
                    x.LastOrderDate,
                    x.CompletedAppointmentCount,
                    x.MissedAppointmentCount,
                    x.NextAppointmentDate,
                    x.LatestPrescriptionDate,
                    x.HasHighPrescription,
                    x.BoughtFrames,
                    x.BoughtLenses,
                    x.BoughtContactLenses,
                    x.BoughtAccessories,
                    x.HasCompleteOpticalJourney,
                    x.DistinctBrandCount,
                    x.DistinctCategoryCount,
                    x.FavoriteBrand,
                    x.FavoriteCategory,
                    x.LastClinicVisited,
                    x.InsuranceClaimCount,
                    x.RejectedInsuranceClaimCount,
                    x.ClaimedInsuranceAmount,
                    x.ApprovedInsuranceAmount,
                    x.CityAverageRevenue,
                    x.CountryAverageRevenue,
                    RevenueVsCityAverage = x.TotalRevenue - x.CityAverageRevenue,
                    RevenueVsCountryAverage = x.TotalRevenue - x.CountryAverageRevenue,
                    x.HasCompletedExamWithoutPurchase,
                    x.BoughtProductsWithInventoryRisk,
                    x.BoughtProductsWithSupplierDelayRisk,
                    x.BoughtProductsWithSupplierPriceSpread,
                    CommercialScore =
                        (x.IsVip ? 50 : 0)
                        + (x.TotalRevenue >= 50000 ? 100 : x.TotalRevenue >= 25000 ? 75 : x.TotalRevenue >= 10000 ? 50 : x.TotalRevenue >= 5000 ? 25 : 0)
                        + (x.HasCompleteOpticalJourney ? 30 : 0)
                        + (x.DistinctBrandCount >= 5 ? 20 : 0)
                        + (x.DistinctCategoryCount >= 4 ? 20 : 0)
                        + (x.AverageOrderValue >= 3000 ? 20 : 0)
                        + (x.HasHighPrescription ? 15 : 0),
                    RiskScore =
                        (x.CancelledOrderCount >= 3 ? 25 : 0)
                        + (x.RefundedOrderCount >= 2 ? 25 : 0)
                        + (x.MissedAppointmentCount >= 2 ? 20 : 0)
                        + (x.RejectedInsuranceClaimCount >= 1 ? 15 : 0)
                        + (x.TotalDiscount > x.TotalRevenue * 0.30m ? 20 : 0)
                        + (x.BoughtProductsWithInventoryRisk >= 2 ? 10 : 0)
                        + (x.BoughtProductsWithSupplierDelayRisk >= 1 ? 10 : 0)
                        + (x.BoughtProductsWithSupplierPriceSpread >= 1 ? 5 : 0)
                })
                .Select(x => new
                {
                    x.Id,
                    x.CustomerNo,
                    x.FullName,
                    x.Email,
                    x.City,
                    x.Country,
                    x.RegisteredAt,
                    x.IsVip,
                    x.OrderCount,
                    x.TotalRevenue,
                    x.NetRevenue,
                    x.GrossProfit,
                    x.MarginPercent,
                    x.AverageOrderValue,
                    x.LargestSingleOrderValue,
                    x.FirstOrderDate,
                    x.LastOrderDate,
                    x.NextAppointmentDate,
                    x.CompletedAppointmentCount,
                    x.MissedAppointmentCount,
                    x.LatestPrescriptionDate,
                    x.HasHighPrescription,
                    x.BoughtFrames,
                    x.BoughtLenses,
                    x.BoughtContactLenses,
                    x.BoughtAccessories,
                    x.HasCompleteOpticalJourney,
                    x.FavoriteBrand,
                    x.FavoriteCategory,
                    x.LastClinicVisited,
                    x.CityAverageRevenue,
                    x.CountryAverageRevenue,
                    x.RevenueVsCityAverage,
                    x.RevenueVsCountryAverage,
                    x.HasCompletedExamWithoutPurchase,
                    x.BoughtProductsWithInventoryRisk,
                    x.BoughtProductsWithSupplierDelayRisk,
                    x.BoughtProductsWithSupplierPriceSpread,
                    x.CommercialScore,
                    x.RiskScore,
                    ValueClass = x.CommercialScore >= 180 ? "Strategic Customer" :
                        x.CommercialScore >= 130 ? "High-Value Customer" :
                        x.CommercialScore >= 80 ? "Growth Customer" :
                        x.CommercialScore >= 40 ? "Standard Customer" :
                        "Low-Engagement Customer",
                    RiskClass = x.RiskScore >= 70 ? "High Risk" :
                        x.RiskScore >= 40 ? "Medium Risk" :
                        x.RiskScore >= 15 ? "Low Risk" :
                        "Normal",
                    CustomerSituation = x.HasCompletedExamWithoutPurchase ? "Exam completed but no purchase within 30 days" :
                        !x.BoughtFrames && x.BoughtLenses ? "Bought lenses but no frames" :
                        x.BoughtFrames && !x.BoughtLenses ? "Bought frames but no lenses" :
                        x.HasCompleteOpticalJourney ? "Complete optical journey" :
                        x.OrderCount == 0 && x.CompletedAppointmentCount > 0 ? "Appointment-only customer" :
                        x.OrderCount == 0 ? "Prospect" :
                        "General customer",
                    RecommendedAction = x.RiskScore >= 70 ? "Manual review before discount or insurance approval" :
                        x.HasCompletedExamWithoutPurchase ? "Follow up after eye exam" :
                        !x.BoughtFrames && x.BoughtLenses ? "Recommend frames" :
                        x.BoughtFrames && !x.BoughtLenses ? "Recommend lens package" :
                        x.HasHighPrescription ? "Offer premium lens consultation" :
                        x.BoughtProductsWithInventoryRisk > 0 ? "Prioritize stock planning before campaign" :
                        x.RevenueVsCityAverage > 5000 ? "Invite to VIP loyalty offer" :
                        "General retention campaign"
                })
                .OrderByDescending(x => x.CommercialScore)
                .ThenByDescending(x => x.GrossProfit)
                .ThenBy(x => x.RiskScore)
                .ThenBy(x => x.FullName)
                .Take(250);

        var result = await advancedCustomerCommercialRiskReport.ToListAsync();

        Assert.NotEmpty(result);
        Assert.Equal("Ada Lovelace", result[0].FullName);
        }

    [Table("CrrCustomer")]
    public sealed class CrrCustomer
    {
        [Key] public int Id { get; set; }
        public string CustomerNo { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
        public DateTime RegisteredAt { get; set; }
        public bool IsVip { get; set; }
        public List<CrrOrder> Orders { get; set; } = new();
        public List<CrrAppointment> Appointments { get; set; } = new();
        public List<CrrPrescription> Prescriptions { get; set; } = new();
    }

    [Table("CrrOrder")]
    public sealed class CrrOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int ClinicId { get; set; }
        public string Status { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public decimal DiscountAmount { get; set; }
        public List<CrrOrderLine> Lines { get; set; } = new();
        public CrrClinic Clinic { get; set; } = null!;
        public CrrInsuranceClaim? InsuranceClaim { get; set; }
    }

    [Table("CrrOrderLine")]
    public sealed class CrrOrderLine
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal UnitCost { get; set; }
        public int Quantity { get; set; }
        public CrrProduct Product { get; set; } = null!;
    }

    [Table("CrrAppointment")]
    public sealed class CrrAppointment
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Status { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public DateTime StartsAt { get; set; }
        public DateTime EndsAt { get; set; }
    }

    [Table("CrrPrescription")]
    public sealed class CrrPrescription
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public DateTime IssuedAt { get; set; }
        public decimal OdSphere { get; set; }
        public decimal OsSphere { get; set; }
        public decimal OdCylinder { get; set; }
        public decimal OsCylinder { get; set; }
    }

    [Table("CrrProduct")]
    public sealed class CrrProduct
    {
        [Key] public int Id { get; set; }
        public string Sku { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public string Brand { get; set; } = string.Empty;
    }

    [Table("CrrClinic")]
    public sealed class CrrClinic
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("CrrInsuranceClaim")]
    public sealed class CrrInsuranceClaim
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Status { get; set; } = string.Empty;
        public decimal ClaimedAmount { get; set; }
        public decimal? ApprovedAmount { get; set; }
    }

    [Table("CrrInventoryItem")]
    public sealed class CrrInventoryItem
    {
        [Key] public int Id { get; set; }
        public int ProductId { get; set; }
        public int QuantityOnHand { get; set; }
        public int ReservedQuantity { get; set; }
        public int ReorderPoint { get; set; }
    }

    [Table("CrrSupplierProduct")]
    public sealed class CrrSupplierProduct
    {
        [Key] public int Id { get; set; }
        public int ProductId { get; set; }
        public int LeadTimeDays { get; set; }
        public decimal SupplierPrice { get; set; }
    }
}
