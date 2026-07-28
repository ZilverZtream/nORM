#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A foreign key declared without an explicit parent column list — <c>customer_id INTEGER REFERENCES Customer</c>
/// — implicitly references the parent's primary key (standard, extremely common hand-written SQL). SQLite's
/// PRAGMA foreign_key_list reports such a key with a NULL "to", which the scaffolder converted to "" and then
/// dropped the row before it reached discovery: no reference navigation, no collection navigation, no
/// HasForeignKey, and not even a suppression warning. The principal column must be back-filled from the
/// parent's primary key so the relationship is scaffolded, matching EF Core.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldSqliteImplicitPrincipalKeyForeignKeyTests
{
    private static async Task<(string Principal, string Dependent, string Context)> ScaffoldAsync(string createSql)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = createSql;
            cmd.ExecuteNonQuery();
        }
        var dir = Path.Combine(Path.GetTempPath(), "scaffold_implicitfk_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ShopCtx");
            return (
                await File.ReadAllTextAsync(Path.Combine(dir, "Customer.cs")),
                await File.ReadAllTextAsync(Path.Combine(dir, "Purchase.cs")),
                await File.ReadAllTextAsync(Path.Combine(dir, "ShopCtx.cs")));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Implicit_parent_key_reference_scaffolds_the_relationship()
    {
        var (customer, purchase, context) = await ScaffoldAsync("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE Customer (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
            CREATE TABLE Purchase (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                CustomerId INTEGER NOT NULL REFERENCES Customer,
                Total TEXT NOT NULL);
            """);

        // The relationship must be discovered even though "REFERENCES Customer" omits the parent column.
        Assert.Contains("[ForeignKey(nameof(CustomerId))]", purchase);
        Assert.Contains("public Customer Customer", purchase);
        Assert.Contains("public List<Purchase> Purchases", customer);
        Assert.Contains(".HasForeignKey(d => d.CustomerId, p => p.Id", context);
    }

    [Fact]
    public async Task Composite_implicit_parent_key_reference_scaffolds_the_relationship()
    {
        var (_, purchase, context) = await ScaffoldAsync("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE Customer (RegionId INTEGER NOT NULL, Code INTEGER NOT NULL, Name TEXT NOT NULL,
                PRIMARY KEY (RegionId, Code));
            CREATE TABLE Purchase (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                CustRegionId INTEGER NOT NULL,
                CustCode INTEGER NOT NULL,
                Total TEXT NOT NULL,
                FOREIGN KEY (CustRegionId, CustCode) REFERENCES Customer);
            """);

        // The composite implicit form maps its columns positionally onto the parent's composite PK.
        Assert.Contains("public Customer Customer", purchase);
        Assert.Contains(".HasForeignKey(", context);
        Assert.Contains("CustRegionId", context);
        Assert.Contains("CustCode", context);
    }
}
