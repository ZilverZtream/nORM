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
/// SQLite's PRAGMA foreign_key_list returns the referenced ("to") column exactly as written in the
/// REFERENCES clause, which may differ in letter-casing from the parent column's declaration. The scaffolder
/// looked the principal column up case-sensitively and, on a miss, PascalCased the as-written name — emitting a
/// HasForeignKey principal-key selector (e.g. p =&gt; p.Customerid) that does not match the parent entity's
/// property (CustomerID), so the generated DbContext did not compile. The lookup must resolve case-insensitively.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldSqliteForeignKeyCaseMismatchTests
{
    [Fact]
    public async Task Case_mismatched_explicit_fk_reference_uses_the_parent_property_name()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Parent column declared "CustomerID"; the child's FK references it as "customerid" (different case).
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                CREATE TABLE Customer (CustomerID INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE Purchase (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrgId INTEGER NOT NULL,
                    FOREIGN KEY (OrgId) REFERENCES Customer(customerid)
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "scaffold_fkcase_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ShopCtx");

            var context = await File.ReadAllTextAsync(Path.Combine(dir, "ShopCtx.cs"));
            var customer = await File.ReadAllTextAsync(Path.Combine(dir, "Customer.cs"));

            // The parent property preserves the declared column casing.
            Assert.Contains("CustomerID", customer);
            // The principal-key selector must reference that ACTUAL property (p.CustomerID), not the PascalCased
            // as-written name "Customerid" (which the entity does not declare → CS1061 in the generated context).
            Assert.Contains(".HasForeignKey(", context);
            Assert.Contains("p.CustomerID", context);
            Assert.DoesNotContain("Customerid", context);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
