#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Identifier, database-name, and generated member naming scaffold tests.

    [Fact]
    public async Task ScaffoldAsync_WithInvalidSqlIdentifiers_GeneratesValidCSharpIdentifiers()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "bad-table" (
                "1st-name" TEXT NOT NULL,
                "has space" INTEGER NULL,
                "class" TEXT NULL
            )
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "MyCtx3");

            var entityCode = File.ReadAllText(Path.Combine(dir, "BadTable.cs"));
            Assert.Contains("public partial class BadTable", entityCode);
            Assert.Contains("[Required]", entityCode);
            Assert.Contains("public string _1stName { get; set; } = default!;", entityCode);
            Assert.Contains("public long? HasSpace", entityCode);
            Assert.Contains("public string? Class", entityCode);
            Assert.DoesNotContain("@1st-name", entityCode);
            Assert.DoesNotContain("Has space", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithUseDatabaseNames_PreservesLegalDatabaseNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE customer (
                customer_id INTEGER PRIMARY KEY,
                display_name TEXT NOT NULL
            );
            CREATE TABLE order_line (
                order_id INTEGER PRIMARY KEY,
                billing_customer_id INTEGER NOT NULL REFERENCES customer(customer_id),
                shipping_customer_id INTEGER NULL REFERENCES customer(customer_id),
                SKU TEXT NOT NULL,
                "class" TEXT NULL,
                "has space" TEXT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(
                cn,
                new SqliteProvider(),
                dir,
                "TestNs",
                "DatabaseNamesCtx",
                new ScaffoldOptions { UseDatabaseNames = true });

            var entityPath = Path.Combine(dir, "order_line.cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var customerCode = File.ReadAllText(Path.Combine(dir, "customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "DatabaseNamesCtx.cs"));

            Assert.Contains("public partial class order_line", entityCode);
            Assert.Contains("public long order_id { get; set; }", entityCode);
            Assert.Contains("public long billing_customer_id { get; set; }", entityCode);
            Assert.Contains("public long? shipping_customer_id { get; set; }", entityCode);
            Assert.Contains("public string SKU { get; set; } = default!;", entityCode);
            Assert.Contains("public string? @class { get; set; }", entityCode);
            Assert.Contains("public string? has_space { get; set; }", entityCode);
            Assert.Contains("public customer BillingCustomer { get; set; } = default!;", entityCode);
            Assert.Contains("public customer? ShippingCustomer { get; set; }", entityCode);
            Assert.Contains("public List<order_line> OrderLinesByBillingCustomerId { get; set; } = new();", customerCode);
            Assert.Contains("public List<order_line> OrderLinesByShippingCustomerId { get; set; } = new();", customerCode);
            Assert.Contains("IQueryable<customer> customers", contextCode);
            Assert.Contains("IQueryable<order_line> order_lines", contextCode);
            Assert.Contains(".HasMany(p => p.OrderLinesByBillingCustomerId)", contextCode);
            Assert.Contains(".WithOne(d => d.BillingCustomer)", contextCode);
            Assert.Contains(".HasMany(p => p.OrderLinesByShippingCustomerId)", contextCode);
            Assert.Contains(".WithOne(d => d.ShippingCustomer)", contextCode);
            Assert.DoesNotContain("OrderLine", entityCode);
            Assert.DoesNotContain("OrderId", entityCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithInvalidContextName_GeneratesValidContextClass()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ContextNameEntity (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "1-bad context");

            var contextPath = Path.Combine(dir, "_1BadContext.cs");
            Assert.True(File.Exists(contextPath));
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("public partial class _1BadContext : DbContext", contextCode);
            Assert.Contains("public _1BadContext(DbConnection cn, DatabaseProvider provider, DbContextOptions? options = null)", contextCode);
            Assert.DoesNotContain("1-bad context", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithQuotedSqlIdentifiers_GeneratesEscapedSourceLiterals()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "Quoted""Back\Table" (
                "Id" INTEGER PRIMARY KEY,
                "bad""col\name<&>
            line" TEXT NOT NULL
            );
            CREATE INDEX "IX""Back\Name
            Line" ON "Quoted""Back\Table" ("bad""col\name<&>
            line");
            """.ReplaceLineEndings("\n");
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "QuotedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "QuotedBackTable.cs"));
            Assert.Contains("[Table(\"Quoted\\\"Back\\\\Table\")]", entityCode);
            Assert.Contains("[Column(\"bad\\\"col\\\\name<&>\\nline\")]", entityCode);
            Assert.Contains("[Index(\"IX\\\"Back\\\\Name\\nLine\")]", entityCode);
            Assert.Contains("Maps to column bad\"col\\name&lt;&amp;&gt;\\nline", entityCode);
            Assert.DoesNotContain("\nline\" TEXT", entityCode, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithLiteralDottedIdentifiers_GeneratesSingleIdentifierMappings()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "audit.events" (
                Id INTEGER PRIMARY KEY,
                "value.part" TEXT NOT NULL
            );
            CREATE INDEX "ix.audit.value" ON "audit.events" ("value.part");
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "DottedCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "AuditEvent.cs"));
            Assert.Contains("[Table(\"audit.events\")]", entityCode);
            Assert.Contains("[Index(\"ix.audit.value\")]", entityCode);
            Assert.Contains("[Column(\"value.part\")]", entityCode);
            Assert.Contains("public string ValuePart { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithIdentifierCollisions_GeneratesUniqueNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE "sales-order" (
                Id INTEGER PRIMARY KEY,
                "first-name" TEXT NOT NULL,
                "first_name" TEXT NOT NULL
            );
            CREATE TABLE "sales_order" (
                Id INTEGER PRIMARY KEY,
                Value TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CollisionCtx");

            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "SalesOrder2.cs")));
            var firstEntityCode = File.ReadAllText(Path.Combine(dir, "SalesOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CollisionCtx.cs"));

            Assert.Contains("public string FirstName { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public string FirstName2 { get; set; } = default!;", firstEntityCode);
            Assert.Contains("public IQueryable<SalesOrder> SalesOrders", contextCode);
            Assert.Contains("public IQueryable<SalesOrder2> SalesOrder2s", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithObjectMemberColumnNames_GeneratesUniquePropertyNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObjectMembers (
                Id INTEGER PRIMARY KEY,
                ToString TEXT NOT NULL,
                Equals TEXT NOT NULL,
                GetHashCode TEXT NOT NULL,
                GetType TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ObjectMemberCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "ObjectMember.cs"));
            Assert.Contains("public partial class ObjectMember", entityCode);
            Assert.Contains("public string ToString2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string Equals2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetHashCode2 { get; set; } = default!;", entityCode);
            Assert.Contains("public string GetType2 { get; set; } = default!;", entityCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithContextMemberEntityNames_GeneratesUniqueQueryPropertyNames()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE Option (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE ConfigureOption (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ContextMemberCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "ContextMemberCtx.cs"));
            Assert.Contains("public IQueryable<Option> Options2", contextCode);
            Assert.Contains("public IQueryable<ConfigureOption> ConfigureOptions2", contextCode);
            Assert.DoesNotContain("public IQueryable<Option> Options =>", contextCode);
            Assert.DoesNotContain("public IQueryable<ConfigureOption> ConfigureOptions =>", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
