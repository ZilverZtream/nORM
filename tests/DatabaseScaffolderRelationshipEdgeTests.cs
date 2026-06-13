#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Relationship edge-case scaffold integration tests.

    [Fact]
    public async Task ScaffoldAsync_WithCompositeForeignKey_EmitsNavigationAndCompositeModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE TenantOrder (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId)
            );
            CREATE TABLE TenantOrderLine (
                TenantId INTEGER NOT NULL,
                OrderId INTEGER NOT NULL,
                LineNo INTEGER NOT NULL,
                Sku TEXT NOT NULL,
                PRIMARY KEY (TenantId, OrderId, LineNo),
                CONSTRAINT FK_Line_Order FOREIGN KEY (TenantId, OrderId) REFERENCES TenantOrder(TenantId, OrderId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "TenantOrder.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "TenantOrderLine.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeFkCtx.cs"));

            Assert.Contains("List<TenantOrderLine>", principalCode);
            Assert.Contains("public TenantOrder TenantOrder { get; set; } = default!;", dependentCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.OrderId }, p => new { p.TenantId, p.OrderId }, cascadeDelete: false);", contextCode);
            Assert.Contains("mb.Entity<TenantOrder>().HasKey(e => new { e.TenantId, e.OrderId });", contextCode);
            Assert.Contains("mb.Entity<TenantOrderLine>().HasKey(e => new { e.TenantId, e.OrderId, e.LineNo });", contextCode);
            Assert.Contains("[Key]", principalCode);
            Assert.Contains("TenantId { get; set; }", principalCode);
            Assert.Contains("OrderId { get; set; }", principalCode);
            Assert.Contains("[Key]", dependentCode);
            Assert.Contains("LineNo { get; set; }", dependentCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositePrimaryKey_UsesDeclaredKeyOrder()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OrderedKeyItem (
                TenantId INTEGER NOT NULL,
                LocalId INTEGER NOT NULL,
                Name TEXT NOT NULL,
                PRIMARY KEY (LocalId, TenantId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "OrderedKeyCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "OrderedKeyCtx.cs"));

            Assert.Contains("mb.Entity<OrderedKeyItem>().HasKey(e => new { e.LocalId, e.TenantId });", contextCode);
            Assert.DoesNotContain("mb.Entity<OrderedKeyItem>().HasKey(e => new { e.TenantId, e.LocalId });", contextCode);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithCompositeForeignKeyToUniqueIndex_EmitsNavigationAndModelConfig()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ExternalOrder (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                ExternalNo TEXT NOT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX UX_ExternalOrder_Tenant_ExternalNo ON ExternalOrder(TenantId, ExternalNo);
            CREATE TABLE ExternalOrderEvent (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                ExternalNo TEXT NOT NULL,
                EventName TEXT NOT NULL,
                CONSTRAINT FK_Event_Order FOREIGN KEY (TenantId, ExternalNo) REFERENCES ExternalOrder(TenantId, ExternalNo)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "CompositeUniqueFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "ExternalOrder.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "ExternalOrderEvent.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "CompositeUniqueFkCtx.cs"));

            Assert.Contains("[Index(\"UX_ExternalOrder_Tenant_ExternalNo\", IsUnique = true, Order = 0)]", principalCode);
            Assert.Contains("[Index(\"UX_ExternalOrder_Tenant_ExternalNo\", IsUnique = true, Order = 1)]", principalCode);
            Assert.Contains("List<ExternalOrderEvent>", principalCode);
            Assert.Contains("public ExternalOrder ExternalOrder { get; set; } = default!;", dependentCode);
            Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.ExternalNo }, p => new { p.TenantId, p.ExternalNo }, cascadeDelete: false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyToNullableUniqueIndex_EmitsAlternateKeyNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE NullableAltCustomer (
                Id INTEGER PRIMARY KEY,
                ExternalId TEXT NULL,
                Name TEXT NOT NULL
            );
            CREATE UNIQUE INDEX UX_NullableAltCustomer_ExternalId ON NullableAltCustomer(ExternalId);
            CREATE TABLE NullableAltOrder (
                Id INTEGER PRIMARY KEY,
                CustomerExternalId TEXT NOT NULL,
                CONSTRAINT FK_NullableAltOrder_Customer
                    FOREIGN KEY (CustomerExternalId) REFERENCES NullableAltCustomer(ExternalId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "NullableUniqueFkCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "NullableAltCustomer.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "NullableAltOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "NullableUniqueFkCtx.cs"));

            Assert.Contains("[Index(\"UX_NullableAltCustomer_ExternalId\", IsUnique = true)]", principalCode);
            Assert.Contains("public List<NullableAltOrder> NullableAltOrders { get; set; } = new();", principalCode);
            Assert.Contains("[ForeignKey(nameof(CustomerExternalId))]", dependentCode);
            Assert.Contains("public NullableAltCustomer NullableAltCustomer { get; set; } = default!;", dependentCode);
            Assert.Contains(".HasForeignKey(d => d.CustomerExternalId, p => p.ExternalId, cascadeDelete: false);", contextCode);
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyReferentialActions_GeneratesExplicitActions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE CascadeChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Cascade_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE CASCADE
            );
            CREATE TABLE RestrictChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NOT NULL,
                CONSTRAINT FK_Restrict_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE RESTRICT ON UPDATE CASCADE
            );
            CREATE TABLE SetNullChild (
                Id INTEGER PRIMARY KEY,
                ParentId INTEGER NULL,
                CONSTRAINT FK_SetNull_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id) ON DELETE SET NULL
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "FkActionCtx");

            var contextCode = File.ReadAllText(Path.Combine(dir, "FkActionCtx.cs"));
            var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");

            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, ReferentialAction.Restrict, ReferentialAction.Cascade);", contextCode);
            Assert.Contains(".HasForeignKey(d => d.ParentId, p => p.Id, ReferentialAction.SetNull, ReferentialAction.NoAction);", contextCode);
            Assert.DoesNotContain("sqlite_fk_", contextCode);
            Assert.False(File.Exists(warningPath), "Valid referential actions should scaffold into fluent configuration rather than warning-only diagnostics.");
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void BuildRelationships_WithUnknownReferentialAction_SuppressesRelationship()
    {
        var foreignKeys = new[]
        {
            new DatabaseScaffolder.ScaffoldForeignKey(
                null,
                "Child",
                "ParentId",
                null,
                "Parent",
                "Id",
                "FK_Child_Parent",
                1,
                "PROVIDER CASCADE",
                "NO ACTION",
                false)
        };

        var entityByTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Child"] = "Child",
            ["Parent"] = "Parent"
        };
        var columnProperties = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Child"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = "Id",
                ["ParentId"] = "ParentId"
            },
            ["Parent"] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = "Id"
            }
        };
        var primaryKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Child"] = new[] { "Id" },
            ["Parent"] = new[] { "Id" }
        };
        var nonNullableColumns = new Dictionary<string, IReadOnlySet<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["Child"] = new HashSet<string>(new[] { "Id", "ParentId" }, StringComparer.OrdinalIgnoreCase),
            ["Parent"] = new HashSet<string>(new[] { "Id" }, StringComparer.OrdinalIgnoreCase)
        };
        var memberNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var indexes = Array.Empty<DatabaseScaffolder.ScaffoldIndex>();
        var relationships = ScaffoldRelationshipAdapter.BuildRelationships(
            foreignKeys,
            entityByTable,
            columnProperties,
            primaryKeys,
            indexes,
            nonNullableColumns,
            memberNames);

        Assert.Empty(relationships);
    }

}
