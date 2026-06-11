#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public partial class DatabaseScaffolderPrivateMethodTests
{
    // Keyless entity, read-only write guard, and unsafe-navigation scaffold safety tests.

    [Fact]
    public async Task ScaffoldAsync_WithKeylessTable_EmitsMissingPrimaryKeyDiagnostic()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE KeylessImport (ExternalId TEXT NOT NULL, Payload TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessCtx");

            var entityCode = File.ReadAllText(Path.Combine(dir, "KeylessImport.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("using nORM.Configuration;", entityCode);
            Assert.Contains("[ReadOnlyEntity]", entityCode);
            Assert.DoesNotContain("[Key]", entityCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("KeylessImport", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            var missingPrimaryKey = Assert.Single(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "KeylessImport");
            Assert.Contains("read", missingPrimaryKey.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
            var metadata = missingPrimaryKey.GetProperty("metadata");
            Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
            Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
            Assert.False(metadata.GetProperty("generatedNavigationSupported").GetBoolean());
            Assert.Equal("KeylessImport", metadata.GetProperty("table").GetString());
            Assert.Equal("missing-primary-key", metadata.GetProperty("reason").GetString());
            Assert.Equal(2, metadata.GetProperty("columnCount").GetInt32());
            Assert.Equal(new[] { "ExternalId", "Payload" }, metadata.GetProperty("columns").EnumerateArray().Select(item => item.GetString()).ToArray());
            Assert.Equal(new[] { "ExternalId", "Payload" }, metadata.GetProperty("properties").EnumerateArray().Select(item => item.GetString()).ToArray());
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ReadOnlyEntity_WritePathsThrowBeforeSqlGeneration()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var item = new SanReadOnlyReport { ExternalId = "ext-1", Payload = "payload" };

        var add = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Add(item));
        Assert.Contains("read-only", add.Message, StringComparison.OrdinalIgnoreCase);

        var update = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Update(item));
        Assert.Contains("read-only", update.Message, StringComparison.OrdinalIgnoreCase);

        var remove = Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Remove(item));
        Assert.Contains("read-only", remove.Message, StringComparison.OrdinalIgnoreCase);

        var insert = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.InsertAsync(item));
        Assert.Contains("read-only", insert.Message, StringComparison.OrdinalIgnoreCase);

        var executeUpdate = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<SanReadOnlyReport>().ExecuteUpdateAsync(setters => setters.SetProperty(r => r.Payload, "changed")));
        Assert.Contains("read-only", executeUpdate.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ExecuteUpdateAsync", executeUpdate.Message, StringComparison.Ordinal);

        var executeDelete = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<SanReadOnlyReport>().ExecuteDeleteAsync());
        Assert.Contains("read-only", executeDelete.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ExecuteDeleteAsync", executeDelete.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyToKeylessPrincipal_DoesNotEmitUnsafeNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE ExternalCustomer (
                ExternalId TEXT NOT NULL UNIQUE,
                Name TEXT NOT NULL
            );
            CREATE TABLE ImportedOrder (
                Id INTEGER PRIMARY KEY,
                CustomerExternalId TEXT NOT NULL,
                CONSTRAINT FK_ImportedOrder_ExternalCustomer
                    FOREIGN KEY (CustomerExternalId) REFERENCES ExternalCustomer(ExternalId)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessRelCtx");

            var principalCode = File.ReadAllText(Path.Combine(dir, "ExternalCustomer.cs"));
            var dependentCode = File.ReadAllText(Path.Combine(dir, "ImportedOrder.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "KeylessRelCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("[Key]", principalCode);
            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("RelationshipPrincipalKey", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "RelationshipPrincipalKey" &&
                item.GetProperty("name").GetString() == "sqlite_fk_0" &&
                item.GetProperty("metadata").GetProperty("navigationSuppressed").GetBoolean() &&
                item.GetProperty("metadata").GetProperty("dependentTable").GetString() == "ImportedOrder" &&
                item.GetProperty("metadata").GetProperty("dependentColumns").EnumerateArray().Single().GetString() == "CustomerExternalId" &&
                item.GetProperty("metadata").GetProperty("principalTable").GetString() == "ExternalCustomer" &&
                item.GetProperty("metadata").GetProperty("principalColumns").EnumerateArray().Single().GetString() == "ExternalId" &&
                item.GetProperty("metadata").GetProperty("columnCount").GetInt32() == 1);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyFromKeylessDependent_DoesNotEmitUnsafeNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Customer (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE ImportLine (
                CustomerId INTEGER NOT NULL,
                Payload TEXT NOT NULL,
                CONSTRAINT FK_ImportLine_Customer
                    FOREIGN KEY (CustomerId) REFERENCES Customer(Id)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "KeylessDependentRelCtx");

            var dependentCode = File.ReadAllText(Path.Combine(dir, "ImportLine.cs"));
            var principalCode = File.ReadAllText(Path.Combine(dir, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "KeylessDependentRelCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[ReadOnlyEntity]", dependentCode);
            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("ImportLines", principalCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("RelationshipDependentKey", warnings);
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
            Assert.Contains(providerOwned.EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                item.GetProperty("table").GetString() == "ImportLine" &&
                item.GetProperty("metadata").GetProperty("navigationSuppressed").GetBoolean() &&
                !item.GetProperty("metadata").GetProperty("generatedNavigationSupported").GetBoolean() &&
                item.GetProperty("metadata").GetProperty("dependentTable").GetString() == "ImportLine" &&
                item.GetProperty("metadata").GetProperty("dependentColumns").EnumerateArray().Single().GetString() == "CustomerId" &&
                item.GetProperty("metadata").GetProperty("principalTable").GetString() == "Customer" &&
                item.GetProperty("metadata").GetProperty("principalColumns").EnumerateArray().Single().GetString() == "Id" &&
                item.GetProperty("metadata").GetProperty("columnCount").GetInt32() == 1);
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_WithForeignKeyToReorderedUniqueIndex_DoesNotEmitUnsafeNavigation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            PRAGMA foreign_keys=ON;
            CREATE TABLE Principal (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                UNIQUE (Code, TenantId)
            );
            CREATE TABLE Dependent (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                CONSTRAINT FK_Dependent_Principal
                    FOREIGN KEY (TenantId, Code) REFERENCES Principal(TenantId, Code)
            );
            """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ReorderedUniqueRelCtx");

            var dependentCode = File.ReadAllText(Path.Combine(dir, "Dependent.cs"));
            var contextCode = File.ReadAllText(Path.Combine(dir, "ReorderedUniqueRelCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

            Assert.DoesNotContain("[ForeignKey(", dependentCode);
            Assert.DoesNotContain("HasForeignKey", contextCode);
            Assert.Contains("RelationshipPrincipalKey", warnings);
            var compositeFk = warningJson.RootElement.GetProperty("compositeForeignKeys").EnumerateArray().Single();
            var compositeMetadata = compositeFk.GetProperty("metadata");
            Assert.Equal("SCF001", compositeFk.GetProperty("code").GetString());
            Assert.Equal("relationship", compositeFk.GetProperty("category").GetString());
            Assert.Equal("Dependent", compositeMetadata.GetProperty("dependentTable").GetString());
            Assert.Equal("Principal", compositeMetadata.GetProperty("principalTable").GetString());
            Assert.True(compositeMetadata.GetProperty("relationshipSuppressed").GetBoolean());
            Assert.Equal("principal-key-not-scaffoldable", compositeMetadata.GetProperty("reason").GetString());
            Assert.Equal(2, compositeMetadata.GetProperty("columnCount").GetInt32());
            Assert.Equal(new[] { "TenantId", "Code" }, compositeMetadata.GetProperty("dependentColumns").EnumerateArray().Select(column => column.GetString()).ToArray());
            Assert.Equal(new[] { "TenantId", "Code" }, compositeMetadata.GetProperty("principalColumns").EnumerateArray().Select(column => column.GetString()).ToArray());
            Assert.Equal(new[] { "Id" }, compositeMetadata.GetProperty("principalPrimaryKeyColumns").EnumerateArray().Select(column => column.GetString()).ToArray());
            Assert.False(compositeMetadata.GetProperty("referencesPrimaryKey").GetBoolean());
            Assert.False(compositeMetadata.GetProperty("referencesScaffoldableUniqueIndex").GetBoolean());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "RelationshipPrincipalKey" &&
                item.GetProperty("name").GetString() == "sqlite_fk_0" &&
                item.GetProperty("metadata").GetProperty("reason").GetString() == "principal-key-not-scaffoldable" &&
                string.Join(",", item.GetProperty("metadata").GetProperty("dependentColumns").EnumerateArray().Select(column => column.GetString())) == "TenantId,Code" &&
                string.Join(",", item.GetProperty("metadata").GetProperty("principalColumns").EnumerateArray().Select(column => column.GetString())) == "TenantId,Code");
            AssertScaffoldOutputBuildsAsConsumerProject(dir);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task ScaffoldAsync_FailOnWarnings_WritesDiagnosticsThenThrows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE WarningOwned (
                    Status TEXT NOT NULL DEFAULT 'new'
                );
                """;
        cmd.ExecuteNonQuery();

        var dir = Path.Combine(Path.GetTempPath(), "san_scaffold_" + Guid.NewGuid().ToString("N"));
        try
        {
            var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                DatabaseScaffolder.ScaffoldAsync(
                    cn,
                    new SqliteProvider(),
                    dir,
                    "TestNs",
                    "WarningOwnedCtx",
                    new ScaffoldOptions { FailOnWarnings = true }));

            Assert.Contains("Scaffolding produced warnings", ex.Message);
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwned.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "WarningOwnedCtx.cs")));
            Assert.True(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            var warnings = File.ReadAllText(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
            Assert.Contains("MissingPrimaryKey", warnings);
            Assert.Contains("WarningOwned", warnings);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

}
