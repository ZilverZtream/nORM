#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void AssertPossibleJoinPayloadDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                if (reason.GetString() == "payload-columns")
                    return;
            }
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic for {tableName}.");
    }

    private static void AssertPossibleJoinPayloadDiagnosticWithoutCompositeForeignKey(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            var hasPayloadReason = false;
            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                if (reasonText == "payload-columns")
                {
                    hasPayloadReason = true;
                    continue;
                }

                Assert.NotEqual("composite-foreign-key", reasonText);
            }

            if (hasPayloadReason)
                return;
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic without composite-FK rejection for {tableName}.");
    }

    private static void AssertPossibleManyToManyDiagnosticReason(string warningJsonPath, string tableName, params string[] expectedReasons)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                foreach (var expectedReason in expectedReasons)
                {
                    if (reasonText == expectedReason)
                        return;
                }
            }
        }

        Assert.Fail($"Expected possible many-to-many diagnostic for {tableName} with one of: {string.Join(", ", expectedReasons)}.");
    }

    private static void AssertRelationshipDependentKeyDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("suggestedAction").GetString()?.Contains("primary key", StringComparison.OrdinalIgnoreCase) == true)
            {
                return;
            }
        }

        Assert.Fail($"Expected relationship dependent-key diagnostic for {tableName}.");
    }

    private static void AssertRelationshipPrincipalKeyDiagnostic(
        string warningJsonPath,
        string dependentTable,
        string principalTable,
        string fkName,
        string dependentColumn,
        string principalColumn)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray())
        {
            if (!item.TryGetProperty("kind", out var kind) ||
                kind.GetString() != "RelationshipPrincipalKey" ||
                !item.TryGetProperty("metadata", out var metadata))
            {
                continue;
            }

            if (LastTableNameEquals(item.GetProperty("table").GetString(), dependentTable) &&
                item.GetProperty("name").GetString() == fkName &&
                item.GetProperty("suggestedAction").GetString()?.Contains("primary key", StringComparison.OrdinalIgnoreCase) == true &&
                metadata.GetProperty("navigationSuppressed").GetBoolean() &&
                !metadata.GetProperty("generatedNavigationSupported").GetBoolean() &&
                LastTableNameEquals(metadata.GetProperty("dependentTable").GetString(), dependentTable) &&
                metadata.GetProperty("dependentColumns").EnumerateArray().Single().GetString() == dependentColumn &&
                LastTableNameEquals(metadata.GetProperty("principalTable").GetString(), principalTable) &&
                metadata.GetProperty("principalColumns").EnumerateArray().Single().GetString() == principalColumn)
            {
                return;
            }
        }

        Assert.Fail($"Expected relationship principal-key diagnostic for {dependentTable}.{dependentColumn} -> {principalTable}.{principalColumn}.");
    }

    private static void AssertTriggerDiagnostic(string warningJsonPath, string tableName, string triggerName, ProviderKind kind)
    {
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        JsonElement? triggerDiagnostic = null;

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("code").GetString() == "SCF110" &&
                item.GetProperty("category").GetString() == "database-object" &&
                item.GetProperty("kind").GetString() == "Trigger" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == triggerName)
            {
                Assert.False(triggerDiagnostic.HasValue, "Expected exactly one trigger diagnostic.");
                triggerDiagnostic = item;
            }
        }

        Assert.True(triggerDiagnostic.HasValue, "Expected trigger diagnostic SCF110 in scaffold warning JSON.");
        var metadata = triggerDiagnostic.Value.GetProperty("metadata");
        Assert.Equal("Trigger", metadata.GetProperty("providerObjectKind").GetString());
        Assert.True(LastTableNameEquals(metadata.GetProperty("table").GetString(), tableName));
        Assert.Equal(triggerName, metadata.GetProperty("triggerName").GetString());
        Assert.True(metadata.GetProperty("providerOwnedDdl").GetBoolean());
        Assert.False(metadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
        Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
        Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
        Assert.Equal("provider-owned-trigger", metadata.GetProperty("reason").GetString());

        if (kind == ProviderKind.Sqlite)
        {
            Assert.True(metadata.GetProperty("definitionAvailable").GetBoolean());
            Assert.Contains("CREATE TRIGGER", metadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
        }
    }

    private static void AssertProviderSpecificColumnDiagnostic(string warningJsonPath, string columnName, string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-column-type", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertWritableProviderSpecificColumnDiagnostic(
        string warningJsonPath,
        string tableName,
        string columnName,
        string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.False(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected writable provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertFeatureMetadataHasNoProviderOwnedDiagnostics(string warningJsonPath, string tableName)
    {
        if (!File.Exists(warningJsonPath))
            return;

        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        foreach (var item in providerOwned.EnumerateArray())
        {
            var kind = item.GetProperty("kind").GetString();
            var table = item.GetProperty("table").GetString();
            Assert.False(
                LastTableNameEquals(table, tableName) &&
                kind is "Default" or "CheckConstraint" or "Computed" or "Collation",
                $"Expected {tableName} {kind} metadata to be promoted instead of reported as provider-owned diagnostics.");
        }
    }
}
