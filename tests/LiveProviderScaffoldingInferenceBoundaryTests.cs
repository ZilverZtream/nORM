#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_does_not_infer_owned_types_or_inheritance_from_column_conventions_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        const string tableName = "ScaffoldLiveInferenceBoundary";
        await using (connection)
        {
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_inference_boundary_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await SetupInferenceBoundaryAsync(connection, provider, kind, tableName);

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldInferenceBoundaryContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { DefaultSchemaTableFilter(kind, tableName) },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, tableName + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldInferenceBoundaryContext.cs"));
                var generatedCode = entityCode + contextCode;

                Assert.Contains("public string Discriminator { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string ShippingAddressStreet { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string ShippingAddressCity { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"ShippingAddress_Street\")]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("OwnsOne", generatedCode, StringComparison.Ordinal);
                Assert.DoesNotContain("OwnsMany", generatedCode, StringComparison.Ordinal);
                Assert.DoesNotContain("DiscriminatorColumn", generatedCode, StringComparison.Ordinal);
                Assert.DoesNotContain("DiscriminatorValue", generatedCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);

                await TeardownInferenceBoundaryAsync(connection, provider, kind, tableName);
            }
        }
    }

    private static async Task SetupInferenceBoundaryAsync(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        await TeardownInferenceBoundaryAsync(connection, provider, kind, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var discriminator = provider.Escape("Discriminator");
        var name = provider.Escape("Name");
        var street = provider.Escape("ShippingAddress_Street");
        var city = provider.Escape("ShippingAddress_City");
        var breed = provider.Escape("Dog_Breed");
        var text40 = TextType(kind, 40);
        var text80 = TextType(kind, 80);

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {discriminator} {text40} NOT NULL, {name} {text80} NOT NULL, {street} {text80} NOT NULL, {city} {text80} NOT NULL, {breed} {text80} NULL)");
    }

    private static async Task TeardownInferenceBoundaryAsync(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, tableName, provider.Escape(tableName)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
