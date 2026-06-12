#nullable enable

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider scalar facets and cross-provider type diagnostics.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_decimal_precision_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDecimalPrecisionAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_decimal_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldDecimalContext",
                    new ScaffoldOptions { Tables = new[] { DecimalPrecisionTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, DecimalPrecisionTable + ".cs"));
                Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(28,6)\")]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownDecimalPrecisionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_preserves_decimal_precision_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDecimalPrecisionAsync(connection, provider, kind);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DecimalPrecisionTable);

                var amount = type.GetProperty("Amount")!;
                var column = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());

                Assert.Equal(typeof(decimal), amount.PropertyType);
                Assert.Equal("Amount", column.Name);
                Assert.Equal("decimal(28,6)", column.TypeName);
            }
            finally
            {
                await TeardownDecimalPrecisionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_preserves_string_binary_facets_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: false);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_string_binary_facets_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldStringBinaryFacetContext",
                    new ScaffoldOptions { Tables = new[] { StringBinaryFacetTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, StringBinaryFacetTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldStringBinaryFacetContext.cs"));

                Assert.Contains("[MaxLength(40)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(12)]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Code).HasMaxLength(40)", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12)", contextCode, StringComparison.Ordinal);

                if (kind == ProviderKind.SqlServer)
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Code).HasMaxLength(40).IsUnicode(false);", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsUnicode(false).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
                }
                else if (kind is ProviderKind.MySql or ProviderKind.Sqlite)
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Token).HasMaxLength", contextCode, StringComparison.Ordinal);
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownStringBinaryFacetsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_schema_signature_includes_string_binary_facets_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var generator = new DynamicEntityTypeGenerator();
            await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: false);
            try
            {
                var firstSignature = generator.ComputeSchemaSignature(connection, StringBinaryFacetTable);
                var type = await generator.GenerateEntityTypeAsync(connection, StringBinaryFacetTable);
                var codeLength = Assert.Single(type.GetProperty("Code")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                var fixedCodeLength = Assert.Single(type.GetProperty("FixedCode")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());

                Assert.Equal(40, codeLength.Length);
                Assert.Equal(12, fixedCodeLength.Length);
                if (kind == ProviderKind.Postgres)
                {
                    Assert.Empty(type.GetProperty("Token")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false));
                }
                else
                {
                    var tokenLength = Assert.Single(type.GetProperty("Token")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                    Assert.Equal(16, tokenLength.Length);
                }

                await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: true);
                var secondSignature = generator.ComputeSchemaSignature(connection, StringBinaryFacetTable);

                Assert.NotEqual(firstSignature, secondSignature);
            }
            finally
            {
                await TeardownStringBinaryFacetsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_maps_temporal_catalog_store_types_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupTemporalStoreTypeTableAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_temporal_store_types_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldTemporalStoreTypeContext",
                    new ScaffoldOptions { Tables = new[] { DefaultSchemaTableFilter(kind, TemporalStoreTypeTable) }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, TemporalStoreTypeTable + ".cs"));

                Assert.Contains("public DateOnly BusinessDate { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public DateTime CreatedAt { get; set; }", entityCode, StringComparison.Ordinal);
                if (kind == ProviderKind.MySql)
                {
                    Assert.Contains("public TimeSpan? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("OffsetAt", entityCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public DateTimeOffset? OffsetAt { get; set; }", entityCode, StringComparison.Ordinal);
                }

                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownTemporalStoreTypeTableAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_maps_temporal_catalog_store_types_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupTemporalStoreTypeTableAsync(connection, provider, kind);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DefaultSchemaTableFilter(kind, TemporalStoreTypeTable));

                Assert.Equal(typeof(DateOnly), type.GetProperty("BusinessDate")!.PropertyType);
                Assert.Equal(typeof(DateTime), type.GetProperty("CreatedAt")!.PropertyType);
                if (kind == ProviderKind.MySql)
                {
                    Assert.Equal(typeof(TimeSpan?), type.GetProperty("StartsAt")!.PropertyType);
                    Assert.Null(type.GetProperty("OffsetAt"));
                }
                else
                {
                    Assert.Equal(typeof(TimeOnly?), type.GetProperty("StartsAt")!.PropertyType);
                    Assert.Equal(typeof(DateTimeOffset?), type.GetProperty("OffsetAt")!.PropertyType);
                }
            }
            finally
            {
                await TeardownTemporalStoreTypeTableAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "Location", "geometry")]
    [InlineData(ProviderKind.Postgres, "Address", "inet")]
    [InlineData(ProviderKind.MySql, "Location", "point")]
    [InlineData(ProviderKind.Sqlite, "Location", "GEOMETRY")]
    public async Task ScaffoldAsync_reports_nonportable_provider_specific_columns_on_live_provider(
        ProviderKind kind,
        string columnName,
        string expectedDetail)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_specific_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderSpecificColumnContext",
                    new ScaffoldOptions { Tables = new[] { ProviderSpecificColumnDiagnosticsTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderSpecificColumnDiagnosticsTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                Assert.Contains(columnName + " { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == columnName &&
                    item.GetProperty("detail").GetString()!.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) &&
                    item.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean() &&
                    !item.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean() &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("provider-specific type", StringComparison.OrdinalIgnoreCase));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

}
