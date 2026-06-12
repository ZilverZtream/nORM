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
    // MySQL-specific provider type scaffold parity tests.

    [Fact]
    public async Task ScaffoldAsync_emits_mysql_json_and_year_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlTypedColumnTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_typed_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlTypedColumnContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { MySqlTypedColumnTable },
                        OverwriteFiles = false
                    });

                var entityName = DefaultScaffoldEntityName(MySqlTypedColumnTable);
                var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, MySqlTypedColumnTable));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldMySqlTypedColumnContext.cs"));

                Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Flags { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("FiscalYear { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("object FiscalYear", entityCode, StringComparison.Ordinal);
                Assert.Contains($".HasCheckConstraint(\"CK_{entityName}_Status_Enum\", \"Status IN ('draft', 'paid', 'cancelled')\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($".HasCheckConstraint(\"CK_{entityName}_Flags_Set\", \"Flags IN ('', 'read', 'write', 'read,write', 'admin', 'read,admin', 'write,admin', 'read,write,admin')\")", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlTypedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_mysql_json_year_enum_set_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlTypedColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, MySqlTypedColumnTable);

                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                Assert.Equal(typeof(string), type.GetProperty("Payload")!.PropertyType);
                Assert.NotEqual(typeof(object), type.GetProperty("FiscalYear")!.PropertyType);
                Assert.Equal(typeof(string), type.GetProperty("Status")!.PropertyType);
                Assert.Equal(typeof(string), type.GetProperty("Flags")!.PropertyType);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownMySqlTypedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_unsigned_columns_as_provider_specific_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsignedColumnTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_unsigned_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlUnsignedColumnContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { MySqlUnsignedColumnTable },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, MySqlUnsignedColumnTable));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("UnsignedCount", entityCode, StringComparison.Ordinal);
                Assert.Contains("UnsignedTotal", entityCode, StringComparison.Ordinal);
                Assert.Contains("UnsignedAmount", entityCode, StringComparison.Ordinal);
                Assert.Contains("public uint UnsignedCount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public ulong UnsignedTotal { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"UnsignedAmount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public decimal UnsignedAmount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                var unsignedCountDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedCount" &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedCountDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedCountDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedCountDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var unsignedTotalDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedTotal" &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var unsignedAmountDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedAmount" &&
                    item.GetProperty("detail").GetString()!.Contains("decimal", StringComparison.OrdinalIgnoreCase) &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlUnsignedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_mysql_unsigned_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsignedColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, MySqlUnsignedColumnTable);

                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                Assert.Equal(typeof(uint), type.GetProperty("UnsignedCount")!.PropertyType);
                Assert.Equal(typeof(ulong), type.GetProperty("UnsignedTotal")!.PropertyType);
                var amount = type.GetProperty("UnsignedAmount")!;
                Assert.Equal(typeof(decimal), amount.PropertyType);
                var amountColumn = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());
                Assert.Equal("UnsignedAmount", amountColumn.Name);
                Assert.Equal("decimal(18,4)", amountColumn.TypeName);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownMySqlUnsignedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_marks_unsafe_mysql_set_columns_read_only_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsafeSetColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, MySqlUnsafeSetColumnTable);

                Assert.NotNull(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownMySqlUnsafeSetColumnTableAsync(connection, provider);
            }
        }
    }
}
