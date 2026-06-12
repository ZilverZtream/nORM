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
    // SQL Server-specific provider type scaffold parity tests.

    [Fact]
    public async Task ScaffoldAsync_reports_sqlserver_alias_type_columns_with_base_type_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            var table = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeTable);
            var aliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeName);
            var decimalAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasDecimalTypeName);
            var binaryAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasBinaryTypeName);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_alias_type_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection, $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerAliasTypeContext",
                    new ScaffoldOptions { Tables = new[] { "dbo." + SqlServerAliasTypeTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerAliasTypeTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public byte[] Token { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(64)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                var aliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasTypeName, StringComparison.Ordinal));
                Assert.Contains("nvarchar(320)", aliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(aliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(aliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", aliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var decimalAliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasDecimalTypeName, StringComparison.Ordinal));
                Assert.Contains("decimal(18,4)", decimalAliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(decimalAliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(decimalAliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", decimalAliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var binaryAliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasBinaryTypeName, StringComparison.Ordinal));
                Assert.Contains("varbinary(64)", binaryAliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(binaryAliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(binaryAliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", binaryAliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_sqlserver_alias_type_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            var table = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeTable);
            var aliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeName);
            var decimalAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasDecimalTypeName);
            var binaryAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasBinaryTypeName);
            try
            {
                await ExecuteAsync(connection, $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");

                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, "dbo." + SqlServerAliasTypeTable);

                var tableAttribute = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());
                Assert.Equal("dbo", tableAttribute.Schema);
                Assert.Equal(SqlServerAliasTypeTable, tableAttribute.Name);
                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                var email = type.GetProperty("Email")!;
                Assert.Equal(typeof(string), email.PropertyType);
                var maxLength = Assert.Single(email.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                Assert.Equal(320, maxLength.Length);
                var amount = type.GetProperty("Amount")!;
                Assert.Equal(typeof(decimal), amount.PropertyType);
                var amountColumn = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());
                Assert.Equal("Amount", amountColumn.Name);
                Assert.Equal("decimal(18,4)", amountColumn.TypeName);
                var token = type.GetProperty("Token")!;
                Assert.Equal(typeof(byte[]), token.PropertyType);
                var tokenMaxLength = Assert.Single(token.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                Assert.Equal(64, tokenMaxLength.Length);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            }
        }
    }

}
