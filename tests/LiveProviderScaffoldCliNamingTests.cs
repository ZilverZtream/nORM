#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_database_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = Guid.NewGuid().ToString("N")[..8].ToLowerInvariant();
        var customerTable = "cli_live_customer_" + suffix;
        var orderLineTable = "cli_live_order_line_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_database_names_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupUseDatabaseNames(connection, provider, kind, customerTable, orderLineTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDatabaseNamesCtx " +
                "--use-database-names " +
                "--no-pluralize " +
                $"--table {Quote(customerTable)} " +
                $"--table {Quote(orderLineTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var customerCode = File.ReadAllText(Path.Combine(output, customerTable + ".cs"));
            var orderLineCode = File.ReadAllText(Path.Combine(output, orderLineTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDatabaseNamesCtx.cs"));

            Assert.Contains($"public partial class {customerTable}", customerCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {orderLineTable}", orderLineCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) customer_id \{ get; set; \}", customerCode);
            Assert.Contains("public string display_name { get; set; } = default!;", customerCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long) order_line_id \{ get; set; \}", orderLineCode);
            Assert.Matches(@"public (int|long) customer_id \{ get; set; \}", orderLineCode);
            Assert.Contains("public string SKU { get; set; } = default!;", orderLineCode, StringComparison.Ordinal);
            Assert.Contains("public string? @class { get; set; }", orderLineCode, StringComparison.Ordinal);
            Assert.Contains("public string? has_space { get; set; }", orderLineCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{customerTable}> {customerTable}", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{orderLineTable}> {orderLineTable}", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("CliLiveCustomer", customerCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderLineId", orderLineCode, StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupUseDatabaseNames(cleanup, provider, customerTable, orderLineTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_generates_valid_unique_identifiers_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix().ToLowerInvariant();
        var generatedSuffix = char.ToUpperInvariant(suffix[0]) + suffix[1..];
        var invalidIdentifierTable = "cli-live-identifier-" + suffix;
        var collisionDashTable = "cli-live-collision-" + suffix;
        var collisionUnderscoreTable = "cli_live_collision_" + suffix;
        var invalidIdentifierEntity = "CliLiveIdentifier" + generatedSuffix;
        var collisionEntity = "CliLiveCollision" + generatedSuffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_identifiers_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupIdentifierCollisionScaffold(connection, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveIdentifierCtx " +
                $"--table {Quote(invalidIdentifierTable)} " +
                $"--table {Quote(collisionDashTable)} " +
                $"--table {Quote(collisionUnderscoreTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var invalidIdentifierCode = File.ReadAllText(Path.Combine(output, invalidIdentifierEntity + ".cs"));
            var firstCollisionPath = Path.Combine(output, collisionEntity + ".cs");
            var secondCollisionPath = Path.Combine(output, collisionEntity + "2.cs");
            Assert.True(File.Exists(firstCollisionPath));
            Assert.True(File.Exists(secondCollisionPath));

            var firstCollisionCode = File.ReadAllText(firstCollisionPath);
            var secondCollisionCode = File.ReadAllText(secondCollisionPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveIdentifierCtx.cs"));

            Assert.Contains($"public partial class {invalidIdentifierEntity}", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{invalidIdentifierTable}\"", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"1st-name\")]", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string _1stName { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Matches(@"public (int|long)\? HasSpace \{ get; set; \}", invalidIdentifierCode);
            Assert.Contains("public string FirstName { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string FirstName2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string ToString2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.Contains("public string Equals2 { get; set; } = default!;", invalidIdentifierCode, StringComparison.Ordinal);
            Assert.DoesNotContain("public string ToString {", invalidIdentifierCode, StringComparison.Ordinal);

            Assert.Contains($"public partial class {collisionEntity}", firstCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {collisionEntity}2", secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{collisionDashTable}\"", firstCollisionCode + secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{collisionUnderscoreTable}\"", firstCollisionCode + secondCollisionCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{collisionEntity}> {collisionEntity}s", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{collisionEntity}2> {collisionEntity}2s", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupIdentifierCollisionScaffold(cleanup, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }
}
