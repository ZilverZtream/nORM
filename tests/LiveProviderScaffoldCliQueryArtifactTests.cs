#nullable enable

using System;
using System.IO;
using System.Text.Json;
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
    public void Dotnet_norm_scaffold_table_filter_suppresses_unselected_principal_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var principalTable = "CliLiveFilterParent" + suffix;
        var dependentTable = "CliLiveFilterChild" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_table_filter_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupFilteredRelationship(connection, provider, kind, principalTable, dependentTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFilteredCtx " +
                $"--table {Quote(dependentTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, dependentTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, principalTable + ".cs")));
            var dependentCode = File.ReadAllText(Path.Combine(output, dependentTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFilteredCtx.cs"));

            Assert.Matches(@"public (int|long) ParentId \{ get; set; \}", dependentCode);
            Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
            Assert.DoesNotContain(principalTable, dependentCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{dependentTable}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(principalTable, contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
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
                CleanupFilteredRelationship(cleanup, provider, principalTable, dependentTable);
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
    public void Dotnet_norm_scaffold_table_filter_suppresses_unselected_dependent_relationship_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var principalTable = "CliLiveFilterParent" + suffix;
        var dependentTable = "CliLiveFilterChild" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_principal_filter_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupFilteredRelationship(connection, provider, kind, principalTable, dependentTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveFilteredPrincipalCtx " +
                $"--table {Quote(principalTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, principalTable + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, dependentTable + ".cs")));
            var principalCode = File.ReadAllText(Path.Combine(output, principalTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveFilteredPrincipalCtx.cs"));

            Assert.Contains($"IQueryable<{principalTable}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(dependentTable, principalCode, StringComparison.Ordinal);
            Assert.DoesNotContain(dependentTable, contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
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
                CleanupFilteredRelationship(cleanup, provider, principalTable, dependentTable);
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
    public void Dotnet_norm_scaffold_table_filter_emits_view_query_artifact_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseTable = "CliLiveViewBase" + suffix;
        var viewName = "CliLiveViewReport" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_view_filter_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupViewQueryArtifact(connection, provider, kind, baseTable, viewName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveViewCtx " +
                $"--table {Quote("view:" + viewName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, viewName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveViewCtx.cs"));
            var warningMarkdown = Path.Combine(output, "nORM.ScaffoldWarnings.md");

            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{viewName}", viewCode, StringComparison.Ordinal);
            Assert.Contains($"public partial class {viewName}", viewCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{viewName}>", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, baseTable + ".cs")));
            AssertViewQueryArtifactCommentDocumentation(kind, viewCode);
            Assert.True(File.Exists(warningMarkdown));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);
            Assert.DoesNotContain("Skipped Database Objects", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupViewQueryArtifact(cleanup, provider, kind, baseTable, viewName);
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
    public void Dotnet_norm_scaffold_emits_provider_query_artifacts_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var baseName = "CliQueryArtifactBase" + suffix;
        var artifactName = "CliQueryArtifact" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_query_artifact_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderQueryArtifact(connection, provider, kind, baseName, artifactName);
            }

            var tableFilter = kind switch
            {
                ProviderKind.SqlServer => "dbo." + artifactName,
                ProviderKind.Postgres => "public." + artifactName,
                _ => artifactName
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveQueryArtifactCtx " +
                "--emit-query-artifacts " +
                $"--table {Quote("query:" + tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var artifactCode = File.ReadAllText(Path.Combine(output, artifactName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveQueryArtifactCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", artifactCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{artifactName}", artifactCode, StringComparison.Ordinal);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.Contains("Schema = ", artifactCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{artifactName}>", contextCode, StringComparison.Ordinal);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                AssertViewQueryArtifactCommentDocumentation(kind, artifactCode);
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), artifactName));

            if (kind == ProviderKind.Sqlite)
            {
                Assert.Contains(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                    item.GetProperty("name").GetString()!.StartsWith(artifactName + "_", StringComparison.Ordinal));
            }
            else if (kind == ProviderKind.Postgres)
            {
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            }
            else
            {
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProviderQueryArtifact(cleanup, provider, kind, baseName, artifactName);
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
    public void Dotnet_norm_scaffold_default_discovery_emits_table_and_view_query_artifacts_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = "CliDefaultViewSchema" + suffix;
        var baseTable = "CliDefaultViewBase" + suffix;
        var viewName = "CliDefaultViewReport" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_default_view_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_default_view_" + suffix.ToLowerInvariant()
            : null;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        var scaffoldConnectionString = scratchDatabase is null
            ? connectionString
            : ConnectionStringWithDatabase(connectionString, scratchDatabase);
        try
        {
            using (connection)
            {
                if (scratchDatabase is not null)
                {
                    Execute(connection,
                        $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}",
                        $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
                    connection.ChangeDatabase(scratchDatabase);
                }

                SetupDefaultDiscoveryViewQueryArtifact(connection, provider, kind, schemaName, baseTable, viewName);
            }

            var schemaArgument = kind is ProviderKind.SqlServer or ProviderKind.Postgres
                ? $" --schema {Quote(schemaName)}"
                : string.Empty;
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveDefaultViewCtx" +
                schemaArgument,
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var baseCode = File.ReadAllText(Path.Combine(output, baseTable + ".cs"));
            var viewCode = File.ReadAllText(Path.Combine(output, viewName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveDefaultViewCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"", baseCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"", viewCode, StringComparison.Ordinal);
            Assert.Contains(baseTable, baseCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(viewName, viewCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.Contains($"Schema = \"{schemaName}\"", baseCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{schemaName}\"", viewCode, StringComparison.Ordinal);
            }

            Assert.Contains($"IQueryable<{baseTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{viewName}>", contextCode, StringComparison.OrdinalIgnoreCase);
            AssertViewQueryArtifactCommentDocumentation(kind, viewCode);
            Assert.True(File.Exists(warningJsonPath));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), viewName));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                if (scratchDatabase is null)
                    CleanupDefaultDiscoveryViewQueryArtifact(cleanup, provider, kind, schemaName, baseTable, viewName);
                else
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
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
