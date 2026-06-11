#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
    public void Dotnet_norm_scaffold_enforces_output_safety_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveSafety" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_output_safety_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var baseCommand =
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSafetyCtx " +
                $"--table {Quote(tableName)}";

            var dryRun = RunCli(baseCommand + " --dry-run --json", root);
            Assert.True(dryRun.ExitCode == 0,
                $"CLI dry run failed with exit code {dryRun.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{dryRun.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{dryRun.Stderr}");
            using (var dryRunJson = JsonDocument.Parse(dryRun.Stdout))
            {
                var json = dryRunJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.True(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            Assert.False(Directory.Exists(output));

            var scaffold = RunCli(baseCommand, root);
            Assert.True(scaffold.ExitCode == 0,
                $"CLI scaffold failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveSafetyCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            var originalEntity = File.ReadAllText(entityPath);

            var noOverwrite = RunCli(baseCommand + " --no-overwrite", root);
            Assert.NotEqual(0, noOverwrite.ExitCode);
            Assert.Contains("already exists", noOverwrite.Stderr + noOverwrite.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalEntity, File.ReadAllText(entityPath));

            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"), "# stale", Encoding.UTF8);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""", Encoding.UTF8);

            var force = RunCli(baseCommand + " --force --json", root);
            Assert.True(force.ExitCode == 0,
                $"CLI force scaffold failed with exit code {force.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{force.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{force.Stderr}");
            using (var forceJson = JsonDocument.Parse(force.Stdout))
            {
                var json = forceJson.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

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
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
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
    public void Dotnet_norm_scaffold_accepts_csv_and_multi_value_table_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var csvTableOne = "CliLiveCsvOne" + suffix;
        var csvTableTwo = "CliLiveCsvTwo" + suffix;
        var multiTableOne = "CliLiveMultiOne" + suffix;
        var multiTableTwo = "CliLiveMultiTwo" + suffix;
        var skippedTable = "CliLiveFilterSkip" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_table_filter_values_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupTableFilterValuesScaffold(
                    connection,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTableFilterValuesCtx " +
                $"--tables {Quote(csvTableOne + "," + csvTableTwo)} " +
                $"--table {multiTableOne} {multiTableTwo}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, csvTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, csvTableTwo + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableTwo + ".cs")));
            Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveTableFilterValuesCtx.cs"));
            Assert.Contains($"IQueryable<{csvTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{csvTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableOne}>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{multiTableTwo}>", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
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
                CleanupTableFilterValuesScaffold(
                    cleanup,
                    provider,
                    kind,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
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
    public void Dotnet_norm_scaffold_accepts_csv_and_multi_value_schema_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var csvSchemaOne = "CliSchemaCsvOne" + suffix;
        var csvSchemaTwo = "CliSchemaCsvTwo" + suffix;
        var multiSchemaOne = "CliSchemaMultiOne" + suffix;
        var multiSchemaTwo = "CliSchemaMultiTwo" + suffix;
        var skippedSchema = "CliSchemaSkip" + suffix;
        var csvTableOne = "CliSchemaCsvOneTable" + suffix;
        var csvTableTwo = "CliSchemaCsvTwoTable" + suffix;
        var multiTableOne = "CliSchemaMultiOneTable" + suffix;
        var multiTableTwo = "CliSchemaMultiTwoTable" + suffix;
        var skippedTable = "CliSchemaSkipTable" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_schema_filter_values_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_schema_filter_" + suffix.ToLowerInvariant()
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

                SetupSchemaFilterValuesScaffold(
                    connection,
                    provider,
                    kind,
                    csvSchemaOne,
                    csvSchemaTwo,
                    multiSchemaOne,
                    multiSchemaTwo,
                    skippedSchema,
                    csvTableOne,
                    csvTableTwo,
                    multiTableOne,
                    multiTableTwo,
                    skippedTable);
            }

            var schemaArguments = kind switch
            {
                ProviderKind.Sqlite => "--schemas main --schema main",
                ProviderKind.MySql => $"--schemas {Quote(scratchDatabase!)} --schema {Quote(scratchDatabase!)}",
                _ => $"--schemas {Quote(csvSchemaOne + "," + csvSchemaTwo)} --schema {multiSchemaOne} {multiSchemaTwo}"
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSchemaFilterValuesCtx " +
                schemaArguments,
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, csvTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, csvTableTwo + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableOne + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, multiTableTwo + ".cs")));
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSchemaFilterValuesCtx.cs"));
            Assert.Contains($"IQueryable<{csvTableOne}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{csvTableTwo}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{multiTableOne}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{multiTableTwo}>", contextCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{csvSchemaOne}\"", File.ReadAllText(Path.Combine(output, csvTableOne + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{csvSchemaTwo}\"", File.ReadAllText(Path.Combine(output, csvTableTwo + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{multiSchemaOne}\"", File.ReadAllText(Path.Combine(output, multiTableOne + ".cs")), StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{multiSchemaTwo}\"", File.ReadAllText(Path.Combine(output, multiTableTwo + ".cs")), StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain("Schema =", File.ReadAllText(Path.Combine(output, csvTableOne + ".cs")), StringComparison.Ordinal);
                Assert.DoesNotContain("Schema =", File.ReadAllText(Path.Combine(output, multiTableOne + ".cs")), StringComparison.Ordinal);
            }

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
                if (scratchDatabase is null)
                {
                    CleanupSchemaFilterValuesScaffold(
                        cleanup,
                        provider,
                        kind,
                        csvSchemaOne,
                        csvSchemaTwo,
                        multiSchemaOne,
                        multiSchemaTwo,
                        skippedSchema,
                        csvTableOne,
                        csvTableTwo,
                        multiTableOne,
                        multiTableTwo,
                        skippedTable);
                }
                else
                {
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                }
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
    public void Dotnet_norm_scaffold_unions_schema_and_table_filters_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var schemaName = "CliFilterUnionSchema" + suffix;
        var schemaTable = "CliFilterUnionSchemaTable" + suffix;
        var explicitTable = "CliFilterUnionExplicit" + suffix;
        var skippedTable = "CliFilterUnionSkip" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_filter_union_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_filter_union_" + suffix.ToLowerInvariant()
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

                SetupSchemaAndTableFilterUnionScaffold(
                    connection,
                    provider,
                    kind,
                    schemaName,
                    schemaTable,
                    explicitTable,
                    skippedTable);
            }

            var schemaArgument = kind switch
            {
                ProviderKind.Sqlite => "--schema main",
                ProviderKind.MySql => $"--schema {Quote(scratchDatabase!)}",
                _ => $"--schema {Quote(schemaName)}"
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSchemaTableUnionCtx " +
                $"{schemaArgument} " +
                $"--table {Quote(explicitTable)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, schemaTable + ".cs")));
            Assert.True(File.Exists(Path.Combine(output, explicitTable + ".cs")));
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                Assert.False(File.Exists(Path.Combine(output, skippedTable + ".cs")));

            var schemaEntityCode = File.ReadAllText(Path.Combine(output, schemaTable + ".cs"));
            var explicitEntityCode = File.ReadAllText(Path.Combine(output, explicitTable + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSchemaTableUnionCtx.cs"));

            Assert.Contains($"IQueryable<{schemaTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains($"IQueryable<{explicitTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.DoesNotContain(skippedTable, contextCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{schemaName}\"", schemaEntityCode, StringComparison.Ordinal);
                Assert.Contains($"Schema = \"{(kind == ProviderKind.SqlServer ? "dbo" : "public")}\"", explicitEntityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain("Schema =", schemaEntityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Schema =", explicitEntityCode, StringComparison.Ordinal);
            }

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
                if (scratchDatabase is null)
                {
                    CleanupSchemaAndTableFilterUnionScaffold(
                        cleanup,
                        provider,
                        kind,
                        schemaName,
                        schemaTable,
                        explicitTable,
                        skippedTable);
                }
                else
                {
                    Execute(cleanup, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                }
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
    public void Dotnet_norm_scaffold_accepts_no_onconfiguring_data_annotations_and_no_pluralize_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfSwitch" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_switches_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveEfSwitchCtx " +
                "--no-onconfiguring " +
                "--data-annotations " +
                "--no-pluralize " +
                "--json " +
                "--verbose " +
                "--no-color " +
                "--prefix-output " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using (var document = JsonDocument.Parse(scaffold.Stdout))
            {
                var json = document.RootElement;
                var warnings = json.GetProperty("warnings");
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.False(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
                Assert.False(warnings.GetProperty("hasDiagnostics").GetBoolean());
                Assert.False(warnings.GetProperty("reportsWritten").GetBoolean());
                Assert.Equal(0, warnings.GetProperty("totalWarnings").GetInt32());
            }

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveEfSwitchCtx.cs"));
            Assert.DoesNotContain("OnConfiguring", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain(connectionString, contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("[Table(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Column(\"", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Key]", entityCode, StringComparison.Ordinal);
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
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
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
