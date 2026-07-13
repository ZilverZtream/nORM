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
    // Live CLI schema and table filter scaffold tests.

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
            ScaffoldCompileVerification.AssertCompiles(output);
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
            ScaffoldCompileVerification.AssertCompiles(output);
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

}
