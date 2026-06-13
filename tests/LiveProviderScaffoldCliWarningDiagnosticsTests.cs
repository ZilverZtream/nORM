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
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_reports_trigger_diagnostics_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveTriggerAudit" + suffix;
        var triggerName = "TR_CliLiveTriggerAudit_Touch_" + suffix;
        var functionName = "fn_CliLiveTriggerAuditTouch_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_trigger_diag_" + kind + "_" + suffix);
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_cli_trigger_" + suffix.ToLowerInvariant()
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

                SetupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(scaffoldConnectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTriggerDiagnosticsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains("Touched { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath), "Trigger diagnostics must write the scaffold warning JSON report.");
            AssertTriggerDiagnostic(warningJsonPath, tableName, triggerName, kind);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                if (scratchDatabase is null)
                    CleanupTriggerDiagnostics(cleanup, provider, kind, tableName, triggerName, functionName);
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

    [Theory]
    [InlineData(ProviderKind.Sqlite, "Location", "GEOMETRY")]
    [InlineData(ProviderKind.SqlServer, "Location", "geometry")]
    [InlineData(ProviderKind.Postgres, "Address", "inet")]
    [InlineData(ProviderKind.MySql, "Location", "point")]
    public void Dotnet_norm_scaffold_reports_provider_specific_column_diagnostics_on_live_provider(
        ProviderKind kind,
        string columnName,
        string expectedDetail)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliProviderColumn" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderSpecificColumnDiagnostics(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProviderColumnCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains(columnName + " { get; set; }", entityCode, StringComparison.Ordinal);
            AssertProviderSpecificColumnDiagnostic(Path.Combine(output, "nORM.ScaffoldWarnings.json"), columnName, expectedDetail);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProviderSpecificColumnDiagnostics(cleanup, provider, kind, tableName);
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
    public void Dotnet_norm_scaffold_preserves_safe_provider_specific_columns_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliSafeProviderColumn" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_safe_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupSafeProviderSpecificColumns(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveSafeProviderColumnCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveSafeProviderColumnCtx.cs"));

            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            switch (kind)
            {
                case ProviderKind.Sqlite:
                    Assert.Contains("using System;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid TraceId { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string? XmlPayload { get; set; }", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.SqlServer:
                    Assert.Contains("public string XmlPayload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.Postgres:
                    Assert.Contains("using System;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid TraceId { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public int[]? Scores { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string[]? Tags { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal[]? Ratings { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string[]? Aliases { get; set; }", entityCode, StringComparison.Ordinal);
                    break;
                case ProviderKind.MySql:
                    Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Flags { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("FiscalYear { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("object FiscalYear", entityCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Status_Enum\", \"Status IN ('draft', 'paid', 'cancelled')\")", contextCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Flags_Set\", \"Flags IN ('', 'read', 'write', 'read,write', 'admin', 'read,admin', 'write,admin', 'read,write,admin')\")", contextCode, StringComparison.Ordinal);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupSafeProviderSpecificColumns(cleanup, provider, kind, tableName);
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
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_writable_provider_specific_diagnostics_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliWritableProviderColumn" + suffix;
        var typeName = "CliWritableEmail" + suffix;
        var decimalTypeName = "CliWritableAmount" + suffix;
        var binaryTypeName = "CliWritableToken" + suffix;
        var arrayTypeName = "cli_writable_scores_" + suffix.ToLowerInvariant();
        var enumTypeName = "cli_writable_status_" + suffix.ToLowerInvariant();
        var enumDomainName = "cli_writable_status_domain_" + suffix.ToLowerInvariant();
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_writable_provider_column_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupWritableProviderSpecificDiagnostics(connection, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);
            }

            var tableFilter = kind switch
            {
                ProviderKind.SqlServer => "dbo." + tableName,
                ProviderKind.Postgres => "public." + tableName,
                _ => tableName
            };
            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveWritableProviderColumnCtx " +
                $"--table {Quote(tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveWritableProviderColumnCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath), "Writable provider-specific diagnostics must still be reported for provider-mobility review.");

            switch (kind)
            {
                case ProviderKind.SqlServer:
                    Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(64)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public byte[] Token { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Email", "user-defined type (dbo." + typeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Amount", "user-defined type (dbo." + decimalTypeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Token", "user-defined type (dbo." + binaryTypeName);
                    break;
                case ProviderKind.Postgres:
                    Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"Score\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal Score { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public int[] Scores { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                    Assert.Contains($".HasCheckConstraint(\"CK_{tableName}_Status_Enum\", \"Status IN ('draft', 'active', 'archived')\")", contextCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Email", "DOMAIN (public." + typeName.ToLowerInvariant());
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Score", "DOMAIN (public." + decimalTypeName.ToLowerInvariant());
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Scores", "DOMAIN (public." + arrayTypeName);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "Status", "DOMAIN (public." + enumDomainName);
                    break;
                case ProviderKind.MySql:
                    Assert.Contains("public uint UnsignedCount { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public ulong UnsignedTotal { get; set; }", entityCode, StringComparison.Ordinal);
                    Assert.Contains("[Column(\"UnsignedAmount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal UnsignedAmount { get; set; }", entityCode, StringComparison.Ordinal);
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedCount", "unsigned");
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedTotal", "unsigned");
                    AssertWritableProviderSpecificColumnDiagnostic(warningJsonPath, tableName, "UnsignedAmount", "decimal");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupWritableProviderSpecificDiagnostics(cleanup, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);
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
    public void Dotnet_norm_scaffold_json_fail_on_warnings_reports_live_provider_diagnostics(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var keylessTable = "CliLiveWarning_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_warning_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupKeylessWarning(connection, provider, kind, keylessTable);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveWarningCtx " +
                "--fail-on-warnings " +
                "--json " +
                $"--table {Quote(keylessTable)}",
                root);

            Assert.NotEqual(0, scaffold.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(scaffold.Stderr), scaffold.Stderr);

            using var document = JsonDocument.Parse(scaffold.Stdout);
            var json = document.RootElement;
            var warnings = json.GetProperty("warnings");
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.Contains("Scaffolding produced warnings", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.True(warnings.GetProperty("hasDiagnostics").GetBoolean());
            Assert.True(warnings.GetProperty("reportsWritten").GetBoolean());
            Assert.Equal(1, warnings.GetProperty("codes").GetProperty("SCF116").GetInt32());
            Assert.Equal(1, warnings.GetProperty("categories").GetProperty("table-shape").GetInt32());

            var warningMarkdown = Path.Combine(output, "nORM.ScaffoldWarnings.md");
            var warningJson = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningMarkdown));
            Assert.True(File.Exists(warningJson));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningMarkdown), StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupKeylessWarning(cleanup, provider, keylessTable);
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
