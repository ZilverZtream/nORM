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
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_routine_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliRoutine" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRoutineStub(connection, provider, kind, routineName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Routine scaffolding should keep provider-owned routine metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routine = Assert.Single(
                warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                item => item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));

            Assert.Contains($"Task<List<TResult>> {routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
            Assert.Contains(
                kind == ProviderKind.Postgres
                    ? "public int? tenantid { get; init; }"
                    : "public int? tenantId { get; init; }",
                contextCode,
                StringComparison.Ordinal);
            Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", contextCode, StringComparison.Ordinal);
            Assert.Contains("/// Routine &lt;summary&gt; &amp; description", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Routine <summary> & description", contextCode, StringComparison.Ordinal);
            Assert.Equal("Routine", routine.GetProperty("kind").GetString());

            if (kind == ProviderKind.SqlServer)
            {
                Assert.Equal(1, routine.GetProperty("metadata").GetProperty("outputParameterCount").GetInt32());
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{routineName}Result>> {routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{routineName}OutputParameters()", contextCode, StringComparison.Ordinal);
            }

            if (kind == ProviderKind.Postgres)
            {
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int Id { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string Name { get; set; } = default!;", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupRoutineStub(cleanup, provider, kind, routineName);
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
    public void Dotnet_norm_scaffold_emits_advanced_routine_stubs_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliAdvancedRoutine" + suffix;
        var tableFunctionName = "CliAdvancedTableFunction" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_advanced_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupAdvancedRoutineStub(connection, provider, kind, routineName, tableFunctionName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveAdvancedRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveAdvancedRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Advanced routine stubs must preserve provider-owned routine metadata.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

            switch (kind)
            {
                case ProviderKind.SqlServer:
                {
                    Assert.Contains(routines, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal) &&
                        item.GetProperty("metadata").GetProperty("callShape").GetString() == "scalar-function");
                    var tableValuedFunction = Assert.Single(routines, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(tableFunctionName, StringComparison.Ordinal) &&
                        item.GetProperty("metadata").GetProperty("callShape").GetString() == "table-valued-function");
                    var resultColumns = tableValuedFunction.GetProperty("metadata").GetProperty("resultColumns").EnumerateArray().ToArray();
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Id" &&
                        item.GetProperty("dataType").GetString() == "int");
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Name" &&
                        item.GetProperty("dataType").GetString()!.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase));

                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<TValue?> {routineName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"public sealed class {tableFunctionName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"public sealed class {tableFunctionName}Result", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public int? Id { get; set; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public string? Name { get; set; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<TResult>> {tableFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<{tableFunctionName}Result>> {tableFunctionName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<TResult> Stream{tableFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<{tableFunctionName}Result> Stream{tableFunctionName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{routineName}\")", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{tableFunctionName}\")", contextCode, StringComparison.Ordinal);
                    break;
                }
                case ProviderKind.Postgres:
                {
                    var routine = Assert.Single(
                        routines,
                        item => item.GetProperty("kind").GetString() == "Routine" &&
                                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                    var parameters = routine.GetProperty("metadata").GetProperty("parameters").EnumerateArray().ToArray();
                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public int[]? ids { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public decimal[]? ratings { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public string[]? labels { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public Guid? trace_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("var casts = new[] { \"integer[]\", \"numeric[]\", \"character varying[]\", \"uuid\" };", contextCode, StringComparison.Ordinal);
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "ids" &&
                        item.GetProperty("clrType").GetString() == "int[]?" &&
                        item.GetProperty("dbType").GetString() == "Object");
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "ratings" &&
                        item.GetProperty("clrType").GetString() == "decimal[]?" &&
                        item.GetProperty("dbType").GetString() == "Object");
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "labels" &&
                        item.GetProperty("clrType").GetString() == "string[]?" &&
                        item.GetProperty("dbType").GetString() == "Object");
                    Assert.Contains(parameters, item =>
                        item.GetProperty("name").GetString() == "trace_id" &&
                        item.GetProperty("clrType").GetString() == "Guid?");
                    break;
                }
                case ProviderKind.MySql:
                {
                    var routine = Assert.Single(
                        routines,
                        item => item.GetProperty("kind").GetString() == "Routine" &&
                                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                    var metadata = routine.GetProperty("metadata");

                    Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public uint? customer_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public ulong? max_id { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public ushort? rank { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public byte? flag { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Equal("scalar-function", metadata.GetProperty("callShape").GetString());
                    Assert.Equal("int", metadata.GetProperty("dataType").GetString());
                    Assert.Contains($"Task<List<TResult>> {routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"private sealed class {routineName}ValueResult<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<TValue?> {routineName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                    Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine stubs target providers with routine catalogs.");
            }

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupAdvancedRoutineStub(cleanup, provider, kind, routineName, tableFunctionName);
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
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_routine_output_and_non_query_wrappers_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var routineName = "CliOutputRoutine" + suffix;
        var nonQueryRoutineName = "CliNonQueryRoutine" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_output_routine_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupRoutineOutputAndNonQueryWrappers(connection, provider, kind, routineName, nonQueryRoutineName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveOutputRoutineCtx " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveOutputRoutineCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Routine output scaffolding should keep provider-owned routine metadata in JSON warnings.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();
            Assert.Contains(routines, item =>
                item.GetProperty("kind").GetString() == "Routine" &&
                item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));

            Assert.Contains($"Task<StoredProcedureResult<TResult>> {routineName}WithOutputAsync<TResult>", contextCode, StringComparison.Ordinal);
            Assert.Contains($"public static OutputParameter[] Create{routineName}OutputParameters()", contextCode, StringComparison.Ordinal);
            Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal, (byte)18, (byte)2)", contextCode, StringComparison.Ordinal);

            if (kind == ProviderKind.MySql)
            {
                Assert.Contains("public string? message { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", contextCode, StringComparison.Ordinal);
            }
            else
            {
                var nonQueryRoutine = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(nonQueryRoutineName, StringComparison.Ordinal));
                var nonQueryMetadata = nonQueryRoutine.GetProperty("metadata");
                Assert.Equal("stored procedure", nonQueryMetadata.GetProperty("routineType").GetString());
                Assert.Equal(2, nonQueryMetadata.GetProperty("outputParameterCount").GetInt32());

                Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32)", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<int> {nonQueryRoutineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<StoredProcedureNonQueryResult> {nonQueryRoutineName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{nonQueryRoutineName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{nonQueryRoutineName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{nonQueryRoutineName}OutputParameters()", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Task<List<TResult>> {nonQueryRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"StoredProcedureResult<TResult> {nonQueryRoutineName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Stream{nonQueryRoutineName}Async", contextCode, StringComparison.Ordinal);
            }

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupRoutineOutputAndNonQueryWrappers(cleanup, provider, kind, routineName, nonQueryRoutineName);
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
