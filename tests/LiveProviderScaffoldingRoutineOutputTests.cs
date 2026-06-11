#nullable enable

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider routine output, function wrapper, and sequence scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_emits_routine_output_factories_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupRoutineWithOutputAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_routine_output_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRoutineOutputContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRoutineOutputContext.cs"));
                Assert.Contains($"Task<StoredProcedureResult<TResult>> {RoutineOutputName}WithOutputAsync<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{RoutineOutputName}OutputParameters()", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal, (byte)18, (byte)2)", contextCode, StringComparison.Ordinal);
                if (kind == ProviderKind.MySql)
                {
                    Assert.Contains("public string? message { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", contextCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32)", contextCode, StringComparison.Ordinal);
                    Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", contextCode, StringComparison.Ordinal);
                }
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownRoutineWithOutputAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.MySql)]
    public async Task Scaffolded_routine_output_invocation_name_executes_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupRoutineWithOutputAsync(connection, provider, kind);
            try
            {
                await using var ctx = new DbContext(connection, provider);
                var routineName = kind == ProviderKind.SqlServer
                    ? provider.Escape("dbo") + "." + provider.Escape(RoutineOutputName)
                    : provider.Escape(RoutineOutputName);
                object parameters = kind == ProviderKind.MySql
                    ? new { tenantId = 7, message = "seed" }
                    : new { tenantId = 7 };
                var outputParameters = kind == ProviderKind.MySql
                    ? new[]
                    {
                        new OutputParameter("total", DbType.Decimal, 18, 2),
                        new OutputParameter("message", DbType.String, 32, ParameterDirection.InputOutput)
                    }
                    : new[]
                    {
                        new OutputParameter("total", DbType.Decimal, 18, 2),
                        new OutputParameter("message", DbType.String, 32),
                        new OutputParameter("return", DbType.Int32, null, ParameterDirection.ReturnValue)
                    };

                var result = await ctx.ExecuteStoredProcedureWithOutputAsync<LiveRoutineOutputRow>(
                    routineName,
                    parameters: parameters,
                    outputParameters: outputParameters);

                var row = Assert.Single(result.Results);
                Assert.Equal(7, row.Id);
                Assert.Equal("ok", row.Name);
                Assert.Equal(12.34m, Convert.ToDecimal(result.OutputParameters["total"]));
                Assert.Equal(kind == ProviderKind.MySql ? "seedok" : "ok", Convert.ToString(result.OutputParameters["message"]));
                if (kind == ProviderKind.SqlServer)
                    Assert.Equal(0, Convert.ToInt32(result.OutputParameters["return"]));
            }
            finally
            {
                await TeardownRoutineWithOutputAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlserver_table_valued_parameter_routine_stub_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerTableValuedParameterRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_tvp_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerTvpRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqlServerTvpRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(RoutineTableValuedParameterName, StringComparison.Ordinal));
                var metadata = routine.GetProperty("metadata");

                Assert.Contains($"public sealed class {RoutineTableValuedParameterName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int? tenantId { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public DbParameter? items { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"table type (dbo.{RoutineTableTypeName})", contextCode, StringComparison.Ordinal);
                Assert.Equal(2, metadata.GetProperty("parameterCount").GetInt32());
                var resultColumns = metadata.GetProperty("resultColumns").EnumerateArray().ToArray();
                Assert.Contains(resultColumns, item => item.GetProperty("name").GetString() == "Id");
                Assert.Contains(resultColumns, item => item.GetProperty("name").GetString() == "LineCount");
                Assert.Contains($"public sealed class {RoutineTableValuedParameterName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{RoutineTableValuedParameterName}Result>> {RoutineTableValuedParameterName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{RoutineTableValuedParameterName}Result> Stream{RoutineTableValuedParameterName}Async", contextCode, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerTableValuedParameterRoutineAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlserver_scalar_and_table_valued_function_wrappers_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerFunctionRoutinesAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_function_routines_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerFunctionRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqlServerFunctionRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(SqlServerScalarFunctionName, StringComparison.Ordinal) &&
                    item.GetProperty("metadata").GetProperty("callShape").GetString() == "scalar-function");
                var tableValuedFunction = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(SqlServerTableValuedFunctionName, StringComparison.Ordinal) &&
                    item.GetProperty("metadata").GetProperty("callShape").GetString() == "table-valued-function");
                var resultColumns = tableValuedFunction.GetProperty("metadata").GetProperty("resultColumns").EnumerateArray().ToArray();
                Assert.Contains(resultColumns, item =>
                    item.GetProperty("name").GetString() == "Id" &&
                    item.GetProperty("dataType").GetString() == "int");
                Assert.Contains(resultColumns, item =>
                    item.GetProperty("name").GetString() == "Name" &&
                    item.GetProperty("dataType").GetString()!.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase));

                Assert.Contains($"public sealed class {SqlServerScalarFunctionName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<TValue?> {SqlServerScalarFunctionName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                Assert.Contains("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {SqlServerTableValuedFunctionName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {SqlServerTableValuedFunctionName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int? Id { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? Name { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {SqlServerTableValuedFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{SqlServerTableValuedFunctionName}Result>> {SqlServerTableValuedFunctionName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<TResult> Stream{SqlServerTableValuedFunctionName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{SqlServerTableValuedFunctionName}Result> Stream{SqlServerTableValuedFunctionName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{SqlServerScalarFunctionName}\")", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"ExecuteStoredProcedureAsync<TResult>(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{SqlServerTableValuedFunctionName}\")", contextCode, StringComparison.Ordinal);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerFunctionRoutinesAsync(connection, provider);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    public async Task ScaffoldAsync_emits_sequence_wrappers_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSequenceAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sequence_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSequenceContext",
                    new ScaffoldOptions { EmitSequenceStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSequenceContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var sequence = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Sequence" &&
                            item.GetProperty("name").GetString()!.EndsWith(SequenceName, StringComparison.Ordinal));

                Assert.Contains("public async Task<", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Next{SequenceName}ValueAsync", contextCode, StringComparison.Ordinal);
                Assert.Contains("QueryUnchangedAsync<", contextCode, StringComparison.Ordinal);
                if (kind == ProviderKind.SqlServer)
                    Assert.Contains("NEXT VALUE FOR", contextCode, StringComparison.Ordinal);
                else
                    Assert.Contains("nextval('", contextCode, StringComparison.Ordinal);
                Assert.Contains("dataType", sequence.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSequenceAsync(connection, provider, kind);
            }
        }
    }

}
