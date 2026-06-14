#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider routine scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_emits_routine_stubs_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupRoutineAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRoutineContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
                Assert.True(File.Exists(warningJsonPath));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains($"Task<List<TResult>> {RoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {RoutineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains(
                    kind == ProviderKind.Postgres
                        ? "public int? tenantid { get; init; }"
                        : "public int? tenantId { get; init; }",
                    contextCode,
                    StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {RoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains(RoutineName, contextCode, StringComparison.Ordinal);
                Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", contextCode, StringComparison.Ordinal);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(RoutineName, StringComparison.Ordinal));
                if (kind == ProviderKind.SqlServer)
                {
                    var routine = Assert.Single(skippedObjects, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(RoutineName, StringComparison.Ordinal));
                    Assert.Equal(1, routine.GetProperty("metadata").GetProperty("outputParameterCount").GetInt32());
                    var resultColumns = routine.GetProperty("metadata").GetProperty("resultColumns").EnumerateArray().ToArray();
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Id" &&
                        item.GetProperty("dataType").GetString() == "int");
                    Assert.Contains(resultColumns, item =>
                        item.GetProperty("name").GetString() == "Name" &&
                        item.GetProperty("dataType").GetString()!.StartsWith("nvarchar", StringComparison.OrdinalIgnoreCase));
                    Assert.Contains($"public sealed class {RoutineName}Result", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<{RoutineName}Result>> {RoutineName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<{RoutineName}Result> Stream{RoutineName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"public static OutputParameter[] Create{RoutineName}OutputParameters()", contextCode, StringComparison.Ordinal);
                    Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", contextCode, StringComparison.Ordinal);
                }
                if (kind == ProviderKind.Postgres)
                {
                    var routine = Assert.Single(skippedObjects, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(RoutineName, StringComparison.Ordinal));
                    var metadata = routine.GetProperty("metadata");
                    var parameters = metadata.GetProperty("parameters").EnumerateArray().ToArray();
                    var parameter = Assert.Single(parameters, item => item.GetProperty("mode").GetString() == "IN");
                    Assert.Equal("IN", parameter.GetProperty("mode").GetString());
                    Assert.Contains($"public sealed class {RoutineName}Result", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public int Id { get; set; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("public string Name { get; set; } = default!;", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"Task<List<{RoutineName}Result>> {RoutineName}Async", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"IAsyncEnumerable<{RoutineName}Result> Stream{RoutineName}Async", contextCode, StringComparison.Ordinal);
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownRoutineAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlserver_no_result_procedure_as_non_query_wrapper()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerNonQueryRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_nonquery_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerNonQueryRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqlServerNonQueryRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(RoutineNonQueryName, StringComparison.Ordinal));
                var metadata = routine.GetProperty("metadata");

                Assert.Equal(2, metadata.GetProperty("outputParameterCount").GetInt32());
                Assert.Empty(metadata.GetProperty("resultColumns").EnumerateArray());
                Assert.Contains($"Task<int> {RoutineNonQueryName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<StoredProcedureNonQueryResult> {RoutineNonQueryName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{RoutineNonQueryName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureNonQueryWithOutputAsync(Provider.Escape(\"dbo\") + \".\" + Provider.Escape(\"{RoutineNonQueryName}\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public static OutputParameter[] Create{RoutineNonQueryName}OutputParameters()", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"status\", System.Data.DbType.String, 32)", contextCode, StringComparison.Ordinal);
                Assert.Contains("new OutputParameter(\"return\", System.Data.DbType.Int32, null, System.Data.ParameterDirection.ReturnValue)", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Task<List<TResult>> {RoutineNonQueryName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"StoredProcedureResult<TResult> {RoutineNonQueryName}WithOutputAsync", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"Stream{RoutineNonQueryName}Async", contextCode, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerNonQueryRoutineAsync(connection, provider);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Postgres)]
    public async Task ScaffoldAsync_emits_postgres_set_returning_function_as_table_valued_wrapper(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresSetReturningRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_setof_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresSetReturningContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresSetReturningContext.cs"));
                Assert.Contains($"Task<List<TResult>> {PostgresSetReturningRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {PostgresSetReturningRoutineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int Value { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{PostgresSetReturningRoutineName}Result>> {PostgresSetReturningRoutineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<TResult> Stream{PostgresSetReturningRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{PostgresSetReturningRoutineName}Result> Stream{PostgresSetReturningRoutineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"return QueryUnchangedAsync<{PostgresSetReturningRoutineName}Result>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains("QueryUnchangedStreamAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"QueryUnchangedStreamAsync<{PostgresSetReturningRoutineName}Result>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                var methodStart = contextCode.IndexOf($"Task<List<TResult>> {PostgresSetReturningRoutineName}Async<TResult>", StringComparison.Ordinal);
                Assert.True(methodStart >= 0);
                var nextMethod = contextCode.IndexOf("/// <summary>", methodStart + 1, StringComparison.Ordinal);
                var methodBlock = nextMethod > methodStart ? contextCode[methodStart..nextMethod] : contextCode[methodStart..];
                Assert.DoesNotContain("SELECT * FROM \" + invocation", methodBlock, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresSetReturningRoutineAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_array_and_uuid_routine_parameters_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresTypedRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_typed_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresTypedRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresTypedRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(PostgresTypedRoutineName, StringComparison.Ordinal));
                var parameters = routine.GetProperty("metadata").GetProperty("parameters").EnumerateArray().ToArray();

                Assert.Contains($"public sealed class {PostgresTypedRoutineName}Parameters", contextCode, StringComparison.Ordinal);
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
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresTypedRoutineAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_overloaded_function_wrappers_without_collisions()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresOverloadedRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_overloaded_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresOverloadedRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresOverloadedRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray()
                    .Where(item => item.GetProperty("kind").GetString() == "Routine" &&
                                   item.GetProperty("name").GetString()!.EndsWith(PostgresOverloadedRoutineName, StringComparison.Ordinal))
                    .ToArray();

                Assert.Equal(2, routines.Length);
                Assert.Contains($"public sealed class {PostgresOverloadedRoutineName}IntegerParameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {PostgresOverloadedRoutineName}TextParameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {PostgresOverloadedRoutineName}IntegerAsync<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {PostgresOverloadedRoutineName}TextAsync<TResult>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"{PostgresOverloadedRoutineName}Async2", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int? @value { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? @value { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("var casts = new[] { \"integer\" };", contextCode, StringComparison.Ordinal);
                Assert.Contains("var casts = new[] { \"text\" };", contextCode, StringComparison.Ordinal);
                Assert.Contains("Provider.ParamPrefix + \"p\" + i + \"::\" + casts[i]", contextCode, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresOverloadedRoutineAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_quoted_parameter_function_as_positional_arguments()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresQuotedParameterRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_quoted_parameter_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresQuotedParameterRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresQuotedParameterRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(PostgresQuotedParameterRoutineName, StringComparison.Ordinal));
                var metadata = routine.GetProperty("metadata");

                Assert.Equal(2, metadata.GetProperty("parameterCount").GetInt32());
                var parameters = metadata.GetProperty("parameters").EnumerateArray().ToArray();
                Assert.Contains(parameters, item => item.GetProperty("name").GetString() == "tenant-id");
                Assert.Contains(parameters, item => item.GetProperty("name").GetString() == "search text");
                Assert.DoesNotContain($"public sealed class {PostgresQuotedParameterRoutineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {PostgresQuotedParameterRoutineName}Async<TResult>(object?[]? arguments = null", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<TValue?> {PostgresQuotedParameterRoutineName}ValueAsync<TValue>(object?[]? arguments = null", contextCode, StringComparison.Ordinal);
                Assert.Contains("was scaffolded with 2 input parameters", contextCode, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresQuotedParameterRoutineAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_mysql_unsigned_routine_parameters_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsignedRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_unsigned_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlUnsignedRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldMySqlUnsignedRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(MySqlUnsignedRoutineName, StringComparison.Ordinal));
                var metadata = routine.GetProperty("metadata");

                Assert.Contains($"public sealed class {MySqlUnsignedRoutineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains("public uint? customer_id { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public ulong? max_id { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public ushort? rank { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public byte? flag { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Equal("scalar-function", metadata.GetProperty("callShape").GetString());
                Assert.Equal("int", metadata.GetProperty("dataType").GetString());
                Assert.Contains($"Task<List<TResult>> {MySqlUnsignedRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"private sealed class {MySqlUnsignedRoutineName}ValueResult<TValue>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<TValue?> {MySqlUnsignedRoutineName}ValueAsync<TValue>", contextCode, StringComparison.Ordinal);
                Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlUnsignedRoutineAsync(connection, provider);
            }
        }
    }

}
