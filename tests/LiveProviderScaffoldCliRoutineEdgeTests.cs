#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_emits_sqlserver_table_valued_parameter_routine_stub_on_live_provider()
    {
        var suffix = IdentifierSuffix();
        var routineName = "CliRoutineTvp" + suffix;
        var typeName = "CliRoutineItemType" + suffix;

        RunRoutineEdgeCliTest(
            ProviderKind.SqlServer,
            "norm_live_cli_sqlserver_tvp_routine_",
            "CliLiveSqlServerTvpRoutineCtx",
            (connection, provider) => SetupSqlServerTableValuedParameterRoutine(connection, provider, routineName, typeName),
            (connection, provider) => CleanupSqlServerTableValuedParameterRoutine(connection, provider, routineName, typeName),
            (contextCode, routines) =>
            {
                var routine = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                var metadata = routine.GetProperty("metadata");

                Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int? tenantId { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public DbParameter? items { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"table type (dbo.{typeName})", contextCode, StringComparison.Ordinal);
                Assert.Equal(2, metadata.GetProperty("parameterCount").GetInt32());
                var resultColumns = metadata.GetProperty("resultColumns").EnumerateArray().ToArray();
                Assert.Contains(resultColumns, item => item.GetProperty("name").GetString() == "Id");
                Assert.Contains(resultColumns, item => item.GetProperty("name").GetString() == "LineCount");
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{routineName}Result>> {routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
            });
    }

    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_overloaded_and_quoted_parameter_function_wrappers_on_live_provider()
    {
        var suffix = IdentifierSuffix();
        var overloadedName = "CliPostgresOverloadedRoutine" + suffix;
        var quotedName = "CliPostgresQuotedRoutine" + suffix;

        RunRoutineEdgeCliTest(
            ProviderKind.Postgres,
            "norm_live_cli_postgres_routine_edges_",
            "CliLivePostgresRoutineEdgesCtx",
            (connection, provider) => SetupPostgresRoutineEdges(connection, provider, overloadedName, quotedName),
            (connection, provider) => CleanupPostgresRoutineEdges(connection, provider, overloadedName, quotedName),
            (contextCode, routines) =>
            {
                var overloads = routines
                    .Where(item => item.GetProperty("kind").GetString() == "Routine" &&
                                   item.GetProperty("name").GetString()!.EndsWith(overloadedName, StringComparison.Ordinal))
                    .ToArray();
                Assert.Equal(2, overloads.Length);
                Assert.Contains($"public sealed class {overloadedName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {overloadedName}Parameters2", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {overloadedName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {overloadedName}Async2<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int? @value { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? @value { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("var casts = new[] { \"integer\" };", contextCode, StringComparison.Ordinal);
                Assert.Contains("var casts = new[] { \"text\" };", contextCode, StringComparison.Ordinal);
                Assert.Contains("Provider.ParamPrefix + \"p\" + i + \"::\" + casts[i]", contextCode, StringComparison.Ordinal);

                var quoted = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(quotedName, StringComparison.Ordinal));
                var quotedMetadata = quoted.GetProperty("metadata");
                Assert.Equal(2, quotedMetadata.GetProperty("parameterCount").GetInt32());
                var parameters = quotedMetadata.GetProperty("parameters").EnumerateArray().ToArray();
                Assert.Contains(parameters, item => item.GetProperty("name").GetString() == "tenant-id");
                Assert.Contains(parameters, item => item.GetProperty("name").GetString() == "search text");
                Assert.DoesNotContain($"public sealed class {quotedName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<TResult>> {quotedName}Async<TResult>(object?[]? arguments = null", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<TValue?> {quotedName}ValueAsync<TValue>(object?[]? arguments = null", contextCode, StringComparison.Ordinal);
                Assert.Contains("was scaffolded with 2 input parameters", contextCode, StringComparison.Ordinal);
            });
    }

    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_domain_routine_parameters_on_live_provider()
    {
        var suffix = IdentifierSuffix().ToLowerInvariant();
        var routineName = "CliPostgresDomainRoutine" + suffix;
        var emailDomainName = "cli_pg_routine_email_" + suffix;
        var ratingsDomainName = "cli_pg_routine_ratings_" + suffix;
        var statusEnumName = "cli_pg_routine_status_" + suffix;
        var statusDomainName = "cli_pg_routine_status_domain_" + suffix;

        RunRoutineEdgeCliTest(
            ProviderKind.Postgres,
            "norm_live_cli_postgres_domain_routine_",
            "CliLivePostgresDomainRoutineCtx",
            (connection, provider) => SetupPostgresDomainRoutine(
                connection,
                provider,
                routineName,
                emailDomainName,
                ratingsDomainName,
                statusEnumName,
                statusDomainName),
            (connection, provider) => CleanupPostgresDomainRoutine(
                connection,
                provider,
                routineName,
                emailDomainName,
                ratingsDomainName,
                statusEnumName,
                statusDomainName),
            (contextCode, routines) =>
            {
                var routine = Assert.Single(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                var parameters = routine.GetProperty("metadata").GetProperty("parameters").EnumerateArray().ToArray();

                Assert.Contains($"public sealed class {routineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? email { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public decimal[]? ratings { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? status { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains(
                    $"var casts = new[] {{ \"public.{emailDomainName}\", \"public.{ratingsDomainName}\", \"public.{statusDomainName}\" }};",
                    contextCode,
                    StringComparison.Ordinal);
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "email" &&
                    item.GetProperty("dataType").GetString()!.Contains(emailDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "string?" &&
                    item.GetProperty("dbType").GetString() == "String");
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "ratings" &&
                    item.GetProperty("dataType").GetString()!.Contains(ratingsDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "decimal[]?" &&
                    item.GetProperty("dbType").GetString() == "Object");
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "status" &&
                    item.GetProperty("dataType").GetString()!.Contains(statusDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "string?" &&
                    item.GetProperty("dbType").GetString() == "String");
            });
    }

    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_scalar_set_returning_function_wrapper_on_live_provider()
    {
        var suffix = IdentifierSuffix();
        var routineName = "CliPostgresSetofRoutine" + suffix;

        RunRoutineEdgeCliTest(
            ProviderKind.Postgres,
            "norm_live_cli_postgres_setof_routine_",
            "CliLivePostgresSetofRoutineCtx",
            (connection, provider) => SetupPostgresScalarSetReturningRoutine(connection, provider, routineName),
            (connection, provider) => CleanupPostgresScalarSetReturningRoutine(connection, provider, routineName),
            (contextCode, routines) =>
            {
                Assert.Contains(routines, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(routineName, StringComparison.Ordinal));
                Assert.Contains($"Task<List<TResult>> {routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public sealed class {routineName}Result", contextCode, StringComparison.Ordinal);
                Assert.Contains("public int Value { get; set; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"Task<List<{routineName}Result>> {routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<TResult> Stream{routineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains($"IAsyncEnumerable<{routineName}Result> Stream{routineName}Async", contextCode, StringComparison.Ordinal);
                Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"return QueryUnchangedAsync<{routineName}Result>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains("QueryUnchangedStreamAsync<TResult>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($"QueryUnchangedStreamAsync<{routineName}Result>(\"SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
                var methodStart = contextCode.IndexOf($"Task<List<TResult>> {routineName}Async<TResult>", StringComparison.Ordinal);
                Assert.True(methodStart >= 0);
                var nextMethod = contextCode.IndexOf("/// <summary>", methodStart + 1, StringComparison.Ordinal);
                var methodBlock = nextMethod > methodStart ? contextCode[methodStart..nextMethod] : contextCode[methodStart..];
                Assert.DoesNotContain("SELECT * FROM \" + invocation", methodBlock, StringComparison.Ordinal);
            });
    }

    private static void RunRoutineEdgeCliTest(
        ProviderKind kind,
        string outputPrefix,
        string contextName,
        Action<DbConnection, DatabaseProvider> setup,
        Action<DbConnection, DatabaseProvider> cleanup,
        Action<string, JsonElement[]> assertScaffold)
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), outputPrefix + IdentifierSuffix());
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                setup(connection, provider);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                $"--context {contextName} " +
                "--emit-routine-stubs",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, contextName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");
            Assert.True(File.Exists(warningJsonPath), "Routine edge scaffolding should preserve provider-owned routine metadata.");
            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var routines = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();
            assertScaffold(contextCode, routines);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanupConnection = Reopen(kind, connectionString);
                cleanup(cleanupConnection, provider);
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

    private static void SetupSqlServerTableValuedParameterRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName,
        string typeName)
    {
        CleanupSqlServerTableValuedParameterRoutine(connection, provider, routineName, typeName);

        Execute(connection,
            $"CREATE TYPE {provider.Escape("dbo")}.{provider.Escape(typeName)} AS TABLE ({provider.Escape("ProductId")} INT NOT NULL, {provider.Escape("Quantity")} INT NOT NULL)",
            $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT, @items {provider.Escape("dbo")}.{provider.Escape(typeName)} READONLY AS SELECT @tenantId AS Id, COUNT(*) AS LineCount FROM @items");
    }

    private static void CleanupSqlServerTableValuedParameterRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName,
        string typeName)
    {
        Execute(connection,
            $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}",
            $"IF TYPE_ID(N'dbo.{typeName}') IS NOT NULL DROP TYPE {provider.Escape("dbo")}.{provider.Escape(typeName)}");
    }

    private static void SetupPostgresRoutineEdges(
        DbConnection connection,
        DatabaseProvider provider,
        string overloadedName,
        string quotedName)
    {
        CleanupPostgresRoutineEdges(connection, provider, overloadedName, quotedName);

        var overloaded = provider.Escape("public") + "." + provider.Escape(overloadedName);
        var quoted = provider.Escape("public") + "." + provider.Escape(quotedName);
        var tenantId = provider.Escape("tenant-id");
        var searchText = provider.Escape("search text");
        Execute(connection,
            $"CREATE FUNCTION {overloaded}(value integer) RETURNS integer LANGUAGE SQL AS $$ SELECT value $$",
            $"CREATE FUNCTION {overloaded}(value text) RETURNS integer LANGUAGE SQL AS $$ SELECT char_length(value) $$",
            $"CREATE FUNCTION {quoted}({tenantId} integer, {searchText} text) RETURNS integer LANGUAGE SQL AS $$ SELECT {tenantId} + length({searchText}) $$");
    }

    private static void CleanupPostgresRoutineEdges(
        DbConnection connection,
        DatabaseProvider provider,
        string overloadedName,
        string quotedName)
    {
        Execute(connection,
            $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(quotedName)}(integer, text)",
            $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(overloadedName)}(integer)",
            $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(overloadedName)}(text)");
    }

    private static void SetupPostgresDomainRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName,
        string emailDomainName,
        string ratingsDomainName,
        string statusEnumName,
        string statusDomainName)
    {
        CleanupPostgresDomainRoutine(
            connection,
            provider,
            routineName,
            emailDomainName,
            ratingsDomainName,
            statusEnumName,
            statusDomainName);

        var emailDomain = provider.Escape("public") + "." + provider.Escape(emailDomainName);
        var ratingsDomain = provider.Escape("public") + "." + provider.Escape(ratingsDomainName);
        var statusEnum = provider.Escape("public") + "." + provider.Escape(statusEnumName);
        var statusDomain = provider.Escape("public") + "." + provider.Escape(statusDomainName);
        var routine = provider.Escape("public") + "." + provider.Escape(routineName);
        Execute(connection,
            $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active')",
            $"CREATE DOMAIN {emailDomain} AS varchar(320) CHECK (VALUE LIKE '%@%')",
            $"CREATE DOMAIN {ratingsDomain} AS numeric(10,2)[]",
            $"CREATE DOMAIN {statusDomain} AS {statusEnum}",
            $"CREATE FUNCTION {routine}(email {emailDomain}, ratings {ratingsDomain}, status {statusDomain}) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ratings, 1), 0) $$");
    }

    private static void CleanupPostgresDomainRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName,
        string emailDomainName,
        string ratingsDomainName,
        string statusEnumName,
        string statusDomainName)
    {
        var emailDomain = provider.Escape("public") + "." + provider.Escape(emailDomainName);
        var ratingsDomain = provider.Escape("public") + "." + provider.Escape(ratingsDomainName);
        var statusEnum = provider.Escape("public") + "." + provider.Escape(statusEnumName);
        var statusDomain = provider.Escape("public") + "." + provider.Escape(statusDomainName);
        var routine = provider.Escape("public") + "." + provider.Escape(routineName);
        Execute(connection,
            $"DROP FUNCTION IF EXISTS {routine}({emailDomain}, {ratingsDomain}, {statusDomain})",
            $"DROP DOMAIN IF EXISTS {statusDomain}",
            $"DROP DOMAIN IF EXISTS {ratingsDomain}",
            $"DROP DOMAIN IF EXISTS {emailDomain}",
            $"DROP TYPE IF EXISTS {statusEnum}");
    }

    private static void SetupPostgresScalarSetReturningRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName)
    {
        CleanupPostgresScalarSetReturningRoutine(connection, provider, routineName);

        Execute(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(tenantId integer) RETURNS SETOF integer LANGUAGE SQL AS $$ SELECT tenantId $$");
    }

    private static void CleanupPostgresScalarSetReturningRoutine(
        DbConnection connection,
        DatabaseProvider provider,
        string routineName)
    {
        Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer)");
    }
}
