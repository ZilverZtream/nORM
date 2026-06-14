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
    [Fact]
    public async Task ScaffoldAsync_emits_postgres_domain_routine_parameters_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresDomainRoutineAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_domain_routine_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresDomainRoutineContext",
                    new ScaffoldOptions { EmitRoutineStubs = true, OverwriteFiles = false });

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresDomainRoutineContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var routine = Assert.Single(
                    warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(),
                    item => item.GetProperty("kind").GetString() == "Routine" &&
                            item.GetProperty("name").GetString()!.EndsWith(PostgresDomainRoutineName, StringComparison.Ordinal));
                var parameters = routine.GetProperty("metadata").GetProperty("parameters").EnumerateArray().ToArray();

                Assert.Contains($"public sealed class {PostgresDomainRoutineName}Parameters", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? email { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public decimal[]? ratings { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains("public string? status { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains(
                    $"var casts = new[] {{ \"public.{PostgresRoutineEmailDomainName}\", \"public.{PostgresRoutineRatingsDomainName}\", \"public.{PostgresRoutineStatusDomainName}\" }};",
                    contextCode,
                    StringComparison.Ordinal);
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "email" &&
                    item.GetProperty("dataType").GetString()!.Contains(PostgresRoutineEmailDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "string?" &&
                    item.GetProperty("dbType").GetString() == "String");
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "ratings" &&
                    item.GetProperty("dataType").GetString()!.Contains(PostgresRoutineRatingsDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "decimal[]?" &&
                    item.GetProperty("dbType").GetString() == "Object");
                Assert.Contains(parameters, item =>
                    item.GetProperty("name").GetString() == "status" &&
                    item.GetProperty("dataType").GetString()!.Contains(PostgresRoutineStatusDomainName, StringComparison.Ordinal) &&
                    item.GetProperty("clrType").GetString() == "string?" &&
                    item.GetProperty("dbType").GetString() == "String");
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresDomainRoutineAsync(connection, provider);
            }
        }
    }
}
