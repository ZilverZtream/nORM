#nullable enable

using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // PostgreSQL-specific provider type scaffold parity tests.

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_uuid_and_array_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresTypedColumnTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_typed_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresTypedColumnContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "public." + PostgresTypedColumnTable },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, PostgresTypedColumnTable + ".cs"));

                Assert.Contains("using System;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public Guid TraceId { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public int[]? Scores { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string[]? Tags { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresTypedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_postgres_uuid_and_array_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresTypedColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, "public." + PostgresTypedColumnTable);

                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                Assert.Equal(typeof(Guid), type.GetProperty("TraceId")!.PropertyType);
                Assert.Equal(typeof(int[]), type.GetProperty("Scores")!.PropertyType);
                Assert.Equal(typeof(string[]), type.GetProperty("Tags")!.PropertyType);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownPostgresTypedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_postgres_domain_columns_with_underlying_type_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownPostgresDomainColumnAsync(connection, provider);
            var table = provider.Escape("public") + "." + provider.Escape(PostgresDomainTable);
            var domain = provider.Escape("public") + "." + provider.Escape(PostgresDomainName);
            var scoreDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainScoreName);
            var scoreArrayDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainScoreArrayName);
            var statusEnum = provider.Escape("public") + "." + provider.Escape(PostgresEnumName);
            var statusDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainStatusName);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_domain_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection, $"CREATE DOMAIN {domain} AS varchar(320) CHECK (VALUE LIKE '%@%')");
                await ExecuteAsync(connection, $"CREATE DOMAIN {scoreDomain} AS numeric(18,4) CHECK (VALUE >= 0)");
                await ExecuteAsync(connection, $"CREATE DOMAIN {scoreArrayDomain} AS integer[]");
                await ExecuteAsync(connection, $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active', 'archived')");
                await ExecuteAsync(connection, $"CREATE DOMAIN {statusDomain} AS {statusEnum}");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("Email")} {domain} NOT NULL, {provider.Escape("Score")} {scoreDomain} NOT NULL, {provider.Escape("Scores")} {scoreArrayDomain} NOT NULL, {provider.Escape("Status")} {statusDomain} NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresDomainContext",
                    new ScaffoldOptions { Tables = new[] { "public." + PostgresDomainTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, PostgresDomainTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresDomainContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"Score\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public decimal Score { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public int[] Scores { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                Assert.Contains($".HasCheckConstraint(\"CK_{PostgresDomainTable}_Status_Enum\", \"Status IN ('draft', 'active', 'archived')\")", contextCode, StringComparison.Ordinal);
                var emailDomain = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "public." + PostgresDomainTable &&
                    item.GetProperty("detail").GetString()!.Contains("DOMAIN (public." + PostgresDomainName, StringComparison.Ordinal));
                Assert.Contains("character varying(320)", emailDomain.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(emailDomain.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(emailDomain.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", emailDomain.GetProperty("metadata").GetProperty("reason").GetString());
                var scoreDomainDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "public." + PostgresDomainTable &&
                    item.GetProperty("name").GetString() == "Score" &&
                    item.GetProperty("detail").GetString()!.Contains("DOMAIN (public." + PostgresDomainScoreName, StringComparison.Ordinal));
                Assert.Contains("numeric(18,4)", scoreDomainDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(scoreDomainDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(scoreDomainDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", scoreDomainDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var scoreArrayDomainDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "public." + PostgresDomainTable &&
                    item.GetProperty("detail").GetString()!.Contains("DOMAIN (public." + PostgresDomainScoreArrayName, StringComparison.Ordinal));
                Assert.False(scoreArrayDomainDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(scoreArrayDomainDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", scoreArrayDomainDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var statusDomainDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "public." + PostgresDomainTable &&
                    item.GetProperty("name").GetString() == "Status" &&
                    item.GetProperty("detail").GetString()!.Contains("DOMAIN (public." + PostgresDomainStatusName, StringComparison.Ordinal));
                Assert.Contains("ENUM (public." + PostgresEnumName, statusDomainDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(statusDomainDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(statusDomainDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", statusDomainDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresDomainColumnAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_postgres_domain_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownPostgresDomainColumnAsync(connection, provider);
            var table = provider.Escape("public") + "." + provider.Escape(PostgresDomainTable);
            var domain = provider.Escape("public") + "." + provider.Escape(PostgresDomainName);
            var scoreDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainScoreName);
            var scoreArrayDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainScoreArrayName);
            var statusEnum = provider.Escape("public") + "." + provider.Escape(PostgresEnumName);
            var statusDomain = provider.Escape("public") + "." + provider.Escape(PostgresDomainStatusName);
            try
            {
                await ExecuteAsync(connection, $"CREATE DOMAIN {domain} AS varchar(320) CHECK (VALUE LIKE '%@%')");
                await ExecuteAsync(connection, $"CREATE DOMAIN {scoreDomain} AS numeric(18,4) CHECK (VALUE >= 0)");
                await ExecuteAsync(connection, $"CREATE DOMAIN {scoreArrayDomain} AS integer[]");
                await ExecuteAsync(connection, $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active', 'archived')");
                await ExecuteAsync(connection, $"CREATE DOMAIN {statusDomain} AS {statusEnum}");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("Email")} {domain} NOT NULL, {provider.Escape("Score")} {scoreDomain} NOT NULL, {provider.Escape("Scores")} {scoreArrayDomain} NOT NULL, {provider.Escape("Status")} {statusDomain} NOT NULL)");

                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, "public." + PostgresDomainTable);

                var tableAttribute = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());
                Assert.Equal("public", tableAttribute.Schema);
                Assert.Equal(PostgresDomainTable, tableAttribute.Name);
                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                var email = type.GetProperty("Email")!;
                Assert.Equal(typeof(string), email.PropertyType);
                var maxLength = Assert.Single(email.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                Assert.Equal(320, maxLength.Length);
                var score = type.GetProperty("Score")!;
                Assert.Equal(typeof(decimal), score.PropertyType);
                var scoreColumn = Assert.Single(score.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());
                Assert.Equal("Score", scoreColumn.Name);
                Assert.Equal("decimal(18,4)", scoreColumn.TypeName);
                Assert.Equal(typeof(int[]), type.GetProperty("Scores")!.PropertyType);
                Assert.Equal(typeof(string), type.GetProperty("Status")!.PropertyType);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownPostgresDomainColumnAsync(connection, provider);
            }
        }
    }
}
