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
    // Live provider scalar facet and provider-specific type scaffold parity tests.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_decimal_precision_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDecimalPrecisionAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_decimal_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldDecimalContext",
                    new ScaffoldOptions { Tables = new[] { DecimalPrecisionTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, DecimalPrecisionTable + ".cs"));
                Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(28,6)\")]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownDecimalPrecisionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Dynamic_scaffolding_preserves_decimal_precision_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupDecimalPrecisionAsync(connection, provider, kind);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, DecimalPrecisionTable);

                var amount = type.GetProperty("Amount")!;
                var column = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());

                Assert.Equal(typeof(decimal), amount.PropertyType);
                Assert.Equal("Amount", column.Name);
                Assert.Equal("decimal(28,6)", column.TypeName);
            }
            finally
            {
                await TeardownDecimalPrecisionAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_string_binary_facets_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: false);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_string_binary_facets_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldStringBinaryFacetContext",
                    new ScaffoldOptions { Tables = new[] { StringBinaryFacetTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, StringBinaryFacetTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldStringBinaryFacetContext.cs"));

                Assert.Contains("[MaxLength(40)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(12)]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Code).HasMaxLength(40)", contextCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12)", contextCode, StringComparison.Ordinal);

                if (kind == ProviderKind.SqlServer)
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Code).HasMaxLength(40).IsUnicode(false);", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsUnicode(false).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
                }
                else if (kind == ProviderKind.MySql)
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.Token).HasMaxLength(16).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains($"mb.Entity<{StringBinaryFacetTable}>().Property(e => e.FixedCode).HasMaxLength(12).IsFixedLength();", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Code).HasMaxLength(40).IsUnicode", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain($".Property(e => e.Token).HasMaxLength", contextCode, StringComparison.Ordinal);
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownStringBinaryFacetsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Dynamic_scaffolding_schema_signature_includes_string_binary_facets_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            var generator = new DynamicEntityTypeGenerator();
            await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: false);
            try
            {
                var firstSignature = generator.ComputeSchemaSignature(connection, StringBinaryFacetTable);
                var type = await generator.GenerateEntityTypeAsync(connection, StringBinaryFacetTable);
                var codeLength = Assert.Single(type.GetProperty("Code")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                var fixedCodeLength = Assert.Single(type.GetProperty("FixedCode")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());

                Assert.Equal(40, codeLength.Length);
                Assert.Equal(12, fixedCodeLength.Length);
                if (kind == ProviderKind.Postgres)
                {
                    Assert.Empty(type.GetProperty("Token")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false));
                }
                else
                {
                    var tokenLength = Assert.Single(type.GetProperty("Token")!.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                    Assert.Equal(16, tokenLength.Length);
                }

                await SetupStringBinaryFacetsAsync(connection, provider, kind, alternate: true);
                var secondSignature = generator.ComputeSchemaSignature(connection, StringBinaryFacetTable);

                Assert.NotEqual(firstSignature, secondSignature);
            }
            finally
            {
                await TeardownStringBinaryFacetsAsync(connection, provider, kind);
            }
        }
    }

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
    public async Task ScaffoldAsync_emits_mysql_json_and_year_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlTypedColumnTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_typed_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlTypedColumnContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { MySqlTypedColumnTable },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, MySqlTypedColumnTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldMySqlTypedColumnContext.cs"));

                Assert.Contains("public string Payload { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Status { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("public string Flags { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("FiscalYear { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("object FiscalYear", entityCode, StringComparison.Ordinal);
                Assert.Contains($".HasCheckConstraint(\"CK_{MySqlTypedColumnTable}_Status_Enum\", \"Status IN ('draft', 'paid', 'cancelled')\")", contextCode, StringComparison.Ordinal);
                Assert.Contains($".HasCheckConstraint(\"CK_{MySqlTypedColumnTable}_Flags_Set\", \"Flags IN ('', 'read', 'write', 'read,write', 'admin', 'read,admin', 'write,admin', 'read,write,admin')\")", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlTypedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_unsigned_columns_as_provider_specific_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsignedColumnTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_unsigned_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlUnsignedColumnContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { MySqlUnsignedColumnTable },
                        OverwriteFiles = false
                    });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, MySqlUnsignedColumnTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("UnsignedCount", entityCode, StringComparison.Ordinal);
                Assert.Contains("UnsignedTotal", entityCode, StringComparison.Ordinal);
                Assert.Contains("UnsignedAmount", entityCode, StringComparison.Ordinal);
                Assert.Contains("public uint UnsignedCount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public ulong UnsignedTotal { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"UnsignedAmount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public decimal UnsignedAmount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                var unsignedCountDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedCount" &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedCountDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedCountDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedCountDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var unsignedTotalDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedTotal" &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedTotalDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var unsignedAmountDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == "UnsignedAmount" &&
                    item.GetProperty("detail").GetString()!.Contains("decimal", StringComparison.OrdinalIgnoreCase) &&
                    item.GetProperty("detail").GetString()!.Contains("unsigned", StringComparison.OrdinalIgnoreCase));
                Assert.False(unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", unsignedAmountDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlUnsignedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_mysql_unsigned_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsignedColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, MySqlUnsignedColumnTable);

                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                Assert.Equal(typeof(uint), type.GetProperty("UnsignedCount")!.PropertyType);
                Assert.Equal(typeof(ulong), type.GetProperty("UnsignedTotal")!.PropertyType);
                var amount = type.GetProperty("UnsignedAmount")!;
                Assert.Equal(typeof(decimal), amount.PropertyType);
                var amountColumn = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());
                Assert.Equal("UnsignedAmount", amountColumn.Name);
                Assert.Equal("decimal(18,4)", amountColumn.TypeName);
            }
            finally
            {
                await TeardownMySqlUnsignedColumnTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_marks_unsafe_mysql_set_columns_read_only_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlUnsafeSetColumnTableAsync(connection, provider);
            try
            {
                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, MySqlUnsafeSetColumnTable);

                Assert.NotNull(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownMySqlUnsafeSetColumnTableAsync(connection, provider);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "Location", "geometry")]
    [InlineData(ProviderKind.Postgres, "Address", "inet")]
    [InlineData(ProviderKind.MySql, "Location", "point")]
    [InlineData(ProviderKind.Sqlite, "Location", "GEOMETRY")]
    public async Task ScaffoldAsync_reports_nonportable_provider_specific_columns_on_live_provider(
        ProviderKind kind,
        string columnName,
        string expectedDetail)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_specific_columns_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderSpecificColumnContext",
                    new ScaffoldOptions { Tables = new[] { ProviderSpecificColumnDiagnosticsTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderSpecificColumnDiagnosticsTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                Assert.Contains(columnName + " { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("name").GetString() == columnName &&
                    item.GetProperty("detail").GetString()!.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) &&
                    item.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean() &&
                    !item.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean() &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("provider-specific type", StringComparison.OrdinalIgnoreCase));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);
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
            }
            finally
            {
                await TeardownPostgresDomainColumnAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_sqlserver_alias_type_columns_with_base_type_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            var table = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeTable);
            var aliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeName);
            var decimalAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasDecimalTypeName);
            var binaryAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasBinaryTypeName);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_alias_type_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection, $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerAliasTypeContext",
                    new ScaffoldOptions { Tables = new[] { "dbo." + SqlServerAliasTypeTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerAliasTypeTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("public string Email { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(320)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[Column(\"Amount\", TypeName = \"decimal(18,4)\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public decimal Amount { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public byte[] Token { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                Assert.Contains("[MaxLength(64)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                var aliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasTypeName, StringComparison.Ordinal));
                Assert.Contains("nvarchar(320)", aliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(aliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(aliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", aliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var decimalAliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasDecimalTypeName, StringComparison.Ordinal));
                Assert.Contains("decimal(18,4)", decimalAliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(decimalAliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(decimalAliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", decimalAliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                var binaryAliasDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                    item.GetProperty("code").GetString() == "SCF104" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerAliasTypeTable &&
                    item.GetProperty("detail").GetString()!.Contains("user-defined type (dbo." + SqlServerAliasBinaryTypeName, StringComparison.Ordinal));
                Assert.Contains("varbinary(64)", binaryAliasDiagnostic.GetProperty("detail").GetString()!, StringComparison.Ordinal);
                Assert.False(binaryAliasDiagnostic.GetProperty("metadata").GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(binaryAliasDiagnostic.GetProperty("metadata").GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", binaryAliasDiagnostic.GetProperty("metadata").GetProperty("reason").GetString());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task Dynamic_scaffolding_handles_sqlserver_alias_type_columns_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            var table = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeTable);
            var aliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasTypeName);
            var decimalAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasDecimalTypeName);
            var binaryAliasType = provider.Escape("dbo") + "." + provider.Escape(SqlServerAliasBinaryTypeName);
            try
            {
                await ExecuteAsync(connection, $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL");
                await ExecuteAsync(connection, $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL");
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");

                var type = await new DynamicEntityTypeGenerator()
                    .GenerateEntityTypeAsync(connection, "dbo." + SqlServerAliasTypeTable);

                var tableAttribute = Assert.Single(type.GetCustomAttributes(typeof(TableAttribute), inherit: false).Cast<TableAttribute>());
                Assert.Equal("dbo", tableAttribute.Schema);
                Assert.Equal(SqlServerAliasTypeTable, tableAttribute.Name);
                Assert.Equal(typeof(int), type.GetProperty("Id")!.PropertyType);
                var email = type.GetProperty("Email")!;
                Assert.Equal(typeof(string), email.PropertyType);
                var maxLength = Assert.Single(email.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                Assert.Equal(320, maxLength.Length);
                var amount = type.GetProperty("Amount")!;
                Assert.Equal(typeof(decimal), amount.PropertyType);
                var amountColumn = Assert.Single(amount.GetCustomAttributes(typeof(ColumnAttribute), inherit: false).Cast<ColumnAttribute>());
                Assert.Equal("Amount", amountColumn.Name);
                Assert.Equal("decimal(18,4)", amountColumn.TypeName);
                var token = type.GetProperty("Token")!;
                Assert.Equal(typeof(byte[]), token.PropertyType);
                var tokenMaxLength = Assert.Single(token.GetCustomAttributes(typeof(MaxLengthAttribute), inherit: false).Cast<MaxLengthAttribute>());
                Assert.Equal(64, tokenMaxLength.Length);
                Assert.Null(type.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
            }
            finally
            {
                await TeardownSqlServerAliasTypeColumnAsync(connection, provider);
            }
        }
    }

}
