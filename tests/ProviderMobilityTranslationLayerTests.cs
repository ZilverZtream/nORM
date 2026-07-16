using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Cli;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using nORM.Sample.Store;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class ProviderMobilityTranslationLayerTests
{
    [Fact]
    public void Translation_layer_classifies_core_provider_mobility_contract()
    {
        var generated = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.GeneratedLinqQuery);
        Assert.Equal(ProviderMobilitySupport.Portable, generated.Support);
        Assert.True(generated.StrictRuntimeAllowed);
        Assert.Equal("Info", generated.CertificationSeverity);

        var temporal = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.NormManagedTemporal);
        Assert.Equal(ProviderMobilitySupport.Emulated, temporal.Support);
        Assert.True(temporal.StrictRuntimeAllowed);

        var rawSql = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.RawSql);
        Assert.Equal(ProviderMobilitySupport.ProviderBound, rawSql.Support);
        Assert.False(rawSql.StrictRuntimeAllowed);
        Assert.Equal("Error", rawSql.CertificationSeverity);

        var bootstrap = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.ProviderBootstrap);
        Assert.Equal(ProviderMobilitySupport.ProviderBound, bootstrap.Support);
        Assert.True(bootstrap.StrictRuntimeAllowed);
        Assert.Equal("Warning", bootstrap.CertificationSeverity);
    }

    [Theory]
    [InlineData("raw-sql", ProviderMobilityFeature.RawSql)]
    [InlineData("stored-procedure", ProviderMobilityFeature.StoredProcedure)]
    [InlineData("custom-sql-function", ProviderMobilityFeature.CustomSqlFunction)]
    [InlineData("dynamic-table-query", ProviderMobilityFeature.DynamicTableQuery)]
    [InlineData("direct-transaction-access", ProviderMobilityFeature.DirectTransactionAccess)]
    [InlineData("provider-bootstrap-connection", ProviderMobilityFeature.ProviderBootstrap)]
    [InlineData("client-projection-tail", ProviderMobilityFeature.ExplicitClientProjectionTail)]
    [InlineData("schema-unsupported-clr-type", ProviderMobilityFeature.SchemaUnsupportedClrType)]
    [InlineData("schema-provider-specific-default", ProviderMobilityFeature.SchemaProviderSpecificDefault)]
    [InlineData("schema-missing-primary-key", ProviderMobilityFeature.SchemaMissingPrimaryKey)]
    [InlineData("provider-target-open", ProviderMobilityFeature.ProviderTargetOpen)]
    [InlineData("provider-target-capability", ProviderMobilityFeature.ProviderTargetCapability)]
    [InlineData("scan-path-missing", ProviderMobilityFeature.SchemaInvalidMetadata)]
    [InlineData("certification-unclassified-finding", ProviderMobilityFeature.CertificationUnclassifiedFinding)]
    [InlineData("constrained-linq-shape", ProviderMobilityFeature.ConstrainedLinqShape)]
    [InlineData("unsupported-linq-shape", ProviderMobilityFeature.UnsupportedLinqShape)]
    public void Translation_layer_maps_certification_finding_kinds(string kind, ProviderMobilityFeature feature)
    {
        Assert.True(ProviderMobilityTranslator.TryDecideFindingKind(kind, out var decision));
        Assert.Equal(feature, decision.Feature);
    }

    [Fact]
    public void Source_scanner_rule_kinds_are_classified_by_translation_layer()
    {
        var ruleKinds = GetRuleKinds(typeof(ProviderMobilitySourceScanner), "CSharpRules")
            .Concat(GetRuleKinds(typeof(ProviderMobilitySourceScanner), "SqlRules"))
            .Concat(GetRuleKinds(typeof(ProviderMobilitySourceScanner), "ProjectRules"))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static kind => kind, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        Assert.NotEmpty(ruleKinds);
        foreach (var kind in ruleKinds)
        {
            Assert.True(
                ProviderMobilityTranslator.TryDecideFindingKind(kind, out _),
                $"Provider mobility source scanner rule kind '{kind}' is missing from ProviderMobilityTranslator.");
        }
    }

    [Fact]
    public void Sample_and_cli_source_scanners_share_translation_layer_finding_kinds()
    {
        var cliRules = GetRules(typeof(ProviderMobilitySourceScanner), "CSharpRules")
            .Concat(GetRules(typeof(ProviderMobilitySourceScanner), "SqlRules"))
            .Concat(GetRules(typeof(ProviderMobilitySourceScanner), "ProjectRules"))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static rule => rule, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var sampleRules = GetRules(typeof(ProviderMobilityScanner), "Rules")
            .Concat(GetRules(typeof(ProviderMobilityScanner), "SqlRules"))
            .Concat(GetRules(typeof(ProviderMobilityScanner), "ProjectRules"))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(static rule => rule, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        Assert.Equal(cliRules, sampleRules);
        foreach (var kind in sampleRules.Select(static rule => rule.Split('|')[1]).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            Assert.True(
                ProviderMobilityTranslator.TryDecideFindingKind(kind, out _),
                $"Sample provider-swap scanner rule kind '{kind}' is missing from ProviderMobilityTranslator.");
        }
    }

    [Theory]
    [InlineData("raw-sql", "Error")]
    [InlineData("stored-procedure-definition", "Error")]
    [InlineData("sql-server-specific-sql", "Error")]
    [InlineData("provider-bootstrap-connection", "Warning")]
    [InlineData("provider-specific-package", "Warning")]
    [InlineData("client-projection-tail", "Warning")]
    [InlineData("schema-missing-primary-key", "Warning")]
    [InlineData("schema-provider-specific-default", "Error")]
    [InlineData("provider-target-open", "Error")]
    [InlineData("certification-unclassified-finding", "Error")]
    public void Translation_layer_pins_certification_severity_for_public_finding_kinds(string kind, string severity)
    {
        Assert.True(ProviderMobilityTranslator.TryDecideFindingKind(kind, out var decision));
        Assert.Equal(severity, decision.CertificationSeverity);
    }

    [Fact]
    public void Provider_version_decisions_accept_supported_and_reject_unsupported_targets()
    {
        var capabilities = new ProviderCapabilities(
            "TestSQL",
            new Version(12, 0),
            100,
            supportsJson: true,
            supportsTemporalVersioning: true,
            supportsNativeBulkInsert: true,
            supportsSavepoints: true,
            "Test provider.");

        var supported = ProviderMobilityTranslator.DecideProviderVersion(capabilities, new Version(12, 1));
        Assert.Equal(ProviderMobilityProviderFeature.ServerVersion, supported.Feature);
        Assert.Equal(ProviderMobilitySupport.Portable, supported.Support);
        Assert.True(supported.StrictRuntimeAllowed);
        Assert.Equal("Info", supported.CertificationSeverity);

        var unknown = ProviderMobilityTranslator.DecideProviderVersion(capabilities, null);
        Assert.Equal(ProviderMobilitySupport.Unsupported, unknown.Support);
        Assert.False(unknown.StrictRuntimeAllowed);
        Assert.Equal("Error", unknown.CertificationSeverity);
        Assert.Contains("could not be determined", unknown.Reason, StringComparison.Ordinal);

        var unsupported = ProviderMobilityTranslator.DecideProviderVersion(capabilities, new Version(11, 9));
        Assert.Equal(ProviderMobilitySupport.Unsupported, unsupported.Support);
        Assert.False(unsupported.StrictRuntimeAllowed);
        Assert.Equal("Error", unsupported.CertificationSeverity);
        Assert.Contains("Minimum supported version is 12.0", unsupported.Reason, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("3.46.1", 3, 46)]
    [InlineData("PostgreSQL 12.17 on x86_64-pc-linux-gnu", 12, 17)]
    [InlineData("Microsoft SQL Server 16.0.1000.6", 16, 0)]
    [InlineData("8.0.36-0ubuntu0.22.04.1", 8, 0)]
    public void Provider_version_parser_extracts_comparable_version(string text, int major, int minor)
    {
        var version = ProviderMobilityTranslator.ParseProviderVersion(text);

        Assert.NotNull(version);
        Assert.Equal(major, version!.Major);
        Assert.Equal(minor, version.Minor);
    }

    [Fact]
    public void Provider_version_parser_returns_null_for_unknown_text()
    {
        Assert.Null(ProviderMobilityTranslator.ParseProviderVersion(null));
        Assert.Null(ProviderMobilityTranslator.ParseProviderVersion("server-version-unavailable"));
    }

    [Fact]
    public void Provider_capability_profile_classifies_version_features_and_emulated_bulk()
    {
        var profile = ProviderMobilityTranslator.DecideProviderCapabilityProfile(
            new SqliteProvider().Capabilities,
            new Version(3, 46));

        Assert.Contains(profile, decision =>
            decision.Feature == ProviderMobilityProviderFeature.ServerVersion &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.StrictRuntimeAllowed);

        Assert.Contains(profile, decision =>
            decision.Feature == ProviderMobilityProviderFeature.BulkInsert &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.StrictRuntimeAllowed &&
            decision.CertificationSeverity == "Warning");

        Assert.Contains(profile, decision =>
            decision.Feature == ProviderMobilityProviderFeature.ParameterLimit &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.StrictRuntimeAllowed &&
            decision.CertificationSeverity == "Warning");
    }

    [Fact]
    public void Descriptor_only_provider_profile_does_not_fake_actual_server_version()
    {
        var profile = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqliteProvider());

        var version = Assert.Single(profile.Where(decision =>
            decision.Feature == ProviderMobilityProviderFeature.ServerVersion));
        Assert.Equal(ProviderMobilitySupport.Portable, version.Support);
        Assert.Null(version.ActualServerVersion);
        Assert.Equal(new Version(3, 25), version.MinimumServerVersion);
        Assert.Contains("descriptor evidence", version.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void Provider_capability_profile_has_one_decision_per_provider_feature()
    {
        var profile = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqlServerProvider());

        Assert.Equal(Enum.GetValues<ProviderMobilityProviderFeature>().Length, profile.Count);
        foreach (var feature in Enum.GetValues<ProviderMobilityProviderFeature>())
            Assert.Single(profile.Where(decision => decision.Feature == feature));
    }

    [Fact]
    public void Provider_implementation_profile_exposes_translation_strategy_quirks()
    {
        var sqlServer = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqlServerProvider());
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.RowTupleComparison &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.CertificationSeverity == "Warning");
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.OrderedStringAggregate &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.CertificationSeverity == "Warning" &&
            decision.MinimumServerVersion == new Version(14, 0));
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.RegexTranslation &&
            decision.Support == ProviderMobilitySupport.Unsupported &&
            decision.CertificationSeverity == "Warning");

        var sqlite = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqliteProvider());
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.OrderedStringAggregate &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.CertificationSeverity == "Warning");
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.RegexTranslation &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.CertificationSeverity == "Info" &&
            decision.Reason.Contains("nORM-registered", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Provider_implementation_profile_rejects_too_old_feature_specific_server_versions()
    {
        var sqlServer2016 = ProviderMobilityTranslator.DecideProviderImplementationProfile(
            new SqlServerProvider(),
            new Version(13, 0),
            requireActualServerVersion: true);

        Assert.Contains(sqlServer2016, decision =>
            decision.Feature == ProviderMobilityProviderFeature.OrderedStringAggregate &&
            decision.Support == ProviderMobilitySupport.Unsupported &&
            decision.CertificationSeverity == "Error" &&
            decision.MinimumServerVersion == new Version(14, 0));

        var sqlServer2019 = ProviderMobilityTranslator.DecideProviderImplementationProfile(
            new SqlServerProvider(),
            new Version(15, 0),
            requireActualServerVersion: true);

        Assert.Contains(sqlServer2019, decision =>
            decision.Feature == ProviderMobilityProviderFeature.OrderedStringAggregate &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.CertificationSeverity == "Info");
    }

    [Fact]
    public void Provider_implementation_profile_stamps_actual_server_version_on_all_rows()
    {
        var actual = new Version(15, 0);
        var profile = ProviderMobilityTranslator.DecideProviderImplementationProfile(
            new SqlServerProvider(),
            actual,
            requireActualServerVersion: true);

        Assert.All(profile, decision => Assert.Equal(actual, decision.ActualServerVersion));
    }

    [Fact]
    public void Provider_implementation_profile_exposes_core_sql_translation_axes()
    {
        var sqlite = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqliteProvider());
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.PagingTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("LIMIT/OFFSET", StringComparison.Ordinal));
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.DecimalComparisonNormalization &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.CertificationSeverity == "Warning" &&
            decision.Reason.Contains("CAST", StringComparison.Ordinal));
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.StringConcatTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("||", StringComparison.Ordinal));
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.JsonPathTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("rejects unsafe path", StringComparison.Ordinal));
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.TypeConversionTranslation &&
            decision.Support == ProviderMobilitySupport.Portable);
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.CharacterTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("unicode", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.FormattingTranslation &&
            decision.Support == ProviderMobilitySupport.Portable);
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.TemporalClockSource &&
            decision.Support == ProviderMobilitySupport.Portable);
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.TemporalConstructionTranslation &&
            decision.Support == ProviderMobilitySupport.Portable);
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.TemporalArithmeticTranslation &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.CertificationSeverity == "Warning");
        Assert.Contains(sqlite, decision =>
            decision.Feature == ProviderMobilityProviderFeature.SqlStatementLengthLimit &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.Reason.Contains("1000000", StringComparison.Ordinal));

        var sqlServer = ProviderMobilityTranslator.DecideProviderImplementationProfile(new SqlServerProvider());
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.PagingTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("OFFSET/FETCH", StringComparison.Ordinal));
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.CaseSensitiveStringComparison &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.Reason.Contains("COLLATE", StringComparison.Ordinal));
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.StringPredicateTranslation &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.Reason.Contains("DATALENGTH", StringComparison.Ordinal));
        Assert.Contains(sqlServer, decision =>
            decision.Feature == ProviderMobilityProviderFeature.InsertOrIgnoreTranslation &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.Reason.Contains("IF NOT EXISTS", StringComparison.Ordinal));

        var postgres = ProviderMobilityTranslator.DecideProviderImplementationProfile(new PostgresProvider());
        Assert.Contains(postgres, decision =>
            decision.Feature == ProviderMobilityProviderFeature.BitwiseXorTranslation &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("#", StringComparison.Ordinal));
        Assert.Contains(postgres, decision =>
            decision.Feature == ProviderMobilityProviderFeature.NullSemantics &&
            decision.Support == ProviderMobilitySupport.Portable &&
            decision.Reason.Contains("IS NOT DISTINCT FROM", StringComparison.Ordinal));

        var mysql = ProviderMobilityTranslator.DecideProviderImplementationProfile(new MySqlProvider());
        Assert.Contains(mysql, decision =>
            decision.Feature == ProviderMobilityProviderFeature.CudSubqueryRewrite &&
            decision.Support == ProviderMobilitySupport.Emulated &&
            decision.Reason.Contains("double-wrap", StringComparison.Ordinal));
        Assert.Contains(mysql, decision =>
            decision.Feature == ProviderMobilityProviderFeature.WindowFunctionTranslation &&
            decision.Support == ProviderMobilitySupport.Portable);
    }

    [Fact]
    public void Supported_v1_provider_profiles_have_no_error_level_generated_target_decisions()
    {
        var providers = new DatabaseProvider[]
        {
            new SqliteProvider(),
            new SqlServerProvider(),
            new PostgresProvider(),
            new MySqlProvider()
        };

        foreach (var provider in providers)
        {
            var profile = ProviderMobilityTranslator.DecideProviderImplementationProfile(provider);
            Assert.DoesNotContain(profile, decision =>
                decision.CertificationSeverity == "Error" &&
                decision.Feature is ProviderMobilityProviderFeature.ServerVersion
                    or ProviderMobilityProviderFeature.JsonTranslation
                    or ProviderMobilityProviderFeature.JsonPathTranslation
                    or ProviderMobilityProviderFeature.TemporalVersioning
                    or ProviderMobilityProviderFeature.BulkInsert
                    or ProviderMobilityProviderFeature.Savepoints
                    or ProviderMobilityProviderFeature.ParameterLimit
                    or ProviderMobilityProviderFeature.RowTupleComparison
                    or ProviderMobilityProviderFeature.OrderedStringAggregate
                    or ProviderMobilityProviderFeature.RegexTranslation
                    or ProviderMobilityProviderFeature.IdentifierEscaping
                    or ProviderMobilityProviderFeature.ParameterBinding
                    or ProviderMobilityProviderFeature.PagingTranslation
                    or ProviderMobilityProviderFeature.BooleanPredicateTranslation
                    or ProviderMobilityProviderFeature.NullSemantics
                    or ProviderMobilityProviderFeature.LikeEscapeTranslation
                    or ProviderMobilityProviderFeature.StringConcatTranslation
                    or ProviderMobilityProviderFeature.TypeConversionTranslation
                    or ProviderMobilityProviderFeature.StringPredicateTranslation
                    or ProviderMobilityProviderFeature.CharacterTranslation
                    or ProviderMobilityProviderFeature.FormattingTranslation
                    or ProviderMobilityProviderFeature.DateTimeComparisonNormalization
                    or ProviderMobilityProviderFeature.DecimalComparisonNormalization
                    or ProviderMobilityProviderFeature.TimeSpanComparisonNormalization
                    or ProviderMobilityProviderFeature.TemporalConstructionTranslation
                    or ProviderMobilityProviderFeature.TemporalArithmeticTranslation
                    or ProviderMobilityProviderFeature.TemporalClockSource
                    or ProviderMobilityProviderFeature.GeneratedKeyRetrieval
                    or ProviderMobilityProviderFeature.InsertOrIgnoreTranslation
                    or ProviderMobilityProviderFeature.CudSubqueryRewrite
                    or ProviderMobilityProviderFeature.BitwiseXorTranslation
                    or ProviderMobilityProviderFeature.WindowFunctionTranslation
                    or ProviderMobilityProviderFeature.CaseSensitiveStringComparison
                    or ProviderMobilityProviderFeature.SqlStatementLengthLimit);
        }
    }

    [Fact]
    public void Provider_mobile_aggregate_fold_rewrites_use_norm_unsupported_exception()
    {
        var providerType = typeof(NormQueryProvider);

        // Comparable accumulators (DateTime here) combine via direct comparison;
        // only a non-comparable accumulator still fails closed.
        var minMax = providerType.GetMethod("CombineMinMax", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(minMax);
        var earlier = new DateTime(2024, 1, 1);
        var later = new DateTime(2025, 6, 1);
        Assert.Equal(later, minMax!.Invoke(null, new object[] { earlier, later, typeof(DateTime), true }));
        var minMaxEx = Assert.Throws<TargetInvocationException>(() =>
            minMax.Invoke(null, new object[] { new object(), new object(), typeof(object), true }));
        Assert.IsType<NormUnsupportedFeatureException>(minMaxEx.InnerException);

        var sum = providerType.GetMethod("AddSeedToSum", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(sum);
        var sumEx = Assert.Throws<TargetInvocationException>(() =>
            sum!.Invoke(null, new object[] { DateTime.UtcNow, DateTime.UtcNow, typeof(DateTime) }));
        Assert.IsType<NormUnsupportedFeatureException>(sumEx.InnerException);
    }

    [Fact]
    public async Task Runtime_provider_version_exception_uses_translation_layer_decision()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var provider = new VersionGateProvider(new Version(99, 0), "3.46.1");
        var decision = ProviderMobilityTranslator.DecideProviderVersion(provider.Capabilities, new Version(3, 46, 1));

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(
            () => provider.InitializeConnectionAsync(connection, CancellationToken.None));

        Assert.Contains(decision.Reason, ex.Message, StringComparison.Ordinal);
        Assert.Contains(decision.SuggestedFix, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("FromSqlRawAsync", ProviderMobilityFeature.RawSql)]
    [InlineData("ExecuteStoredProcedureAsync", ProviderMobilityFeature.StoredProcedure)]
    [InlineData("Query", ProviderMobilityFeature.DynamicTableQuery)]
    [InlineData("Connection", ProviderMobilityFeature.DirectConnectionAccess)]
    [InlineData("Provider", ProviderMobilityFeature.DirectProviderAccess)]
    [InlineData("CurrentTransaction", ProviderMobilityFeature.DirectTransactionAccess)]
    [InlineData("CreateSavepointAsync", ProviderMobilityFeature.RawTransactionSavepoint)]
    [InlineData("GenerateNativeTenantPolicySql", ProviderMobilityFeature.ProviderNativeTenantSecurity)]
    [InlineData("GenerateProviderNativeTemporalBootstrapSql", ProviderMobilityFeature.ProviderNativeTemporal)]
    public void Translation_layer_maps_runtime_strict_features(string runtimeFeature, ProviderMobilityFeature feature)
    {
        var decision = ProviderMobilityTranslator.DecideRuntimeFeature(runtimeFeature);
        Assert.Equal(feature, decision.Feature);
        Assert.False(decision.StrictRuntimeAllowed);
    }

    [Fact]
    public async Task Runtime_strict_exception_uses_translation_layer_decision()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var ctx = new DbContext(
            connection,
            new SqliteProvider(),
            new DbContextOptions().UseStrictProviderMobility());

        var decision = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.RawSql);
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.FromSqlRawAsync<Row>("SELECT 1 AS Id"));

        Assert.Contains(decision.Reason, ex.Message, StringComparison.Ordinal);
        Assert.Contains(decision.SuggestedFix, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Static_certification_uses_translation_layer_decision_text_and_severity()
    {
        var root = Path.Combine(Path.GetTempPath(), "norm-mobility-layer-" + Path.GetRandomFileName());
        Directory.CreateDirectory(root);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using Microsoft.Data.Sqlite;
                using nORM.Core;

                public sealed class Repository
                {
                    public async System.Threading.Tasks.Task Run(DbContext db)
                    {
                        await db.FromSqlRawAsync<Row>("SELECT * FROM Rows");
                    }

                    public DbContext Create(string cs) => new(new SqliteConnection(cs), new nORM.Providers.SqliteProvider());
                }

                public sealed class Row { public int Id { get; set; } }
                """);

            var report = ProviderMobilityCertificationRunner.Run(new ProviderMobilityCertificationOptions
            {
                ScanPath = root
            });

            var rawSql = Assert.Single(report.Findings.Where(f => f.Kind == "raw-sql"));
            var rawSqlDecision = ProviderMobilityTranslator.Decide(ProviderMobilityFeature.RawSql);
            Assert.Equal(rawSqlDecision.CertificationSeverity, rawSql.Severity);
            Assert.Equal(rawSqlDecision.Reason, rawSql.Reason);
            Assert.StartsWith(rawSqlDecision.SuggestedFix, rawSql.SuggestedFix, StringComparison.Ordinal);
            Assert.Contains("Pattern-specific remediation", rawSql.SuggestedFix, StringComparison.Ordinal);

            Assert.Contains(report.Findings, finding =>
                finding.Kind == "provider-bootstrap-connection" &&
                finding.Severity == ProviderMobilityTranslator.Decide(ProviderMobilityFeature.ProviderBootstrap).CertificationSeverity);
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    private sealed class Row
    {
        public int Id { get; set; }
    }

    private sealed class VersionGateProvider : DatabaseProvider
    {
        private readonly Version _minimumVersion;
        private readonly string _versionText;

        public VersionGateProvider(Version minimumVersion, string versionText)
        {
            _minimumVersion = minimumVersion;
            _versionText = versionText;
        }

        public override ProviderCapabilities Capabilities => new(
            "TestSQL",
            _minimumVersion,
            100,
            supportsJson: false,
            supportsTemporalVersioning: false,
            supportsNativeBulkInsert: false,
            supportsSavepoints: false,
            "Test provider.");

        public override string Escape(string id) => id;

        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
        }

        public override string GetIdentityRetrievalString(TableMapping m) => string.Empty;

        public override DbParameter CreateParameter(string name, object? value)
            => new SqliteParameter(name, value ?? DBNull.Value);

        public override string? TranslateFunction(string name, Type declaringType, params string[] args) => null;

        public override string TranslateJsonPathAccess(string columnName, string jsonPath) => columnName;

        public override string GenerateCreateHistoryTableSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
            => string.Empty;

        public override string GenerateTemporalTriggersSql(TableMapping mapping, System.Collections.Generic.IReadOnlyList<LiveColumnInfo>? liveColumns = null) => string.Empty;

        protected override Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
            => Task.FromResult<string?>(_versionText);

        protected override string? GetServerVersionString(DbConnection connection) => _versionText;
    }

    private static IEnumerable<string> GetRuleKinds(Type type, string fieldName)
        => GetRules(type, fieldName)
            .Select(static rule => rule.Split('|')[1]);

    private static IEnumerable<string> GetRules(Type type, string fieldName)
    {
        var field = type.GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);

        var values = Assert.IsAssignableFrom<System.Collections.IEnumerable>(field.GetValue(null));
        foreach (var value in values)
        {
            Assert.NotNull(value);
            var patternField = value.GetType().GetField("Item1");
            var kindField = value.GetType().GetField("Item2");
            var fixField = value.GetType().GetField("Item3");
            Assert.NotNull(patternField);
            Assert.NotNull(kindField);
            Assert.NotNull(fixField);
            yield return Assert.IsType<string>(patternField.GetValue(value)) + "|" +
                         Assert.IsType<string>(kindField.GetValue(value)) + "|" +
                         Assert.IsType<string>(fixField.GetValue(value));
        }
    }
}
