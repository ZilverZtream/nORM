#nullable enable
using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
public sealed class LiveProviderScaffoldingParityTests
{
    private const string AuthorTable = "ScaffoldLiveAuthor";
    private const string BookTable = "ScaffoldLiveBook";
    private const string LabelTable = "ScaffoldLiveLabel";
    private const string BookLabelTable = "ScaffoldLiveBookLabel";
    private const string FkName = "FK_ScaffoldLiveBook_Author";
    private const string BookLabelBookFkName = "FK_ScaffoldLiveBookLabel_Book";
    private const string BookLabelLabelFkName = "FK_ScaffoldLiveBookLabel_Label";
    private const string CompositeParentTable = "ScaffoldLiveCompositeParent";
    private const string CompositeChildTable = "ScaffoldLiveCompositeChild";
    private const string CompositeFkName = "FK_ScaffoldLiveCompositeChild_Parent";
    private const string CompositeStudentTable = "ScaffoldLiveCompositeStudent";
    private const string CompositeCourseTable = "ScaffoldLiveCompositeCourse";
    private const string CompositeStudentCourseTable = "ScaffoldLiveCompositeStudentCourse";
    private const string CompositeStudentCourseStudentFkName = "FK_ScaffoldLiveCompositeStudentCourse_Student";
    private const string CompositeStudentCourseCourseFkName = "FK_ScaffoldLiveCompositeStudentCourse_Course";
    private const string UniqueParentTable = "ScaffoldLiveUniqueParent";
    private const string UniqueChildTable = "ScaffoldLiveUniqueChild";
    private const string UniqueFkName = "FK_ScaffoldLiveUniqueChild_Parent";
    private const string UniqueIndexName = "UX_ScaffoldLiveUniqueParent_Tenant_External";
    private const string WarningTable = "ScaffoldLiveWarning";
    private const string KeylessTable = "ScaffoldLiveKeyless";
    private const string WarningView = "ScaffoldLiveWarningView";
    private const string ProviderIndexTable = "ScaffoldLiveProviderIndex";
    private const string ProviderPartialIndex = "IX_ScaffoldLiveProviderIndex_Partial";
    private const string ProviderExpressionIndex = "IX_ScaffoldLiveProviderIndex_Expression";
    private const string ProviderIncludedIndex = "IX_ScaffoldLiveProviderIndex_Included";
    private const string ProviderPrefixIndex = "IX_ScaffoldLiveProviderIndex_Prefix";
    private const string SqlServerTemporalBaseTable = "ScaffoldLiveTemporalOrder";
    private const string SqlServerTemporalHistoryTable = "ScaffoldLiveTemporalOrderHistory";
    private const string PostgresSerialTable = "ScaffoldLivePostgresSerial";
    private const string DynamicComputedTable = "ScaffoldLiveDynamicComputed";
    private const string DecimalPrecisionTable = "ScaffoldLiveDecimalPrecision";
    private const string RoutineName = "ScaffoldLiveGetRevenue";
    private const string RoutineOutputName = "ScaffoldLiveGetRevenueOutput";
    private const string PostgresSetReturningRoutineName = "ScaffoldLiveSetReturningRevenue";

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_same_single_fk_model_shape_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldContext",
                    new ScaffoldOptions { Tables = new[] { AuthorTable, BookTable, LabelTable, BookLabelTable }, OverwriteFiles = false });

                var authorCode = await File.ReadAllTextAsync(Path.Combine(dir, AuthorTable + ".cs"));
                var bookCode = await File.ReadAllTextAsync(Path.Combine(dir, BookTable + ".cs"));
                var labelCode = await File.ReadAllTextAsync(Path.Combine(dir, LabelTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, BookLabelTable + ".cs")));
                Assert.Contains("public List<ScaffoldLiveBook> ScaffoldLiveBooks { get; set; } = new();", authorCode);
                Assert.Contains("[ForeignKey(nameof(AuthorId))]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 0)]", bookCode);
                Assert.Contains("[Index(\"IX_ScaffoldLiveBook_Author_Title\", Order = 1)]", bookCode);
                Assert.Contains("public ScaffoldLiveAuthor? ScaffoldLiveAuthor { get; set; }", bookCode);
                Assert.Contains("public List<ScaffoldLiveLabel> ScaffoldLiveLabels { get; set; } = new();", bookCode);
                Assert.Contains("public List<ScaffoldLiveBook> ScaffoldLiveBooks { get; set; } = new();", labelCode);
                Assert.Contains(".HasMany(p => p.ScaffoldLiveBooks)", contextCode);
                Assert.Contains(".WithOne(d => d.ScaffoldLiveAuthor)", contextCode);
                Assert.Contains(".HasForeignKey(d => d.AuthorId, p => p.Id, cascadeDelete: false);", contextCode);
                Assert.Contains(".HasMany<ScaffoldLiveLabel>(p => p.ScaffoldLiveLabels)", contextCode);
                Assert.Contains(".WithMany(p => p.ScaffoldLiveBooks)", contextCode);
                Assert.Contains($".UsingTable(\"", contextCode);
                Assert.Contains(BookLabelTable, contextCode);
                Assert.Contains("\"BookId\", \"LabelId\");", contextCode);

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_same_composite_fk_model_shape_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeContext",
                    new ScaffoldOptions { Tables = new[] { CompositeParentTable, CompositeChildTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeChildTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeContext.cs"));

                Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
                Assert.Contains($"public {CompositeParentTable}? {CompositeParentTable} {{ get; set; }}", childCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeChildTable}> {CompositeChildTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains($"mb.Entity<{CompositeParentTable}>().HasKey(e => new {{ e.TenantId, e.OrderNo }});", contextCode, StringComparison.Ordinal);
                Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.OrderNo }, p => new { p.TenantId, p.OrderNo }, cascadeDelete: false);", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_composite_many_to_many_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeManyToManyAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_composite_m2m_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldCompositeManyToManyContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { CompositeStudentTable, CompositeCourseTable, CompositeStudentCourseTable },
                        OverwriteFiles = false
                    });

                var studentCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeStudentTable + ".cs"));
                var courseCode = await File.ReadAllTextAsync(Path.Combine(dir, CompositeCourseTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldCompositeManyToManyContext.cs"));

                Assert.False(File.Exists(Path.Combine(dir, CompositeStudentCourseTable + ".cs")));
                Assert.Contains($"public List<{CompositeCourseTable}> {CompositeCourseTable}s {{ get; set; }} = new();", studentCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{CompositeStudentTable}> {CompositeStudentTable}s {{ get; set; }} = new();", courseCode, StringComparison.Ordinal);
                Assert.Contains($".UsingTable(\"{CompositeStudentCourseTable}\", new[] {{ \"StudentTenantId\", \"StudentId\" }}, new[] {{ \"CourseTenantId\", \"CourseId\" }});", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeManyToManyAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_generates_composite_fk_to_unique_index_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupCompositeUniqueAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_unique_fk_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldUniqueContext",
                    new ScaffoldOptions { Tables = new[] { UniqueParentTable, UniqueChildTable }, OverwriteFiles = false });

                var childCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueChildTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, UniqueParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldUniqueContext.cs"));

                Assert.DoesNotContain("[ForeignKey(", childCode, StringComparison.Ordinal);
                Assert.Contains($"public {UniqueParentTable}? {UniqueParentTable} {{ get; set; }}", childCode, StringComparison.Ordinal);
                Assert.Contains($"public List<{UniqueChildTable}> {UniqueChildTable}s {{ get; set; }} = new();", parentCode, StringComparison.Ordinal);
                Assert.Contains(".HasForeignKey(d => new { d.TenantId, d.ExternalNo }, p => new { p.TenantId, p.ExternalNo }, cascadeDelete: false);", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownCompositeUniqueAsync(connection, provider, kind);
            }
        }
    }

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
                Assert.Contains("public int? tenantId { get; init; }", contextCode, StringComparison.Ordinal);
                Assert.Contains($"ExecuteStoredProcedureAsync<TResult>(\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(RoutineName, contextCode, StringComparison.Ordinal);
                Assert.Contains("Routine bodies are provider-owned and are not translated by nORM", contextCode, StringComparison.Ordinal);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "Routine" &&
                    item.GetProperty("name").GetString()!.EndsWith(RoutineName, StringComparison.Ordinal));
                if (kind == ProviderKind.Postgres)
                {
                    var routine = Assert.Single(skippedObjects, item =>
                        item.GetProperty("kind").GetString() == "Routine" &&
                        item.GetProperty("name").GetString()!.EndsWith(RoutineName, StringComparison.Ordinal));
                    var metadata = routine.GetProperty("metadata");
                    Assert.Equal(1, metadata.GetProperty("parameterCount").GetInt32());
                    var parameters = metadata.GetProperty("parameters").EnumerateArray().ToArray();
                    var parameter = Assert.Single(parameters);
                    Assert.Equal("IN", parameter.GetProperty("mode").GetString());
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
                Assert.Contains($"IAsyncEnumerable<TResult> Stream{PostgresSetReturningRoutineName}Async<TResult>", contextCode, StringComparison.Ordinal);
                Assert.Contains("return QueryUnchangedAsync<TResult>(\"SELECT * FROM \" + invocation", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("SELECT \" + invocation + \" AS \" + Provider.Escape(\"Value\")", contextCode, StringComparison.Ordinal);
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
                Assert.Contains("new OutputParameter(\"total\", System.Data.DbType.Decimal)", contextCode, StringComparison.Ordinal);
                if (kind == ProviderKind.MySql)
                {
                    Assert.Contains("public string? message { get; init; }", contextCode, StringComparison.Ordinal);
                    Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32, System.Data.ParameterDirection.InputOutput)", contextCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains("new OutputParameter(\"message\", System.Data.DbType.String, 32)", contextCode, StringComparison.Ordinal);
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
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_owned_and_keyless_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupWarningDiagnosticsAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_warnings_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldWarningContext",
                    new ScaffoldOptions { Tables = new[] { WarningTable, KeylessTable }, OverwriteFiles = false });

                var warnings = await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.md"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("MissingPrimaryKey", warnings, StringComparison.Ordinal);
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldWarningContext.cs"));
                var defaultPromotedToModel = contextCode.Contains("HasDefaultValueSql", StringComparison.Ordinal);
                if (!defaultPromotedToModel)
                {
                    Assert.Contains("Provider-Owned Schema Features", warnings, StringComparison.Ordinal);
                    Assert.Contains("Default", warnings, StringComparison.Ordinal);
                }

                var providerOwned = warningJson.RootElement
                    .GetProperty("providerOwnedSchemaFeatures")
                    .EnumerateArray()
                    .ToArray();

                if (defaultPromotedToModel)
                {
                    Assert.Contains(".Property(e => e.Status).HasDefaultValueSql(", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "Default" &&
                        item.GetProperty("table").GetString()!.Split('.').Last() == WarningTable &&
                        item.GetProperty("name").GetString() == "Status");
                }
                else
                {
                    Assert.Contains(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "Default" &&
                        item.GetProperty("table").GetString()!.Split('.').Last() == WarningTable &&
                        item.GetProperty("name").GetString() == "Status" &&
                        item.GetProperty("suggestedAction").GetString()!.Contains("default", StringComparison.OrdinalIgnoreCase));
                }

                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString()!.Split('.').Last() == KeylessTable &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownWarningDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_view_filter_fails_with_skipped_object_diagnostic_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSkippedViewAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_view_filter_" + Guid.NewGuid().ToString("N"));
            try
            {
                var ex = await Assert.ThrowsAsync<nORM.Core.NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldViewContext",
                        new ScaffoldOptions { Tables = new[] { WarningView }, OverwriteFiles = false }));

                Assert.Contains("matched database object", ex.Message, StringComparison.Ordinal);
                Assert.Contains("View", ex.Message, StringComparison.Ordinal);
                Assert.Contains(WarningView, ex.Message, StringComparison.Ordinal);
                Assert.Contains("does not emit as entity classes", ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSkippedViewAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_emits_view_entities_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSkippedViewAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_view_entity_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldViewEntityContext",
                    new ScaffoldOptions { Tables = new[] { WarningView }, EmitViewEntities = true, OverwriteFiles = false });

                var viewCode = await File.ReadAllTextAsync(Path.Combine(dir, WarningView + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldViewEntityContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Table(\"{WarningView}", viewCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{WarningView}>", contextCode, StringComparison.Ordinal);
                Assert.True(File.Exists(warningJsonPath));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString()!.EndsWith(WarningView, StringComparison.Ordinal));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSkippedViewAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_provider_specific_index_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupProviderSpecificIndexesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderIndexContext.cs"));
                var warningPath = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");
                var warnings = File.Exists(warningPath) ? await File.ReadAllTextAsync(warningPath) : string.Empty;
                using var warningJson = File.Exists(warningJsonPath)
                    ? JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath))
                    : JsonDocument.Parse("{\"providerOwnedSchemaFeatures\":[]}");

                Assert.Contains($"[Index(\"{ProviderPartialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("PartialIndex", warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderPartialIndex, warnings, StringComparison.Ordinal);

                if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
                {
                    Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("ExpressionIndex", warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionIndex, warnings, StringComparison.Ordinal);
                }

                if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                {
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("IncludedColumnIndex", warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderIncludedIndex, warnings, StringComparison.Ordinal);
                }

                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PartialIndex" &&
                    item.GetProperty("name").GetString() == ProviderPartialIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderIncludedIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_prefix_index_without_emitting_normal_index()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMySqlPrefixIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_prefix_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlPrefixIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderPrefixIndex, entityCode, StringComparison.Ordinal);
                var prefix = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PrefixIndex" &&
                    item.GetProperty("name").GetString() == ProviderPrefixIndex);
                Assert.Equal("SCF117", prefix.GetProperty("code").GetString());
                Assert.Equal("index", prefix.GetProperty("category").GetString());
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_sqlserver_native_temporal_tables_and_marks_history_read_only()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerNativeTemporalTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_temporal_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerTemporalContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "dbo." + SqlServerTemporalBaseTable, "dbo." + SqlServerTemporalHistoryTable },
                        OverwriteFiles = false
                    });

                var baseCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerTemporalBaseTable + ".cs"));
                var historyCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerTemporalHistoryTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
                Assert.Contains("[ReadOnlyEntity]", historyCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalBaseTable &&
                    item.GetProperty("code").GetString() == "SCF115");
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalHistoryTable &&
                    item.GetProperty("detail").GetString()!.Contains("history table", StringComparison.OrdinalIgnoreCase));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerNativeTemporalTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_postgres_serial_primary_key_does_not_emit_default_or_owned_sequence_warnings()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider Postgres not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresSerialTable, provider.Escape(PostgresSerialTable)));
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_serial_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection, $"CREATE TABLE {provider.Escape(PostgresSerialTable)} ({provider.Escape("Id")} SERIAL PRIMARY KEY, {provider.Escape("Name")} {TextType(ProviderKind.Postgres, 40)} NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresSerialContext",
                    new ScaffoldOptions { Tables = new[] { PostgresSerialTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, PostgresSerialTable + ".cs"));
                Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Identity)]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresSerialTable, provider.Escape(PostgresSerialTable)));
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_generated_columns_as_computed_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicComputedTable, provider.Escape(DynamicComputedTable)));
            try
            {
                await ExecuteAsync(connection, GeneratedColumnTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicComputedTable);
                var generated = type.GetProperty("NameLength")!
                    .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
                    .Cast<DatabaseGeneratedAttribute>()
                    .SingleOrDefault();

                Assert.NotNull(generated);
                Assert.Equal(DatabaseGeneratedOption.Computed, generated.DatabaseGeneratedOption);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicComputedTable, provider.Escape(DynamicComputedTable)));
            }
        }
    }

    private static async Task SetupAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, BookLabelTable, provider.Escape(BookLabelTable)));
        await ExecuteAsync(connection, DropTable(kind, BookTable, provider.Escape(BookTable)));
        await ExecuteAsync(connection, DropTable(kind, LabelTable, provider.Escape(LabelTable)));
        await ExecuteAsync(connection, DropTable(kind, AuthorTable, provider.Escape(AuthorTable)));

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("Author_Id");
        var bookId = provider.Escape("BookId");
        var labelId = provider.Escape("LabelId");
        var author = provider.Escape(AuthorTable);
        var book = provider.Escape(BookTable);
        var label = provider.Escape(LabelTable);
        var bookLabel = provider.Escape(BookLabelTable);

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {authorId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(FkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}))");
        await ExecuteAsync(connection, $"CREATE TABLE {label} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {bookLabel} ({bookId} {IntType(kind)} NOT NULL, {labelId} {IntType(kind)} NOT NULL, PRIMARY KEY ({bookId}, {labelId}), " +
            $"CONSTRAINT {provider.Escape(BookLabelBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}), " +
            $"CONSTRAINT {provider.Escape(BookLabelLabelFkName)} FOREIGN KEY ({labelId}) REFERENCES {label} ({id}))");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveBook_Author_Title")} ON {book} ({authorId}, {title})");
    }

    private static async Task SetupCompositeAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeChildTable, provider.Escape(CompositeChildTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeParentTable, provider.Escape(CompositeParentTable)));

        var parent = provider.Escape(CompositeParentTable);
        var child = provider.Escape(CompositeChildTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {orderNo}))");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {orderNo} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeFkName)} FOREIGN KEY ({tenantId}, {orderNo}) REFERENCES {parent} ({tenantId}, {orderNo}))");
    }

    private static async Task SetupCompositeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeStudentCourseTable, provider.Escape(CompositeStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeCourseTable, provider.Escape(CompositeCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeStudentTable, provider.Escape(CompositeStudentTable)));

        var student = provider.Escape(CompositeStudentTable);
        var course = provider.Escape(CompositeCourseTable);
        var join = provider.Escape(CompositeStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositeStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositeStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupCompositeUniqueAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, UniqueChildTable, provider.Escape(UniqueChildTable)));
        await ExecuteAsync(connection, DropTable(kind, UniqueParentTable, provider.Escape(UniqueParentTable)));

        var parent = provider.Escape(UniqueParentTable);
        var child = provider.Escape(UniqueChildTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var externalNo = provider.Escape("ExternalNo");
        var name = provider.Escape("Name");
        var eventName = provider.Escape("EventName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {externalNo} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(UniqueIndexName)} ON {parent} ({tenantId}, {externalNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {externalNo} {TextType(kind, 40)} NOT NULL, {eventName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(UniqueFkName)} FOREIGN KEY ({tenantId}, {externalNo}) REFERENCES {parent} ({tenantId}, {externalNo}))");
    }

    private static async Task SetupDecimalPrecisionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, DecimalPrecisionTable, provider.Escape(DecimalPrecisionTable)));

        var table = provider.Escape(DecimalPrecisionTable);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL)");
    }

    private static async Task SetupRoutineAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoutineAsync(connection, provider, kind);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineName)} @tenantId INT AS SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(RoutineName)}(tenantId integer) RETURNS TABLE(\"Id\" integer, \"Name\" text) LANGUAGE SQL AS $$ SELECT tenantId, 'ok'::text $$");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape(RoutineName)}(IN tenantId INT) SELECT tenantId AS Id, 'ok' AS Name");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffolding live test only targets providers with routine support.");
        }
    }

    private static async Task SetupRoutineWithOutputAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoutineWithOutputAsync(connection, provider, kind);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineOutputName)} @tenantId INT, @total DECIMAL(18,2) OUTPUT, @message NVARCHAR(32) OUTPUT AS BEGIN SET @total = 12.34; SET @message = N'ok'; SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name; END");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE PROCEDURE {provider.Escape(RoutineOutputName)}(IN tenantId INT, OUT total DECIMAL(18,2), INOUT message VARCHAR(32)) BEGIN SET total = 12.34; SET message = CONCAT(COALESCE(message, ''), 'ok'); SELECT tenantId AS Id, 'ok' AS Name; END");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output scaffolding live test only targets providers with OUT parameter support in this test harness.");
        }
    }

    private static async Task SetupPostgresSetReturningRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresSetReturningRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(PostgresSetReturningRoutineName)}(tenantId integer) RETURNS SETOF integer LANGUAGE SQL AS $$ SELECT tenantId $$");
    }

    private static async Task SetupWarningDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, KeylessTable, provider.Escape(KeylessTable)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var keyless = provider.Escape(KeylessTable);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");
        var externalId = provider.Escape("ExternalId");
        var payload = provider.Escape("Payload");
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape("DF_ScaffoldLiveWarning_Status")} DEFAULT ('new')"
            : "DEFAULT 'new'";

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL {defaultClause})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {keyless} ({externalId} {TextType(kind, 40)} NOT NULL, {payload} {TextType(kind, 80)} NOT NULL)");
    }

    private static async Task SetupSkippedViewAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropView(kind, WarningView, provider.Escape(WarningView)));
        await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));

        var warning = provider.Escape(WarningTable);
        var view = provider.Escape(WarningView);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {status} {TextType(kind, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE VIEW {view} AS SELECT {id}, {status} FROM {warning}");
    }

    private static async Task SetupProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var activeType = kind == ProviderKind.SqlServer ? "BIT" : kind == ProviderKind.Postgres ? "BOOLEAN" : "INTEGER";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL, {active} {activeType} NOT NULL, {includedValue} {IntType(kind)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialIndex)} ON {table} ({name}) WHERE {activePredicate}");

        if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} (lower({name}))");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderIncludedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static async Task SetupMySqlPrefixIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPrefixIndex)} ON {table} ({name}(8))");
    }

    private static async Task SetupSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerNativeTemporalTableAsync(connection, provider);

        var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
        var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var validFrom = provider.Escape("ValidFrom");
        var validTo = provider.Escape("ValidTo");

        await ExecuteAsync(connection, $$"""
            CREATE TABLE {{table}} (
                {{id}} INT NOT NULL PRIMARY KEY,
                {{name}} NVARCHAR(80) NOT NULL,
                {{validFrom}} DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                {{validTo}} DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                PERIOD FOR SYSTEM_TIME ({{validFrom}}, {{validTo}})
            ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {{history}}))
            """);
    }

    private static async Task TeardownSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
            var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
            await ExecuteAsync(connection, $$"""
                IF OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}', N'U') IS NOT NULL
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM sys.tables
                        WHERE object_id = OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}')
                          AND temporal_type = 2
                    )
                    BEGIN
                        ALTER TABLE {{table}} SET (SYSTEM_VERSIONING = OFF);
                    END;

                    DROP TABLE {{table}};
                END;

                IF OBJECT_ID(N'dbo.{{SqlServerTemporalHistoryTable}}', N'U') IS NOT NULL
                    DROP TABLE {{history}};
                """);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string SqlServerQualified(DatabaseProvider provider, string tableName)
        => provider.Escape("dbo") + "." + provider.Escape(tableName);

    private static async Task TeardownAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, BookLabelTable, provider.Escape(BookLabelTable)));
            await ExecuteAsync(connection, DropTable(kind, BookTable, provider.Escape(BookTable)));
            await ExecuteAsync(connection, DropTable(kind, LabelTable, provider.Escape(LabelTable)));
            await ExecuteAsync(connection, DropTable(kind, AuthorTable, provider.Escape(AuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeChildTable, provider.Escape(CompositeChildTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeParentTable, provider.Escape(CompositeParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeStudentCourseTable, provider.Escape(CompositeStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeCourseTable, provider.Escape(CompositeCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeStudentTable, provider.Escape(CompositeStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeUniqueAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, UniqueChildTable, provider.Escape(UniqueChildTable)));
            await ExecuteAsync(connection, DropTable(kind, UniqueParentTable, provider.Escape(UniqueParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownDecimalPrecisionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, DecimalPrecisionTable, provider.Escape(DecimalPrecisionTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownRoutineAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{RoutineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineName)}",
                ProviderKind.Postgres => $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(RoutineName)}(integer)",
                ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(RoutineName)}",
                _ => ""
            };

            if (!string.IsNullOrWhiteSpace(sql))
                await ExecuteAsync(connection, sql);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownRoutineWithOutputAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{RoutineOutputName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineOutputName)}",
                ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(RoutineOutputName)}",
                _ => ""
            };

            if (!string.IsNullOrWhiteSpace(sql))
                await ExecuteAsync(connection, sql);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresSetReturningRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresSetReturningRoutineName)}(integer)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownWarningDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, KeylessTable, provider.Escape(KeylessTable)));
            await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSkippedViewAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropView(kind, WarningView, provider.Escape(WarningView)));
            await ExecuteAsync(connection, DropTable(kind, WarningTable, provider.Escape(WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownProviderSpecificIndexesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static void AssertScaffoldOutputBuilds(string outputDirectory)
    {
        var root = FindRepositoryRoot();
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(outputDirectory, "LiveScaffolded.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>enable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);

        var psi = new ProcessStartInfo("dotnet", "build -c Release --nologo")
        {
            WorkingDirectory = outputDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start dotnet build.");
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0,
            $"Scaffolded live-provider output failed to build with exit code {process.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{stderr}");
    }

    private static string FindRepositoryRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (!string.IsNullOrEmpty(dir))
        {
            if (File.Exists(Path.Combine(dir, "nORM.sln")))
                return dir;

            dir = Directory.GetParent(dir)?.FullName;
        }

        throw new InvalidOperationException("Could not locate repository root from " + AppContext.BaseDirectory);
    }

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static string DropView(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'V') IS NOT NULL DROP VIEW {escapedName}"
        : $"DROP VIEW IF EXISTS {escapedName}";

    private static string IntType(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string TextType(ProviderKind kind, int length) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({length})"
        : kind == ProviderKind.Sqlite
            ? "TEXT"
            : $"VARCHAR({length})";

    private static string GeneratedColumnTableSql(ProviderKind kind, DatabaseProvider provider)
    {
        var table = provider.Escape(DynamicComputedTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var nameLength = provider.Escape("NameLength");
        return kind switch
        {
            ProviderKind.SqlServer => $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {name} NVARCHAR(40) NOT NULL, {nameLength} AS LEN({name}))",
            ProviderKind.Postgres => $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {name} varchar(40) NOT NULL, {nameLength} integer GENERATED ALWAYS AS (length({name})) STORED)",
            ProviderKind.MySql => $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {name} VARCHAR(40) NOT NULL, {nameLength} INT GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED)",
            _ => $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {name} TEXT NOT NULL, {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL)"
        };
    }
}
