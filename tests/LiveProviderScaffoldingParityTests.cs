#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.LiveProvider)]
[Collection("LiveProviderScaffolding")]
public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string AuthorTable = "ScaffoldLiveAuthor";
    private const string BookTable = "ScaffoldLiveBook";
    private const string LabelTable = "ScaffoldLiveLabel";
    private const string BookLabelTable = "ScaffoldLiveBookLabel";
    private const string FkName = "FK_ScaffoldLiveBook_Author";
    private const string BookLabelBookFkName = "FK_ScaffoldLiveBookLabel_Book";
    private const string BookLabelLabelFkName = "FK_ScaffoldLiveBookLabel_Label";
    private const string SurrogateAuthorTable = "ScaffoldLiveSurrogateAuthor";
    private const string SurrogateBookTable = "ScaffoldLiveSurrogateBook";
    private const string SurrogateAuthorBookTable = "ScaffoldLiveSurrogateAuthorBook";
    private const string SurrogateAuthorBookAuthorFkName = "FK_ScaffoldLiveSurrogateAuthorBook_Author";
    private const string SurrogateAuthorBookBookFkName = "FK_ScaffoldLiveSurrogateAuthorBook_Book";
    private const string GeneratedBridgeStudentTable = "ScaffoldLiveGeneratedBridgeStudent";
    private const string GeneratedBridgeCourseTable = "ScaffoldLiveGeneratedBridgeCourse";
    private const string GeneratedBridgeStudentCourseTable = "ScaffoldLiveGeneratedBridgeStudentCourse";
    private const string GeneratedBridgeStudentCourseStudentFkName = "FK_ScaffoldLiveGeneratedBridgeStudentCourse_Student";
    private const string GeneratedBridgeStudentCourseCourseFkName = "FK_ScaffoldLiveGeneratedBridgeStudentCourse_Course";
    private const string CompositeParentTable = "ScaffoldLiveCompositeParent";
    private const string CompositeChildTable = "ScaffoldLiveCompositeChild";
    private const string CompositeFkName = "FK_ScaffoldLiveCompositeChild_Parent";
    private const string ReferentialParentTable = "ScaffoldLiveReferentialParent";
    private const string ReferentialChildTable = "ScaffoldLiveReferentialChild";
    private const string ReferentialFkName = "FK_ScaffoldLiveReferentialChild_Parent";
    private const string ReferentialRestrictParentTable = "ScaffoldLiveRestrictParent";
    private const string ReferentialRestrictChildTable = "ScaffoldLiveRestrictChild";
    private const string ReferentialRestrictFkName = "FK_ScaffoldLiveRestrictChild_Parent";
    private const string ReferentialDefaultParentTable = "ScaffoldLiveDefaultParent";
    private const string ReferentialDefaultChildTable = "ScaffoldLiveDefaultChild";
    private const string ReferentialDefaultFkName = "FK_ScaffoldLiveDefaultChild_Parent";
    private const string CompositeStudentTable = "ScaffoldLiveCompositeStudent";
    private const string CompositeCourseTable = "ScaffoldLiveCompositeCourse";
    private const string CompositeStudentCourseTable = "ScaffoldLiveCompositeStudentCourse";
    private const string CompositeStudentCourseStudentFkName = "FK_ScaffoldLiveCompositeStudentCourse_Student";
    private const string CompositeStudentCourseCourseFkName = "FK_ScaffoldLiveCompositeStudentCourse_Course";
    private const string CompositeSurrogateStudentTable = "ScaffoldLiveSurrogateStudent";
    private const string CompositeSurrogateCourseTable = "ScaffoldLiveSurrogateCourse";
    private const string CompositeSurrogateStudentCourseTable = "ScaffoldLiveSurrogateStudentCourse";
    private const string CompositeSurrogateStudentCourseStudentFkName = "FK_ScaffoldLiveSurrogateStudentCourse_Student";
    private const string CompositeSurrogateStudentCourseCourseFkName = "FK_ScaffoldLiveSurrogateStudentCourse_Course";
    private const string CompositePayloadStudentTable = "ScaffoldLivePayloadStudent";
    private const string CompositePayloadCourseTable = "ScaffoldLivePayloadCourse";
    private const string CompositePayloadStudentCourseTable = "ScaffoldLivePayloadStudentCourse";
    private const string CompositePayloadStudentCourseStudentFkName = "FK_ScaffoldLivePayloadStudentCourse_Student";
    private const string CompositePayloadStudentCourseCourseFkName = "FK_ScaffoldLivePayloadStudentCourse_Course";
    private const string SharedTenantStudentTable = "ScaffoldLiveSharedTenantStudent";
    private const string SharedTenantCourseTable = "ScaffoldLiveSharedTenantCourse";
    private const string SharedTenantStudentCourseTable = "ScaffoldLiveSharedTenantStudentCourse";
    private const string SharedTenantStudentCourseStudentFkName = "FK_ScaffoldLiveSharedTenantStudentCourse_Student";
    private const string SharedTenantStudentCourseCourseFkName = "FK_ScaffoldLiveSharedTenantStudentCourse_Course";
    private const string SharedAlternateAuthorTable = "ScaffoldLiveSharedAlternateAuthor";
    private const string SharedAlternateBookTable = "ScaffoldLiveSharedAlternateBook";
    private const string SharedAlternateAuthorBookTable = "ScaffoldLiveSharedAlternateAuthorBook";
    private const string SharedAlternateAuthorBookAuthorFkName = "FK_ScaffoldLiveSharedAlternateAuthorBook_Author";
    private const string SharedAlternateAuthorBookBookFkName = "FK_ScaffoldLiveSharedAlternateAuthorBook_Book";
    private const string AlternateAuthorTable = "ScaffoldLiveAlternateAuthor";
    private const string AlternateBookTable = "ScaffoldLiveAlternateBook";
    private const string AlternateAuthorBookTable = "ScaffoldLiveAlternateAuthorBook";
    private const string AlternateAuthorBookAuthorFkName = "FK_ScaffoldLiveAlternateAuthorBook_Author";
    private const string AlternateAuthorBookBookFkName = "FK_ScaffoldLiveAlternateAuthorBook_Book";
    private const string SelfPersonTable = "ScaffoldLivePerson";
    private const string SelfPersonRelationshipTable = "ScaffoldLivePersonRelationship";
    private const string SelfPersonRelationshipMentorFkName = "FK_ScaffoldLivePersonRelationship_Mentor";
    private const string SelfPersonRelationshipMenteeFkName = "FK_ScaffoldLivePersonRelationship_Mentee";
    private const string FilteredStudentTable = "ScaffoldLiveFilteredStudent";
    private const string FilteredCourseTable = "ScaffoldLiveFilteredCourse";
    private const string FilteredStudentCourseTable = "ScaffoldLiveFilteredStudentCourse";
    private const string FilteredStudentCourseStudentFkName = "FK_ScaffoldLiveFilteredStudentCourse_Student";
    private const string FilteredStudentCourseCourseFkName = "FK_ScaffoldLiveFilteredStudentCourse_Course";
    private const string FilteredStudentCourseUniqueIndex = "UX_ScaffoldLiveFilteredStudentCourse_ActivePair";
    private const string SchemaName = "scaffold_live_schema";
    private const string SchemaAuthorTable = "ScaffoldLiveSchemaAuthor";
    private const string SchemaBookTable = "ScaffoldLiveSchemaBook";
    private const string SchemaAuthorBookTable = "ScaffoldLiveSchemaAuthorBook";
    private const string SchemaAuthorBookAuthorFkName = "FK_ScaffoldLiveSchemaAuthorBook_Author";
    private const string SchemaAuthorBookBookFkName = "FK_ScaffoldLiveSchemaAuthorBook_Book";
    private const string UniqueParentTable = "ScaffoldLiveUniqueParent";
    private const string UniqueChildTable = "ScaffoldLiveUniqueChild";
    private const string UniqueFkName = "FK_ScaffoldLiveUniqueChild_Parent";
    private const string UniqueIndexName = "UX_ScaffoldLiveUniqueParent_Tenant_External";
    private const string CompositeRoleAccountTable = "ScaffoldLiveCompositeRoleAccount";
    private const string CompositeRoleTransferTable = "ScaffoldLiveCompositeRoleTransfer";
    private const string CompositeRolePrimaryFkName = "FK_ScaffoldLiveCompositeRoleTransfer_Primary";
    private const string CompositeRoleBackupFkName = "FK_ScaffoldLiveCompositeRoleTransfer_Backup";
    private const string CompositeRoleAccountIndexName = "UX_ScaffoldLiveCompositeRoleAccount_Tenant_Account";
    private const string SingleAlternateParentTable = "ScaffoldLiveSingleAlternateParent";
    private const string SingleAlternateChildTable = "ScaffoldLiveSingleAlternateChild";
    private const string SingleAlternateFkName = "FK_ScaffoldLiveSingleAlternateChild_Parent";
    private const string SingleAlternateIndexName = "UX_ScaffoldLiveSingleAlternateParent_Code";
    private const string NullableAlternateParentTable = "ScaffoldLiveNullableAlternateParent";
    private const string NullableAlternateChildTable = "ScaffoldLiveNullableAlternateChild";
    private const string NullableAlternateFkName = "FK_ScaffoldLiveNullableAlternateChild_Parent";
    private const string NullableAlternateIndexName = "UX_ScaffoldLiveNullableAlternateParent_Code";
    private const string UniqueDependentParentTable = "ScaffoldLiveUniqueDependentParent";
    private const string UniqueDependentProfileTable = "ScaffoldLiveUniqueDependentProfile";
    private const string UniqueDependentFkName = "FK_ScaffoldLiveUniqueDependentProfile_Parent";
    private const string UniqueDependentIndexName = "UX_ScaffoldLiveUniqueDependentProfile_Parent";
    private const string OptionalUniqueParentTable = "ScaffoldLiveOptionalUniqueParent";
    private const string OptionalUniqueProfileTable = "ScaffoldLiveOptionalUniqueProfile";
    private const string OptionalUniqueFkName = "FK_ScaffoldLiveOptionalUniqueProfile_Parent";
    private const string OptionalUniqueIndexName = "UX_ScaffoldLiveOptionalUniqueProfile_Parent";
    private const string RoleOneParentTable = "ScaffoldLiveRoleOneParent";
    private const string RoleOneProfileTable = "ScaffoldLiveRoleOneProfile";
    private const string RoleOnePrimaryFkName = "FK_ScaffoldLiveRoleOneProfile_Primary";
    private const string RoleOneBackupFkName = "FK_ScaffoldLiveRoleOneProfile_Backup";
    private const string RoleOnePrimaryIndexName = "UX_ScaffoldLiveRoleOneProfile_Primary";
    private const string RoleOneBackupIndexName = "UX_ScaffoldLiveRoleOneProfile_Backup";
    private const string SharedPkParentTable = "ScaffoldLiveSharedPkParent";
    private const string SharedPkProfileTable = "ScaffoldLiveSharedPkProfile";
    private const string SharedPkFkName = "FK_ScaffoldLiveSharedPkProfile_Parent";
    private const string CompositeUniqueDependentParentTable = "ScaffoldLiveCompositeUniqueDependentParent";
    private const string CompositeUniqueDependentProfileTable = "ScaffoldLiveCompositeUniqueDependentProfile";
    private const string CompositeUniqueDependentFkName = "FK_ScaffoldLiveCompositeUniqueDependentProfile_Parent";
    private const string CompositeUniqueDependentParentIndexName = "UX_ScaffoldLiveCompositeUniqueDependentParent_Tenant_Account";
    private const string CompositeUniqueDependentProfileIndexName = "UX_ScaffoldLiveCompositeUniqueDependentProfile_Tenant_Account";
    private const string OptionalCompositeUniqueDependentParentTable = "ScaffoldLiveOptionalCompositeUniqueParent";
    private const string OptionalCompositeUniqueDependentProfileTable = "ScaffoldLiveOptionalCompositeUniqueProfile";
    private const string OptionalCompositeUniqueDependentFkName = "FK_ScaffoldLiveOptionalCompositeUniqueProfile_Parent";
    private const string OptionalCompositeUniqueDependentParentIndexName = "UX_ScaffoldLiveOptionalCompositeUniqueParent_Tenant_Account";
    private const string OptionalCompositeUniqueDependentProfileIndexName = "UX_ScaffoldLiveOptionalCompositeUniqueProfile_Tenant_Account";
    private const string DatabaseNamesCustomerTable = "scaffold_database_names_customer";
    private const string DatabaseNamesOrderLineTable = "scaffold_database_names_order_line";
    private const string DatabaseNamesBillingFkName = "FK_scaffold_database_names_order_line_billing";
    private const string DatabaseNamesShippingFkName = "FK_scaffold_database_names_order_line_shipping";
    private const string WarningTable = "ScaffoldLiveWarning";
    private const string KeylessTable = "ScaffoldLiveKeyless";
    private const string KeylessDependentParentTable = "ScaffoldLiveKeylessDependentParent";
    private const string KeylessDependentTable = "ScaffoldLiveKeylessDependent";
    private const string KeylessDependentFkName = "FK_ScaffoldLiveKeylessDependent_Parent";
    private const string WarningView = "ScaffoldLiveWarningView";
    private const string FeatureOwnedTable = "ScaffoldLiveFeatureOwned";
    private const string FeatureOwnedCheckName = "CK_ScaffoldLiveFeatureOwned_Name";
    private const string SqlServerWarningSynonym = "ScaffoldLiveWarningSynonym";
    private const string SqlServerProcedureSynonym = "ScaffoldLiveProcedureSynonym";
    private const string SqlServerSynonymProcedure = "ScaffoldLiveSynonymProcedure";
    private const string PostgresMaterializedView = "ScaffoldLiveWarningMatView";
    private const string SqliteVirtualTable = "ScaffoldLiveVirtualSearch";
    private const string MySqlEventDiagnosticsName = "ScaffoldLiveScheduledEvent";
    private const string PostgresTypedColumnTable = "ScaffoldLivePostgresTypedColumns";
    private const string MySqlTypedColumnTable = "ScaffoldLiveMySqlTypedColumns";
    private const string MySqlUnsignedColumnTable = "ScaffoldLiveMySqlUnsignedColumns";
    private const string MySqlUnsafeSetColumnTable = "ScaffoldLiveMySqlUnsafeSetColumns";
    private const string ProviderSpecificColumnDiagnosticsTable = "ScaffoldLiveProviderSpecificColumns";
    private const string ProviderIndexTable = "ScaffoldLiveProviderIndex";
    private const string ProviderPartialIndex = "IX_ScaffoldLiveProviderIndex_Partial";
    private const string ProviderExpressionIndex = "IX_ScaffoldLiveProviderIndex_Expression";
    private const string ProviderPartialExpressionIndex = "IX_ScaffoldLiveProviderIndex_ExpressionPartial";
    private const string ProviderExpressionDescendingIndex = "IX_ScaffoldLiveProviderIndex_ExprDesc";
    private const string ProviderExpressionLiteralDescIndex = "IX_ScaffoldLiveProviderIndex_ExprLiteral";
    private const string ProviderExpressionIncludedIndex = "IX_ScaffoldLiveProviderIndex_ExpressionIncluded";
    private const string ProviderIncludedIndex = "IX_ScaffoldLiveProviderIndex_Included";
    private const string ProviderDescendingIndex = "IX_ScaffoldLiveProviderIndex_Descending";
    private const string ProviderPrefixIndex = "IX_ScaffoldLiveProviderIndex_Prefix";
    private const string ProviderFullPrefixIndex = "IX_ScaffoldLiveProviderIndex_FullPrefix";
    private const string ProviderSpecificIndex = "IX_ScaffoldLiveProviderIndex_ProviderSpecific";
    private const string TriggerDiagnosticsTable = "ScaffoldLiveTriggerAudit";
    private const string TriggerDiagnosticsTrigger = "TR_ScaffoldLiveTriggerAudit_Touch";
    private const string TriggerDiagnosticsPostgresFunction = "fn_scaffold_live_trigger_audit_touch";
    private const string SqlServerTemporalBaseTable = "ScaffoldLiveTemporalOrder";
    private const string SqlServerTemporalHistoryTable = "ScaffoldLiveTemporalOrderHistory";
    private const string PostgresSerialTable = "ScaffoldLivePostgresSerial";
    private const string DynamicComputedTable = "ScaffoldLiveDynamicComputed";
    private const string DynamicIdentityTable = "ScaffoldLiveDynamicIdentity";
    private const string DynamicCompositeKeyTable = "ScaffoldLiveDynamicCompositeKey";
    private const string DecimalPrecisionTable = "ScaffoldLiveDecimalPrecision";
    private const string StringBinaryFacetTable = "ScaffoldLiveStringBinaryFacets";
    private const string SqlServerRowVersionTable = "ScaffoldLiveRowVersion";
    private const string SqlServerAliasTypeTable = "ScaffoldLiveAliasCustomer";
    private const string SqlServerAliasTypeName = "ScaffoldLiveEmailAddress";
    private const string SqlServerAliasDecimalTypeName = "ScaffoldLiveMoneyAmount";
    private const string SqlServerAliasBinaryTypeName = "ScaffoldLiveTokenBytes";
    private const string PostgresDomainTable = "ScaffoldLiveDomainCustomer";
    private const string PostgresDomainName = "scaffold_live_email_address";
    private const string PostgresDomainScoreName = "scaffold_live_score_value";
    private const string PostgresDomainScoreArrayName = "scaffold_live_score_values";
    private const string PostgresEnumName = "scaffold_live_customer_status";
    private const string PostgresDomainStatusName = "scaffold_live_customer_status_domain";
    private const string RoutineName = "ScaffoldLiveGetRevenue";
    private const string RoutineNonQueryName = "ScaffoldLiveRecalculateLedger";
    private const string RoutineOutputName = "ScaffoldLiveGetRevenueOutput";
    private const string RoutineTableTypeName = "ScaffoldLiveLineItemList";
    private const string RoutineTableValuedParameterName = "ScaffoldLiveImportLines";
    private const string SqlServerScalarFunctionName = "ScaffoldLiveCalculateRisk";
    private const string SqlServerTableValuedFunctionName = "ScaffoldLiveRevenueRows";
    private const string SequenceName = "ScaffoldLiveOrderNo";
    private const string PostgresSetReturningRoutineName = "ScaffoldLiveSetReturningRevenue";
    private const string PostgresTypedRoutineName = "ScaffoldLiveTypedRoutine";
    private const string PostgresOverloadedRoutineName = "ScaffoldLiveOverloadedRoutine";
    private const string PostgresQuotedParameterRoutineName = "ScaffoldLiveQuotedParameterRoutine";
    private const string MySqlUnsignedRoutineName = "ScaffoldLiveUnsignedRoutine";

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
                        LastTableNameEquals(item.GetProperty("table").GetString(), WarningTable) &&
                        item.GetProperty("name").GetString() == "Status");
                }
                else
                {
                    Assert.Contains(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "Default" &&
                        LastTableNameEquals(item.GetProperty("table").GetString(), WarningTable) &&
                        item.GetProperty("name").GetString() == "Status" &&
                        item.GetProperty("suggestedAction").GetString()!.Contains("default", StringComparison.OrdinalIgnoreCase));
                }

                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), KeylessTable) &&
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
    public async Task ScaffoldAsync_suppresses_keyless_dependent_fk_navigation_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupKeylessDependentRelationshipAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_keyless_dependent_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldKeylessDependentContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { KeylessDependentParentTable, KeylessDependentTable },
                        OverwriteFiles = false
                    });

                var dependentCode = await File.ReadAllTextAsync(Path.Combine(dir, KeylessDependentTable + ".cs"));
                var parentCode = await File.ReadAllTextAsync(Path.Combine(dir, KeylessDependentParentTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldKeylessDependentContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", dependentCode, StringComparison.Ordinal);
                Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
                Assert.DoesNotContain(KeylessDependentTable + "s", parentCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), KeylessDependentTable) &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("primary key", StringComparison.OrdinalIgnoreCase));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownKeylessDependentRelationshipAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_promotes_check_and_computed_metadata_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupFeatureOwnedMetadataAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_feature_owned_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFeatureOwnedContext",
                    new ScaffoldOptions { Tables = new[] { FeatureOwnedTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, FeatureOwnedTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFeatureOwnedContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
                Assert.Contains("NameLength { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(".HasCheckConstraint(", contextCode, StringComparison.Ordinal);
                Assert.Contains(FeatureOwnedCheckName, contextCode, StringComparison.Ordinal);
                Assert.Contains("HasComputedColumnSql(", contextCode, StringComparison.Ordinal);
                Assert.Contains("HasCollation(", contextCode, StringComparison.Ordinal);

                if (File.Exists(warningJsonPath))
                {
                    using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                    var providerOwned = warningJson.RootElement
                        .GetProperty("providerOwnedSchemaFeatures")
                        .EnumerateArray()
                        .ToArray();

                    Assert.DoesNotContain(providerOwned, item =>
                        LastTableNameEquals(item.GetProperty("table").GetString(), FeatureOwnedTable) &&
                        item.GetProperty("kind").GetString() is "CheckConstraint" or "Computed" or "Collation");
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownFeatureOwnedMetadataAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_reports_trigger_diagnostics_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupTriggerDiagnosticsAsync(connection, provider, kind);
            }
            catch (DbException ex)
            {
                await TeardownTriggerDiagnosticsAsync(connection, provider, kind);
                if (Skip.If(true, $"{kind} trigger diagnostics setup is not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_trigger_diag_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldTriggerDiagnosticsContext",
                    new ScaffoldOptions { Tables = new[] { TriggerDiagnosticsTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, TriggerDiagnosticsTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
                Assert.Contains("Touched { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.True(File.Exists(warningJsonPath), "Trigger diagnostics must write the scaffold warning JSON report.");

                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                var providerOwned = warningJson.RootElement
                    .GetProperty("providerOwnedSchemaFeatures")
                    .EnumerateArray()
                    .ToArray();

                var triggerDiagnostic = Assert.Single(providerOwned, item =>
                    item.GetProperty("code").GetString() == "SCF110" &&
                    item.GetProperty("category").GetString() == "database-object" &&
                    item.GetProperty("kind").GetString() == "Trigger" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), TriggerDiagnosticsTable) &&
                    item.GetProperty("name").GetString() == TriggerDiagnosticsTrigger &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("trigger", StringComparison.OrdinalIgnoreCase));
                var triggerMetadata = triggerDiagnostic.GetProperty("metadata");
                Assert.Equal("Trigger", triggerMetadata.GetProperty("providerObjectKind").GetString());
                Assert.True(LastTableNameEquals(triggerMetadata.GetProperty("table").GetString(), TriggerDiagnosticsTable));
                Assert.Equal(TriggerDiagnosticsTrigger, triggerMetadata.GetProperty("triggerName").GetString());
                Assert.True(triggerMetadata.GetProperty("providerOwnedDdl").GetBoolean());
                Assert.False(triggerMetadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
                Assert.True(triggerMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(triggerMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-owned-trigger", triggerMetadata.GetProperty("reason").GetString());
                switch (kind)
                {
                    case ProviderKind.SqlServer:
                        Assert.Equal("AFTER", triggerMetadata.GetProperty("timing").GetString());
                        Assert.False(triggerMetadata.GetProperty("isDisabled").GetBoolean());
                        Assert.False(triggerMetadata.GetProperty("isInsteadOf").GetBoolean());
                        break;
                    case ProviderKind.Postgres:
                    case ProviderKind.MySql:
                        Assert.Equal("BEFORE", triggerMetadata.GetProperty("timing").GetString());
                        Assert.Equal("INSERT", triggerMetadata.GetProperty("event").GetString());
                        Assert.Equal("ROW", triggerMetadata.GetProperty("orientation").GetString());
                        break;
                    case ProviderKind.Sqlite:
                        Assert.True(triggerMetadata.GetProperty("definitionAvailable").GetBoolean());
                        Assert.Contains("CREATE TRIGGER", triggerMetadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
                        break;
                }

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownTriggerDiagnosticsAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_view_filter_emits_query_artifact_on_live_provider(ProviderKind kind)
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
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldViewContext",
                    new ScaffoldOptions { Tables = new[] { WarningView }, OverwriteFiles = false });

                var viewCode = await File.ReadAllTextAsync(Path.Combine(dir, WarningView + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldViewContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Table(\"{WarningView}", viewCode, StringComparison.Ordinal);
                Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{WarningView}>", contextCode, StringComparison.Ordinal);
                Assert.True(File.Exists(warningJsonPath));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(warningJsonPath));
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString()!.EndsWith(WarningView, StringComparison.Ordinal));
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

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_materialized_view_as_read_only_query_artifact()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresMaterializedViewAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_matview_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresMaterializedViewContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "public." + PostgresMaterializedView },
                        EmitQueryArtifacts = true,
                        OverwriteFiles = false
                    });

                var viewCode = await File.ReadAllTextAsync(Path.Combine(dir, PostgresMaterializedView + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresMaterializedViewContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{PostgresMaterializedView}>", contextCode, StringComparison.Ordinal);
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString() == "public." + PostgresMaterializedView);
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPostgresMaterializedViewAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlserver_local_table_synonym_as_read_only_query_artifact()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerSynonymAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_synonym_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerSynonymContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "dbo." + SqlServerWarningSynonym },
                        EmitQueryArtifacts = true,
                        OverwriteFiles = false
                    });

                var synonymCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerWarningSynonym + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqlServerSynonymContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                Assert.Contains("[ReadOnlyEntity]", synonymCode, StringComparison.Ordinal);
                Assert.Contains($"[Table(\"{SqlServerWarningSynonym}", synonymCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{SqlServerWarningSynonym}>", contextCode, StringComparison.Ordinal);
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerWarningSynonym);
                var dynamicSynonymType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + SqlServerWarningSynonym);
                Assert.NotNull(dynamicSynonymType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerSynonymAsync(connection, provider);
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
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains($"[Index(\"{ProviderPartialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{ProviderDescendingIndex}\", IsDescending = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("PartialIndex", warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderPartialIndex, warnings, StringComparison.Ordinal);
                Assert.DoesNotContain(ProviderDescendingIndex, warnings, StringComparison.Ordinal);

                if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
                {
                    Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderPartialExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.Contains("filterSql:", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderPartialExpressionIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionDescendingIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionDescendingIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("name").GetString() == ProviderExpressionDescendingIndex);
                }

                if (kind is ProviderKind.Postgres)
                {
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, entityCode, StringComparison.Ordinal);
                    Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionLiteralDescIndex}\"", contextCode, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderExpressionLiteralDescIndex, warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "DescendingIndex" &&
                        item.GetProperty("name").GetString() == ProviderExpressionLiteralDescIndex);
                }

                if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                {
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\")]", entityCode, StringComparison.Ordinal);
                    Assert.Contains($"[Index(\"{ProviderIncludedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("IncludedColumnIndex", warnings, StringComparison.Ordinal);
                    Assert.DoesNotContain(ProviderIncludedIndex, warnings, StringComparison.Ordinal);
                }

                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PartialIndex" &&
                    item.GetProperty("name").GetString() == ProviderPartialIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderIncludedIndex);
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "DescendingIndex" &&
                    item.GetProperty("name").GetString() == ProviderDescendingIndex);
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
                Assert.Contains($"[Index(\"{ProviderFullPrefixIndex}\")]", entityCode, StringComparison.Ordinal);
                var prefix = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PrefixIndex" &&
                    item.GetProperty("name").GetString() == ProviderPrefixIndex);
                Assert.Equal("SCF117", prefix.GetProperty("code").GetString());
                Assert.Equal("index", prefix.GetProperty("category").GetString());
                var prefixMetadata = prefix.GetProperty("metadata");
                var prefixColumn = Assert.Single(prefixMetadata.GetProperty("prefixColumns").EnumerateArray());
                Assert.Equal("Name", prefixColumn.GetProperty("name").GetString());
                Assert.Equal(8, prefixColumn.GetProperty("prefixLength").GetInt32());
                Assert.DoesNotContain(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "PrefixIndex" &&
                    item.GetProperty("name").GetString() == ProviderFullPrefixIndex);
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
    public async Task ScaffoldAsync_reports_mysql_expression_index_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupMySqlExpressionIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
                if (Skip.If(true, $"MySQL expression indexes are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_expression_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlExpressionIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldMySqlExpressionIndexContext.cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.DoesNotContain(ProviderExpressionIndex, entityCode, StringComparison.Ordinal);
                Assert.Contains($"HasExpressionIndex(\"{ProviderExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains("LOWER", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.Contains(provider.Escape("Score"), contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported MySQL expression indexes should scaffold as provider-bound expression-index metadata.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.MySql);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_reports_provider_specific_index_access_methods_as_provider_owned(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupProviderSpecificAccessMethodIndexAsync(connection, provider, kind);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, kind);
                if (Skip.If(true, $"{kind} provider-specific index access method is not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_provider_specific_index_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldProviderSpecificIndexContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldProviderSpecificIndexContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderSpecificIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderSpecificIndex}\"", contextCode, StringComparison.Ordinal);
                var providerSpecific = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
                Assert.Equal("index", providerSpecific.GetProperty("category").GetString());
                var providerSpecificMetadata = providerSpecific.GetProperty("metadata");
                if (kind is ProviderKind.SqlServer)
                {
                    Assert.Equal("SQL Server", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Contains("COLUMNSTORE", providerSpecificMetadata.GetProperty("indexType").GetString(), StringComparison.OrdinalIgnoreCase);
                }
                else if (kind is ProviderKind.Postgres)
                {
                    Assert.Equal("PostgreSQL", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Equal("hash", providerSpecificMetadata.GetProperty("accessMethod").GetString());
                    Assert.Contains("USING hash", providerSpecificMetadata.GetProperty("indexSql").GetString(), StringComparison.OrdinalIgnoreCase);
                }
                else if (kind is ProviderKind.MySql)
                {
                    Assert.Equal("MySQL", providerSpecificMetadata.GetProperty("provider").GetString());
                    Assert.Equal("FULLTEXT", providerSpecificMetadata.GetProperty("indexType").GetString());
                }

                if (kind is ProviderKind.Postgres)
                {
                    var expression = Assert.Single(providerOwned, item =>
                        item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                        item.GetProperty("name").GetString() == ProviderSpecificIndex);
                    Assert.Equal("SCF112", expression.GetProperty("code").GetString());
                    Assert.Equal("index", expression.GetProperty("category").GetString());
                }
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
    public async Task ScaffoldAsync_reports_postgres_expression_index_with_include_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresExpressionIncludedIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_expression_include_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresExpressionIncludeContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresExpressionIncludeContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderExpressionIncludedIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderExpressionIncludedIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                    item.GetProperty("name").GetString() == ProviderExpressionIncludedIndex);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "IncludedColumnIndex" &&
                    item.GetProperty("name").GetString() == ProviderExpressionIncludedIndex);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_null_sort_order_index_metadata()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPostgresProviderSpecificBtreeOptionIndexAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_null_sort_order_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresNullSortOrderContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Index(\"{ProviderSpecificIndex}\", NullSortOrder = IndexNullSortOrder.First)]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported PostgreSQL NULLS FIRST/LAST column indexes should not produce provider-owned warnings.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_postgres_expression_btree_key_options_as_provider_owned()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupPostgresExpressionBtreeOptionIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
                if (Skip.If(true, $"PostgreSQL expression B-tree key options are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_expression_btree_options_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresExpressionBtreeOptionsContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPostgresExpressionBtreeOptionsContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.DoesNotContain(ProviderSpecificIndex, entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"HasExpressionIndex(\"{ProviderSpecificIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                var providerSpecific = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                    item.GetProperty("name").GetString() == ProviderSpecificIndex);
                Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
                Assert.Equal("index", providerSpecific.GetProperty("category").GetString());
                Assert.Contains("provider-specific key options", providerSpecific.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);
                var metadata = providerSpecific.GetProperty("metadata");
                Assert.Equal("PostgreSQL", metadata.GetProperty("provider").GetString());
                Assert.Equal("btree", metadata.GetProperty("accessMethod").GetString());
                Assert.True(metadata.GetProperty("hasNonDefaultOperatorClass").GetBoolean());
                Assert.False(metadata.GetProperty("hasNullsNotDistinct").GetBoolean());
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_postgres_nulls_not_distinct_unique_index_metadata()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider PostgreSQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupPostgresNullsNotDistinctUniqueIndexAsync(connection, provider);
            }
            catch (DbException ex)
            {
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
                if (Skip.If(true, $"PostgreSQL NULLS NOT DISTINCT indexes are not available on this server: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_postgres_nulls_not_distinct_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresNullsNotDistinctContext",
                    new ScaffoldOptions { Tables = new[] { ProviderIndexTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, ProviderIndexTable + ".cs"));
                var warningJsonPath = Path.Combine(dir, "nORM.ScaffoldWarnings.json");

                Assert.Contains($"[Index(\"{ProviderSpecificIndex}\", IsUnique = true, NullsNotDistinct = true)]", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("ProviderSpecificIndex", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(warningJsonPath), "Supported PostgreSQL NULLS NOT DISTINCT indexes should not produce provider-owned warnings.");
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownProviderSpecificIndexesAsync(connection, provider, ProviderKind.Postgres);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_sqlserver_native_temporal_tables_and_marks_them_read_only()
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

                Assert.Contains("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
                Assert.Contains("[ReadOnlyEntity]", historyCode, StringComparison.Ordinal);
                var baseTemporal = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalBaseTable &&
                    item.GetProperty("code").GetString() == "SCF115");
                var baseTemporalMetadata = baseTemporal.GetProperty("metadata");
                Assert.Equal("TemporalTable", baseTemporalMetadata.GetProperty("providerObjectKind").GetString());
                Assert.True(baseTemporalMetadata.GetProperty("providerNativeTemporal").GetBoolean());
                Assert.False(baseTemporalMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
                Assert.True(baseTemporalMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(baseTemporalMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-native-temporal", baseTemporalMetadata.GetProperty("reason").GetString());
                Assert.Equal("system-versioned", baseTemporalMetadata.GetProperty("temporalType").GetString());
                Assert.Contains(SqlServerTemporalHistoryTable, baseTemporalMetadata.GetProperty("historyTable").GetString(), StringComparison.OrdinalIgnoreCase);
                var historyTemporal = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalHistoryTable &&
                    item.GetProperty("detail").GetString()!.Contains("history table", StringComparison.OrdinalIgnoreCase));
                var historyTemporalMetadata = historyTemporal.GetProperty("metadata");
                Assert.Equal("history", historyTemporalMetadata.GetProperty("temporalType").GetString());
                Assert.True(historyTemporalMetadata.GetProperty("providerNativeTemporal").GetBoolean());
                Assert.False(historyTemporalMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
                Assert.True(historyTemporalMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(historyTemporalMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-native-temporal", historyTemporalMetadata.GetProperty("reason").GetString());

                var dynamicBaseType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + SqlServerTemporalBaseTable);
                var dynamicHistoryType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + SqlServerTemporalHistoryTable);
                Assert.NotNull(dynamicBaseType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.NotNull(dynamicHistoryType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());

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
    public async Task ScaffoldAsync_rejects_sqlserver_procedure_synonym_as_entity_filter()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerProcedureSynonymAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_proc_synonym_" + Guid.NewGuid().ToString("N"));
            try
            {
                var ex = await Assert.ThrowsAsync<nORM.Core.NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldSqlServerProcedureSynonymContext",
                        new ScaffoldOptions
                        {
                            Tables = new[] { "dbo." + SqlServerProcedureSynonym },
                            EmitQueryArtifacts = true,
                            OverwriteFiles = false
                        }));

                Assert.Contains("matched database object", ex.Message, StringComparison.Ordinal);
                Assert.Contains("Synonym dbo." + SqlServerProcedureSynonym, ex.Message, StringComparison.Ordinal);
                Assert.Contains("does not emit as entity classes", ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerProcedureSynonymAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_event_diagnostics_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupMySqlEventDiagnosticsAsync(connection, provider);
            }
            catch (Exception ex)
            {
                await TeardownMySqlEventDiagnosticsAsync(connection, provider);
                if (Skip.If(true, $"MySQL EVENT privilege is not available in this live database: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_event_diag_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlEventDiagnosticsContext",
                    new ScaffoldOptions { Tables = new[] { MySqlEventDiagnosticsName }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, MySqlEventDiagnosticsName + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains("public int Id { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "Event" &&
                    item.GetProperty("code").GetString() == "SCF205" &&
                    item.GetProperty("category").GetString() == "routine" &&
                    item.GetProperty("name").GetString() == MySqlEventDiagnosticsName &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("scheduled event", StringComparison.OrdinalIgnoreCase));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlEventDiagnosticsAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlite_virtual_table_as_read_only_query_artifact()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Sqlite);
        if (Skip.If(live is null, "Live provider SQLite not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, SqliteVirtualTable, provider.Escape(SqliteVirtualTable)));
            try
            {
                await ExecuteAsync(connection,
                    $"CREATE VIRTUAL TABLE {provider.Escape(SqliteVirtualTable)} USING fts5({provider.Escape("Content")})");
            }
            catch (Exception ex)
            {
                if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlite_virtual_table_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqliteVirtualTableContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SqliteVirtualTable },
                        EmitQueryArtifacts = true,
                        OverwriteFiles = false
                    });

                var virtualCode = await File.ReadAllTextAsync(Path.Combine(dir, SqliteVirtualTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqliteVirtualTableContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
                var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", virtualCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{SqliteVirtualTable}>", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString() == SqliteVirtualTable);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                    item.GetProperty("code").GetString() == "SCF207" &&
                    item.GetProperty("name").GetString()!.StartsWith(SqliteVirtualTable + "_", StringComparison.Ordinal));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, SqliteVirtualTable, provider.Escape(SqliteVirtualTable)));
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

    [Fact]
    public async Task ScaffoldAsync_marks_sqlserver_rowversion_as_timestamp_and_database_generated()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, SqlServerRowVersionTable, provider.Escape(SqlServerRowVersionTable)));
            var table = provider.Escape(SqlServerRowVersionTable);
            var id = provider.Escape("Id");
            var name = provider.Escape("Name");
            var rowVersion = provider.Escape("RowVersion");
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_rowversion_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {name} NVARCHAR(80) NOT NULL, {rowVersion} rowversion NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRowVersionContext",
                    new ScaffoldOptions { Tables = new[] { SqlServerRowVersionTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerRowVersionTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRowVersionContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[Timestamp]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public byte[] RowVersion { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain(".Property(e => e.RowVersion).HasComputedColumnSql", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "RowVersion" &&
                    item.GetProperty("code").GetString() == "SCF108" &&
                    item.GetProperty("table").GetString()!.EndsWith(SqlServerRowVersionTable, StringComparison.Ordinal));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, SqlServerRowVersionTable, provider.Escape(SqlServerRowVersionTable)));
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

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_marks_identity_columns_as_database_generated_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicIdentityTable, provider.Escape(DynamicIdentityTable)));
            try
            {
                await ExecuteAsync(connection, IdentityColumnTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicIdentityTable);
                var generated = type.GetProperty("Id")!
                    .GetCustomAttributes(typeof(DatabaseGeneratedAttribute), inherit: false)
                    .Cast<DatabaseGeneratedAttribute>()
                    .SingleOrDefault();

                Assert.NotNull(generated);
                Assert.Equal(DatabaseGeneratedOption.Identity, generated.DatabaseGeneratedOption);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicIdentityTable, provider.Escape(DynamicIdentityTable)));
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Dynamic_scaffolding_preserves_composite_primary_key_order_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(kind, DynamicCompositeKeyTable, provider.Escape(DynamicCompositeKeyTable)));
            try
            {
                await ExecuteAsync(connection, DynamicCompositeKeyTableSql(kind, provider));

                var type = new DynamicEntityTypeGenerator().GenerateEntityType(connection, DynamicCompositeKeyTable);
                var properties = type.GetProperties().Select(prop => prop.Name).ToArray();

                Assert.Equal("LocalId", properties[0]);
                Assert.Equal("TenantId", properties[1]);
                Assert.Contains(
                    type.GetProperty("LocalId")!.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.KeyAttribute), inherit: false),
                    attr => attr is System.ComponentModel.DataAnnotations.KeyAttribute);
                Assert.Contains(
                    type.GetProperty("TenantId")!.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.KeyAttribute), inherit: false),
                    attr => attr is System.ComponentModel.DataAnnotations.KeyAttribute);
            }
            finally
            {
                await ExecuteAsync(connection, DropTable(kind, DynamicCompositeKeyTable, provider.Escape(DynamicCompositeKeyTable)));
            }
        }
    }

    private static string ExpectedCascadeForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, cascadeDelete: false);"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, \"{constraintName}\", false);";

    private static string ExpectedReferentialForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string onDelete,
        string onUpdate,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate});"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate}, \"{constraintName}\");";

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

    private static async Task SetupDatabaseNamesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownDatabaseNamesAsync(connection, provider, kind);

        var customer = provider.Escape(DatabaseNamesCustomerTable);
        var orderLine = provider.Escape(DatabaseNamesOrderLineTable);
        var customerId = provider.Escape("scaffold_database_names_customer_id");
        var displayName = provider.Escape("display_name");
        var orderLineId = provider.Escape("scaffold_database_names_order_line_id");
        var billingCustomerId = provider.Escape("billing_scaffold_database_names_customer_id");
        var shippingCustomerId = provider.Escape("shipping_scaffold_database_names_customer_id");
        var sku = provider.Escape("SKU");
        var classColumn = provider.Escape("class");
        var hasSpace = provider.Escape("has space");

        await ExecuteAsync(connection,
            $"CREATE TABLE {customer} ({customerId} {IntType(kind)} NOT NULL PRIMARY KEY, {displayName} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {orderLine} ({orderLineId} {IntType(kind)} NOT NULL PRIMARY KEY, {billingCustomerId} {IntType(kind)} NOT NULL, {shippingCustomerId} {IntType(kind)} NULL, {sku} {TextType(kind, 40)} NOT NULL, {classColumn} {TextType(kind, 40)} NULL, {hasSpace} {TextType(kind, 40)} NULL, " +
            $"CONSTRAINT {provider.Escape(DatabaseNamesBillingFkName)} FOREIGN KEY ({billingCustomerId}) REFERENCES {customer} ({customerId}), " +
            $"CONSTRAINT {provider.Escape(DatabaseNamesShippingFkName)} FOREIGN KEY ({shippingCustomerId}) REFERENCES {customer} ({customerId}))");
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

    private static async Task SetupSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorBookTable, provider.Escape(SurrogateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SurrogateBookTable, provider.Escape(SurrogateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorTable, provider.Escape(SurrogateAuthorTable)));

        var author = provider.Escape(SurrogateAuthorTable);
        var book = provider.Escape(SurrogateBookTable);
        var join = provider.Escape(SurrogateAuthorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection, $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({IdentityPrimaryKeyColumn(kind, id)}, {authorId} {IntType(kind)} NOT NULL, {bookId} {IntType(kind)} NOT NULL, UNIQUE ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(SurrogateAuthorBookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(SurrogateAuthorBookBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");
    }

    private static async Task SetupGeneratedBridgeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownGeneratedBridgeManyToManyAsync(connection, provider, kind);

        var student = provider.Escape(GeneratedBridgeStudentTable);
        var course = provider.Escape(GeneratedBridgeCourseTable);
        var join = provider.Escape(GeneratedBridgeStudentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var pairSum = provider.Escape("PairSum");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var generatedColumn = kind switch
        {
            ProviderKind.SqlServer => $"{pairSum} AS ({studentId} + {courseId}) PERSISTED",
            ProviderKind.Postgres => $"{pairSum} integer GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.MySql => $"{pairSum} INT GENERATED ALWAYS AS ({studentId} + {courseId}) STORED",
            ProviderKind.Sqlite => $"{pairSum} INTEGER GENERATED ALWAYS AS ({studentId} + {courseId}) VIRTUAL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, $"CREATE TABLE {student} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE TABLE {course} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {generatedColumn}, PRIMARY KEY ({studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(GeneratedBridgeStudentCourseStudentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(GeneratedBridgeStudentCourseCourseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
    }

    private static async Task SetupReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialChildTable, provider.Escape(ReferentialChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialParentTable, provider.Escape(ReferentialParentTable)));

        var parent = provider.Escape(ReferentialParentTable);
        var child = provider.Escape(ReferentialChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET NULL ON UPDATE CASCADE)");
    }

    private static async Task SetupRestrictReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictChildTable, provider.Escape(ReferentialRestrictChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictParentTable, provider.Escape(ReferentialRestrictParentTable)));

        var parent = provider.Escape(ReferentialRestrictParentTable);
        var child = provider.Escape(ReferentialRestrictChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialRestrictFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE)");
    }

    private static async Task SetupSetDefaultReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultChildTable, provider.Escape(ReferentialDefaultChildTable)));
        await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultParentTable, provider.Escape(ReferentialDefaultParentTable)));

        var parent = provider.Escape(ReferentialDefaultParentTable);
        var child = provider.Escape(ReferentialDefaultChildTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape("DF_ScaffoldLiveDefaultChild_ParentId")} DEFAULT (0)"
            : "DEFAULT 0";

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL {defaultClause}, {name} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(ReferentialDefaultFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)");
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

    private static async Task SetupCompositePayloadJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentCourseTable, provider.Escape(CompositePayloadStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadCourseTable, provider.Escape(CompositePayloadCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentTable, provider.Escape(CompositePayloadStudentTable)));

        var student = provider.Escape(CompositePayloadStudentTable);
        var course = provider.Escape(CompositePayloadCourseTable);
        var join = provider.Escape(CompositePayloadStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var studentTenantId = provider.Escape("StudentTenantId");
        var courseTenantId = provider.Escape("CourseTenantId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var enrollmentCode = provider.Escape("EnrollmentCode");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {enrollmentCode} {TextType(kind, 40)} NOT NULL, " +
            $"PRIMARY KEY ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositePayloadStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositePayloadStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupCompositeSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentCourseTable, provider.Escape(CompositeSurrogateStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateCourseTable, provider.Escape(CompositeSurrogateCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentTable, provider.Escape(CompositeSurrogateStudentTable)));

        var student = provider.Escape(CompositeSurrogateStudentTable);
        var course = provider.Escape(CompositeSurrogateCourseTable);
        var join = provider.Escape(CompositeSurrogateStudentCourseTable);
        var id = provider.Escape("Id");
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
            $"CREATE TABLE {join} ({IdentityPrimaryKeyColumn(kind, id)}, {studentTenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseTenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"UNIQUE ({studentTenantId}, {studentId}, {courseTenantId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(CompositeSurrogateStudentCourseStudentFkName)} FOREIGN KEY ({studentTenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(CompositeSurrogateStudentCourseCourseFkName)} FOREIGN KEY ({courseTenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");
    }

    private static async Task SetupSharedTenantManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentCourseTable, provider.Escape(SharedTenantStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedTenantCourseTable, provider.Escape(SharedTenantCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentTable, provider.Escape(SharedTenantStudentTable)));

        var student = provider.Escape(SharedTenantStudentTable);
        var course = provider.Escape(SharedTenantCourseTable);
        var join = provider.Escape(SharedTenantStudentCourseTable);
        var tenantId = provider.Escape("TenantId");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {studentId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({tenantId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, PRIMARY KEY ({tenantId}, {courseId}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({tenantId} {IntType(kind)} NOT NULL, {studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"PRIMARY KEY ({tenantId}, {studentId}, {courseId}), " +
            $"CONSTRAINT {provider.Escape(SharedTenantStudentCourseStudentFkName)} FOREIGN KEY ({tenantId}, {studentId}) REFERENCES {student} ({tenantId}, {studentId}), " +
            $"CONSTRAINT {provider.Escape(SharedTenantStudentCourseCourseFkName)} FOREIGN KEY ({tenantId}, {courseId}) REFERENCES {course} ({tenantId}, {courseId}))");

        if (kind == ProviderKind.MySql)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveSharedTenantStudentCourse_Course")} ON {join} ({tenantId}, {courseId})");
    }

    private static async Task SetupSharedAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorBookTable, provider.Escape(SharedAlternateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateBookTable, provider.Escape(SharedAlternateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorTable, provider.Escape(SharedAlternateAuthorTable)));

        var author = provider.Escape(SharedAlternateAuthorTable);
        var book = provider.Escape(SharedAlternateBookTable);
        var join = provider.Escape(SharedAlternateAuthorBookTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, UNIQUE ({tenantId}, {code}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {isbn} {TextType(kind, 40)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, UNIQUE ({tenantId}, {isbn}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({tenantId} {IntType(kind)} NOT NULL, {authorCode} {TextType(kind, 40)} NOT NULL, {bookIsbn} {TextType(kind, 40)} NOT NULL, " +
            $"PRIMARY KEY ({tenantId}, {authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(SharedAlternateAuthorBookAuthorFkName)} FOREIGN KEY ({tenantId}, {authorCode}) REFERENCES {author} ({tenantId}, {code}), " +
            $"CONSTRAINT {provider.Escape(SharedAlternateAuthorBookBookFkName)} FOREIGN KEY ({tenantId}, {bookIsbn}) REFERENCES {book} ({tenantId}, {isbn}))");

        if (kind == ProviderKind.MySql)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape("IX_ScaffoldLiveSharedAlternateAuthorBook_Book")} ON {join} ({tenantId}, {bookIsbn})");
    }

    private static async Task SetupAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, AlternateAuthorBookTable, provider.Escape(AlternateAuthorBookTable)));
        await ExecuteAsync(connection, DropTable(kind, AlternateBookTable, provider.Escape(AlternateBookTable)));
        await ExecuteAsync(connection, DropTable(kind, AlternateAuthorTable, provider.Escape(AlternateAuthorTable)));

        var author = provider.Escape(AlternateAuthorTable);
        var book = provider.Escape(AlternateBookTable);
        var join = provider.Escape(AlternateAuthorBookTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var isbn = provider.Escape("Isbn");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorCode = provider.Escape("AuthorCode");
        var bookIsbn = provider.Escape("BookIsbn");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, UNIQUE ({code}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {isbn} {TextType(kind, 40)} NOT NULL, {title} {TextType(kind, 80)} NOT NULL, UNIQUE ({isbn}))");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({authorCode} {TextType(kind, 40)} NOT NULL, {bookIsbn} {TextType(kind, 40)} NOT NULL, PRIMARY KEY ({authorCode}, {bookIsbn}), " +
            $"CONSTRAINT {provider.Escape(AlternateAuthorBookAuthorFkName)} FOREIGN KEY ({authorCode}) REFERENCES {author} ({code}), " +
            $"CONSTRAINT {provider.Escape(AlternateAuthorBookBookFkName)} FOREIGN KEY ({bookIsbn}) REFERENCES {book} ({isbn}))");
    }

    private static async Task SetupSelfReferencingManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, SelfPersonRelationshipTable, provider.Escape(SelfPersonRelationshipTable)));
        await ExecuteAsync(connection, DropTable(kind, SelfPersonTable, provider.Escape(SelfPersonTable)));

        var person = provider.Escape(SelfPersonTable);
        var relationship = provider.Escape(SelfPersonRelationshipTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var mentorId = provider.Escape("MentorId");
        var menteeId = provider.Escape("MenteeId");

        await ExecuteAsync(connection,
            $"CREATE TABLE {person} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {relationship} ({mentorId} {IntType(kind)} NOT NULL, {menteeId} {IntType(kind)} NOT NULL, PRIMARY KEY ({mentorId}, {menteeId}), " +
            $"CONSTRAINT {provider.Escape(SelfPersonRelationshipMentorFkName)} FOREIGN KEY ({mentorId}) REFERENCES {person} ({id}), " +
            $"CONSTRAINT {provider.Escape(SelfPersonRelationshipMenteeFkName)} FOREIGN KEY ({menteeId}) REFERENCES {person} ({id}))");
    }

    private static async Task SetupFilteredUniqueSurrogateJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, FilteredStudentCourseTable, provider.Escape(FilteredStudentCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, FilteredCourseTable, provider.Escape(FilteredCourseTable)));
        await ExecuteAsync(connection, DropTable(kind, FilteredStudentTable, provider.Escape(FilteredStudentTable)));

        var student = provider.Escape(FilteredStudentTable);
        var course = provider.Escape(FilteredCourseTable);
        var join = provider.Escape(FilteredStudentCourseTable);
        var id = provider.Escape("Id");
        var studentId = provider.Escape("StudentId");
        var courseId = provider.Escape("CourseId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var activePredicate = $"{studentId} > 0";

        await ExecuteAsync(connection,
            $"CREATE TABLE {student} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {course} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {studentId} {IntType(kind)} NOT NULL, {courseId} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(FilteredStudentCourseStudentFkName)} FOREIGN KEY ({studentId}) REFERENCES {student} ({id}), " +
            $"CONSTRAINT {provider.Escape(FilteredStudentCourseCourseFkName)} FOREIGN KEY ({courseId}) REFERENCES {course} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(FilteredStudentCourseUniqueIndex)} ON {join} ({studentId}, {courseId}) WHERE {activePredicate}");
    }

    private static async Task SetupSchemaQualifiedManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSchemaQualifiedManyToManyAsync(connection, provider, kind);

        if (kind == ProviderKind.Sqlite)
        {
            try
            {
                await ExecuteAsync(connection, $"DETACH DATABASE {provider.Escape(SchemaName)}");
            }
            catch
            {
                // Attachment may not exist before setup.
            }

            await ExecuteAsync(connection, $"ATTACH DATABASE ':memory:' AS {provider.Escape(SchemaName)}");
        }
        else if (kind == ProviderKind.SqlServer)
        {
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{SchemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(SchemaName)}')");
        }
        else
        {
            await ExecuteAsync(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(SchemaName)}");
        }

        var author = Qualified(provider, SchemaName, SchemaAuthorTable);
        var book = Qualified(provider, SchemaName, SchemaBookTable);
        var join = Qualified(provider, SchemaName, SchemaAuthorBookTable);
        var fkAuthor = kind == ProviderKind.Sqlite ? provider.Escape(SchemaAuthorTable) : author;
        var fkBook = kind == ProviderKind.Sqlite ? provider.Escape(SchemaBookTable) : book;
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");

        await ExecuteAsync(connection,
            $"CREATE TABLE {author} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {book} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {title} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {join} ({authorId} {IntType(kind)} NOT NULL, {bookId} {IntType(kind)} NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(SchemaAuthorBookAuthorFkName)} FOREIGN KEY ({authorId}) REFERENCES {fkAuthor} ({id}), " +
            $"CONSTRAINT {provider.Escape(SchemaAuthorBookBookFkName)} FOREIGN KEY ({bookId}) REFERENCES {fkBook} ({id}))");
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

    private static async Task SetupCompositeRoleForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownCompositeRoleForeignKeysAsync(connection, provider, kind);

        var account = provider.Escape(CompositeRoleAccountTable);
        var transfer = provider.Escape(CompositeRoleTransferTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var primaryAccountNo = provider.Escape("PrimaryAccountNo");
        var backupAccountNo = provider.Escape("BackupAccountNo");
        var name = provider.Escape("Name");
        var amount = provider.Escape("Amount");

        await ExecuteAsync(connection,
            $"CREATE TABLE {account} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeRoleAccountIndexName)} ON {account} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {transfer} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {primaryAccountNo} {IntType(kind)} NOT NULL, {backupAccountNo} {IntType(kind)} NOT NULL, {amount} {IntType(kind)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeRolePrimaryFkName)} FOREIGN KEY ({tenantId}, {primaryAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}), " +
            $"CONSTRAINT {provider.Escape(CompositeRoleBackupFkName)} FOREIGN KEY ({tenantId}, {backupAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}))");
    }

    private static async Task SetupSingleColumnAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSingleColumnAlternateKeyAsync(connection, provider, kind);

        var parent = provider.Escape(SingleAlternateParentTable);
        var child = provider.Escape(SingleAlternateChildTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(SingleAlternateIndexName)} ON {parent} ({code})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentCode} {TextType(kind, 40)} NOT NULL, {notes} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(SingleAlternateFkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static async Task SetupNullableAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownNullableAlternateKeyAsync(connection, provider, kind);

        var parent = provider.Escape(NullableAlternateParentTable);
        var child = provider.Escape(NullableAlternateChildTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {TextType(kind, 40)} NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(NullableAlternateIndexName)} ON {parent} ({code})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {child} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentCode} {TextType(kind, 40)} NOT NULL, {notes} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(NullableAlternateFkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static async Task SetupUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(UniqueDependentParentTable);
        var profile = provider.Escape(UniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(UniqueDependentFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(UniqueDependentIndexName)} ON {profile} ({parentId})");
    }

    private static async Task SetupOptionalUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownOptionalUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(OptionalUniqueParentTable);
        var profile = provider.Escape(OptionalUniqueProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(OptionalUniqueFkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalUniqueIndexName)} ON {profile} ({parentId})");
    }

    private static async Task SetupRoleNamedUniqueDependentForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownRoleNamedUniqueDependentForeignKeysAsync(connection, provider, kind);

        var parent = provider.Escape(RoleOneParentTable);
        var profile = provider.Escape(RoleOneProfileTable);
        var id = provider.Escape("Id");
        var primaryAccountId = provider.Escape("PrimaryAccountId");
        var backupAccountId = provider.Escape("BackupAccountId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {primaryAccountId} {IntType(kind)} NOT NULL, {backupAccountId} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(RoleOnePrimaryFkName)} FOREIGN KEY ({primaryAccountId}) REFERENCES {parent} ({id}), " +
            $"CONSTRAINT {provider.Escape(RoleOneBackupFkName)} FOREIGN KEY ({backupAccountId}) REFERENCES {parent} ({id}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(RoleOnePrimaryIndexName)} ON {profile} ({primaryAccountId})");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(RoleOneBackupIndexName)} ON {profile} ({backupAccountId})");
    }

    private static async Task SetupSharedPrimaryKeyForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSharedPrimaryKeyForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(SharedPkParentTable);
        var profile = provider.Escape(SharedPkProfileTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(SharedPkFkName)} FOREIGN KEY ({id}) REFERENCES {parent} ({id}))");
    }

    private static async Task SetupCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(CompositeUniqueDependentParentTable);
        var profile = provider.Escape(CompositeUniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeUniqueDependentParentIndexName)} ON {parent} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(CompositeUniqueDependentFkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(CompositeUniqueDependentProfileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static async Task SetupOptionalCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownOptionalCompositeUniqueDependentForeignKeyAsync(connection, provider, kind);

        var parent = provider.Escape(OptionalCompositeUniqueDependentParentTable);
        var profile = provider.Escape(OptionalCompositeUniqueDependentProfileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalCompositeUniqueDependentParentIndexName)} ON {parent} ({tenantId}, {accountNo})");
        await ExecuteAsync(connection,
            $"CREATE TABLE {profile} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {tenantId} {IntType(kind)} NOT NULL, {accountNo} {IntType(kind)} NULL, {displayName} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(OptionalCompositeUniqueDependentFkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))");
        await ExecuteAsync(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(OptionalCompositeUniqueDependentProfileIndexName)} ON {profile} ({tenantId}, {accountNo})");
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

    private static async Task SetupStringBinaryFacetsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind, bool alternate)
    {
        await ExecuteAsync(connection, DropTable(kind, StringBinaryFacetTable, provider.Escape(StringBinaryFacetTable)));

        var table = provider.Escape(StringBinaryFacetTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer when alternate => ("NVARCHAR(40)", "NCHAR(12)", "VARBINARY(16)"),
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres when alternate => ("CHAR(40)", "VARCHAR(12)", "BYTEA"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql when alternate => ("CHAR(40)", "VARCHAR(12)", "VARBINARY(16)"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "String/binary facet live test only targets SQL Server, PostgreSQL, and MySQL.")
        };

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
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

    private static async Task SetupSqlServerNonQueryRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerNonQueryRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineNonQueryName)} @tenantId INT, @status NVARCHAR(32) OUTPUT AS BEGIN SET NOCOUNT ON; SET @status = N'ok'; DECLARE @ignored INT = @tenantId; END");
    }

    private static async Task SetupSqlServerTableValuedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerTableValuedParameterRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE TYPE {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)} AS TABLE ({provider.Escape("ProductId")} INT NOT NULL, {provider.Escape("Quantity")} INT NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineTableValuedParameterName)} @tenantId INT, @items {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)} READONLY AS SELECT @tenantId AS Id, COUNT(*) AS LineCount FROM @items");
    }

    private static async Task SetupSqlServerFunctionRoutinesAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerFunctionRoutinesAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerScalarFunctionName)} (@customerId INT) RETURNS INT AS BEGIN RETURN @customerId + 7; END");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerTableValuedFunctionName)} (@tenantId INT) RETURNS TABLE AS RETURN SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
    }

    private static async Task SetupSequenceAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownSequenceAsync(connection, provider, kind);

        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(SequenceName)
            : provider.Escape("public") + "." + provider.Escape(SequenceName);
        var sql = kind switch
        {
            ProviderKind.SqlServer => $"CREATE SEQUENCE {qualifiedName} AS BIGINT START WITH 42 INCREMENT BY 1",
            ProviderKind.Postgres => $"CREATE SEQUENCE {qualifiedName} AS integer START WITH 42 INCREMENT BY 1",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffolding live test only targets SQL Server and PostgreSQL.")
        };
        await ExecuteAsync(connection, sql);
    }

    private static async Task SetupPostgresSetReturningRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresSetReturningRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(PostgresSetReturningRoutineName)}(tenantId integer) RETURNS SETOF integer LANGUAGE SQL AS $$ SELECT tenantId $$");
    }

    private static async Task SetupPostgresTypedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresTypedRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(PostgresTypedRoutineName)}(ids integer[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) $$");
    }

    private static async Task SetupPostgresOverloadedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresOverloadedRoutineAsync(connection, provider);

        var routine = provider.Escape("public") + "." + provider.Escape(PostgresOverloadedRoutineName);
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}(value integer) RETURNS integer LANGUAGE SQL AS $$ SELECT value $$");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}(value text) RETURNS integer LANGUAGE SQL AS $$ SELECT char_length(value) $$");
    }

    private static async Task SetupPostgresQuotedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresQuotedParameterRoutineAsync(connection, provider);

        var routine = provider.Escape("public") + "." + provider.Escape(PostgresQuotedParameterRoutineName);
        var tenantId = provider.Escape("tenant-id");
        var searchText = provider.Escape("search text");
        await ExecuteAsync(connection,
            $"CREATE FUNCTION {routine}({tenantId} integer, {searchText} text) RETURNS integer LANGUAGE SQL AS $$ SELECT {tenantId} + length({searchText}) $$");
    }

    private static async Task SetupMySqlUnsignedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsignedRoutineAsync(connection, provider);

        await ExecuteAsync(connection,
            $"CREATE FUNCTION {provider.Escape(MySqlUnsignedRoutineName)}(customer_id INT UNSIGNED, max_id BIGINT UNSIGNED, {provider.Escape("rank")} SMALLINT UNSIGNED, flag TINYINT UNSIGNED) RETURNS INT DETERMINISTIC NO SQL RETURN customer_id");
    }

    private static async Task SetupPostgresTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresTypedColumnTableAsync(connection, provider);

        var table = Qualified(provider, "public", PostgresTypedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL)");
    }

    private static async Task SetupMySqlTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlTypedColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlTypedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("FiscalYear")} YEAR NOT NULL, {provider.Escape("Status")} ENUM('draft','paid','cancelled') NOT NULL, {provider.Escape("Flags")} SET('read','write','admin') NOT NULL)");
    }

    private static async Task SetupMySqlUnsafeSetColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsafeSetColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlUnsafeSetColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Flags")} SET('a','b','c','d','e','f','g','h','i') NOT NULL)");
    }

    private static async Task SetupMySqlUnsignedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlUnsignedColumnTableAsync(connection, provider);

        var table = provider.Escape(MySqlUnsignedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("UnsignedCount")} INT UNSIGNED NOT NULL, {provider.Escape("UnsignedTotal")} BIGINT UNSIGNED NOT NULL, {provider.Escape("UnsignedAmount")} DECIMAL(18,4) UNSIGNED NOT NULL)");
    }

    private static async Task SetupProviderSpecificColumnDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownProviderSpecificColumnDiagnosticsAsync(connection, provider, kind);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, ProviderSpecificColumnDiagnosticsTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", ProviderSpecificColumnDiagnosticsTable)
                : provider.Escape(ProviderSpecificColumnDiagnosticsTable);
        var id = provider.Escape("Id");
        var providerColumnSql = kind switch
        {
            ProviderKind.SqlServer => $"{provider.Escape("Location")} geometry NULL",
            ProviderKind.Postgres => $"{provider.Escape("Address")} inet NULL",
            ProviderKind.MySql => $"{provider.Escape("Location")} POINT NULL",
            ProviderKind.Sqlite => $"{provider.Escape("Location")} GEOMETRY NULL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {providerColumnSql})");
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

    private static async Task SetupKeylessDependentRelationshipAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownKeylessDependentRelationshipAsync(connection, provider, kind);

        var parent = provider.Escape(KeylessDependentParentTable);
        var dependent = provider.Escape(KeylessDependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var payload = provider.Escape("Payload");
        var fkName = provider.Escape(KeylessDependentFkName);

        await ExecuteAsync(connection,
            $"CREATE TABLE {parent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {dependent} ({parentId} {IntType(kind)} NOT NULL, {payload} {TextType(kind, 80)} NOT NULL, " +
            $"CONSTRAINT {fkName} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static async Task SetupFeatureOwnedMetadataAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownFeatureOwnedMetadataAsync(connection, provider, kind);

        var table = provider.Escape(FeatureOwnedTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var nameLength = provider.Escape("NameLength");
        var checkName = provider.Escape(FeatureOwnedCheckName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE Latin1_General_BIN2 NOT NULL, {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {checkName} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE \"C\" NOT NULL, {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {checkName} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, {nameLength} INT GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {checkName} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} COLLATE NOCASE NOT NULL, {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {checkName} CHECK (length({name}) > 0))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, createSql);
    }

    private static async Task SetupTriggerDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownTriggerDiagnosticsAsync(connection, provider, kind);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, TriggerDiagnosticsTable)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", TriggerDiagnosticsTable)
                : provider.Escape(TriggerDiagnosticsTable);
        var id = provider.Escape("Id");
        var touched = provider.Escape("Touched");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {touched} {IntType(kind)} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, TriggerDiagnosticsTrigger)}} ON {{table}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                        UPDATE target
                        SET {{touched}} = 1
                        FROM {{table}} AS target
                        INNER JOIN inserted AS source ON source.{{id}} = target.{{id}};
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                var function = Qualified(provider, "public", TriggerDiagnosticsPostgresFunction);
                await ExecuteAsync(connection, $$"""
                    CREATE FUNCTION {{function}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        NEW."Touched" := 1;
                        RETURN NEW;
                    END
                    $$
                    """);
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(TriggerDiagnosticsTrigger)} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE TRIGGER {provider.Escape(TriggerDiagnosticsTrigger)} BEFORE INSERT ON {table} FOR EACH ROW SET NEW.{touched} = 1");
                break;
            case ProviderKind.Sqlite:
                await ExecuteAsync(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(TriggerDiagnosticsTrigger)}} AFTER INSERT ON {{table}}
                    BEGIN
                        UPDATE {{table}} SET {{touched}} = 1 WHERE {{id}} = NEW.{{id}};
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static async Task TeardownFeatureOwnedMetadataAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, FeatureOwnedTable, provider.Escape(FeatureOwnedTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownTriggerDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            switch (kind)
            {
                case ProviderKind.SqlServer:
                    await ExecuteAsync(connection,
                        $"IF OBJECT_ID(N'dbo.{TriggerDiagnosticsTrigger}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, "dbo." + TriggerDiagnosticsTable, SqlServerQualified(provider, TriggerDiagnosticsTable)));
                    break;
                case ProviderKind.Postgres:
                    var table = Qualified(provider, "public", TriggerDiagnosticsTable);
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)} ON {table}");
                    await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {Qualified(provider, "public", TriggerDiagnosticsPostgresFunction)}()");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, table));
                    break;
                case ProviderKind.MySql:
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, provider.Escape(TriggerDiagnosticsTable)));
                    break;
                case ProviderKind.Sqlite:
                    await ExecuteAsync(connection, $"DROP TRIGGER IF EXISTS {provider.Escape(TriggerDiagnosticsTrigger)}");
                    await ExecuteAsync(connection, DropTable(kind, TriggerDiagnosticsTable, provider.Escape(TriggerDiagnosticsTable)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
            }
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
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

    private static async Task SetupPostgresMaterializedViewAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresMaterializedViewAsync(connection, provider);

        var warning = provider.Escape(WarningTable);
        var matView = provider.Escape(PostgresMaterializedView);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {status} {TextType(ProviderKind.Postgres, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE MATERIALIZED VIEW {matView} AS SELECT {id}, {status} FROM {warning}");
    }

    private static async Task SetupSqlServerSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerSynonymAsync(connection, provider);

        var warning = SqlServerQualified(provider, WarningTable);
        var synonym = SqlServerQualified(provider, SqlServerWarningSynonym);
        var id = provider.Escape("Id");
        var status = provider.Escape("Status");

        await ExecuteAsync(connection,
            $"CREATE TABLE {warning} ({id} {IntType(ProviderKind.SqlServer)} NOT NULL PRIMARY KEY, {status} {TextType(ProviderKind.SqlServer, 32)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE SYNONYM {synonym} FOR {warning}");
    }

    private static async Task SetupSqlServerProcedureSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerProcedureSynonymAsync(connection, provider);

        var procedure = SqlServerQualified(provider, SqlServerSynonymProcedure);
        var synonym = SqlServerQualified(provider, SqlServerProcedureSynonym);
        await ExecuteAsync(connection, $"CREATE PROCEDURE {procedure} AS SELECT 1 AS {provider.Escape("Value")}");
        await ExecuteAsync(connection, $"CREATE SYNONYM {synonym} FOR {procedure}");
    }

    private static async Task SetupMySqlEventDiagnosticsAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownMySqlEventDiagnosticsAsync(connection, provider);

        var table = provider.Escape(MySqlEventDiagnosticsName);
        var id = provider.Escape("Id");
        await ExecuteAsync(connection, $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY)");
        await ExecuteAsync(connection,
            $"CREATE EVENT {provider.Escape(MySqlEventDiagnosticsName)} ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 DAY DO UPDATE {table} SET {id} = {id}");
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
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderDescendingIndex)} ON {table} ({name} DESC)");

        if (kind is ProviderKind.Postgres or ProviderKind.Sqlite)
        {
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} (lower({name}))");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderPartialExpressionIndex)} ON {table} (lower({name})) WHERE {activePredicate}");
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionDescendingIndex)} ON {table} (lower({name}) DESC)");
        }

        if (kind is ProviderKind.Postgres)
            await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionLiteralDescIndex)} ON {table} (strpos({name}, ' DESC'))");

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
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderFullPrefixIndex)} ON {table} ({name}(80))");
    }

    private static async Task SetupMySqlExpressionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.MySql, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.MySql)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.MySql, 80)} NOT NULL, {score} {IntType(ProviderKind.MySql)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIndex)} ON {table} ((LOWER({name})), {score})");
    }

    private static async Task SetupProviderSpecificAccessMethodIndexAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 160)} NOT NULL, {score} {IntType(kind)} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection, $"CREATE NONCLUSTERED COLUMNSTORE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({score})");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} USING hash (lower({name}))");
                break;
            case ProviderKind.MySql:
                await ExecuteAsync(connection, $"CREATE FULLTEXT INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name})");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported provider-specific access-method index test provider.");
        }
    }

    private static async Task SetupPostgresExpressionIncludedIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var score = provider.Escape("Score");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL, {score} {IntType(ProviderKind.Postgres)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderExpressionIncludedIndex)} ON {table} (lower({name})) INCLUDE ({score})");
    }

    private static async Task SetupPostgresProviderSpecificBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name} ASC NULLS FIRST)");
    }

    private static async Task SetupPostgresExpressionBtreeOptionIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} (lower({name}) text_pattern_ops)");
    }

    private static async Task SetupPostgresNullsNotDistinctUniqueIndexAsync(DbConnection connection, DatabaseProvider provider)
    {
        await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, ProviderIndexTable, provider.Escape(ProviderIndexTable)));

        var table = provider.Escape(ProviderIndexTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(ProviderKind.Postgres)} NOT NULL PRIMARY KEY, {name} {TextType(ProviderKind.Postgres, 160)} NULL)");
        await ExecuteAsync(connection, $"CREATE UNIQUE INDEX {provider.Escape(ProviderSpecificIndex)} ON {table} ({name}) NULLS NOT DISTINCT");
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

    private static string Qualified(DatabaseProvider provider, string schemaName, string tableName)
        => provider.Escape(schemaName) + "." + provider.Escape(tableName);

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

    private static async Task TeardownDatabaseNamesAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, DatabaseNamesOrderLineTable, provider.Escape(DatabaseNamesOrderLineTable)));
            await ExecuteAsync(connection, DropTable(kind, DatabaseNamesCustomerTable, provider.Escape(DatabaseNamesCustomerTable)));
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

    private static async Task TeardownSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorBookTable, provider.Escape(SurrogateAuthorBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SurrogateBookTable, provider.Escape(SurrogateBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SurrogateAuthorTable, provider.Escape(SurrogateAuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownGeneratedBridgeManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, GeneratedBridgeStudentCourseTable, provider.Escape(GeneratedBridgeStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, GeneratedBridgeCourseTable, provider.Escape(GeneratedBridgeCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, GeneratedBridgeStudentTable, provider.Escape(GeneratedBridgeStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, ReferentialChildTable, provider.Escape(ReferentialChildTable)));
            await ExecuteAsync(connection, DropTable(kind, ReferentialParentTable, provider.Escape(ReferentialParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownRestrictReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictChildTable, provider.Escape(ReferentialRestrictChildTable)));
            await ExecuteAsync(connection, DropTable(kind, ReferentialRestrictParentTable, provider.Escape(ReferentialRestrictParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSetDefaultReferentialActionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultChildTable, provider.Escape(ReferentialDefaultChildTable)));
            await ExecuteAsync(connection, DropTable(kind, ReferentialDefaultParentTable, provider.Escape(ReferentialDefaultParentTable)));
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

    private static async Task TeardownCompositePayloadJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentCourseTable, provider.Escape(CompositePayloadStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositePayloadCourseTable, provider.Escape(CompositePayloadCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositePayloadStudentTable, provider.Escape(CompositePayloadStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeSurrogateManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentCourseTable, provider.Escape(CompositeSurrogateStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateCourseTable, provider.Escape(CompositeSurrogateCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeSurrogateStudentTable, provider.Escape(CompositeSurrogateStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSharedTenantManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentCourseTable, provider.Escape(SharedTenantStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, SharedTenantCourseTable, provider.Escape(SharedTenantCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, SharedTenantStudentTable, provider.Escape(SharedTenantStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSharedAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorBookTable, provider.Escape(SharedAlternateAuthorBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SharedAlternateBookTable, provider.Escape(SharedAlternateBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SharedAlternateAuthorTable, provider.Escape(SharedAlternateAuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownAlternateKeyManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, AlternateAuthorBookTable, provider.Escape(AlternateAuthorBookTable)));
            await ExecuteAsync(connection, DropTable(kind, AlternateBookTable, provider.Escape(AlternateBookTable)));
            await ExecuteAsync(connection, DropTable(kind, AlternateAuthorTable, provider.Escape(AlternateAuthorTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSelfReferencingManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SelfPersonRelationshipTable, provider.Escape(SelfPersonRelationshipTable)));
            await ExecuteAsync(connection, DropTable(kind, SelfPersonTable, provider.Escape(SelfPersonTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownFilteredUniqueSurrogateJoinAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, FilteredStudentCourseTable, provider.Escape(FilteredStudentCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, FilteredCourseTable, provider.Escape(FilteredCourseTable)));
            await ExecuteAsync(connection, DropTable(kind, FilteredStudentTable, provider.Escape(FilteredStudentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSchemaQualifiedManyToManyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SchemaName + "." + SchemaAuthorBookTable, Qualified(provider, SchemaName, SchemaAuthorBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SchemaName + "." + SchemaBookTable, Qualified(provider, SchemaName, SchemaBookTable)));
            await ExecuteAsync(connection, DropTable(kind, SchemaName + "." + SchemaAuthorTable, Qualified(provider, SchemaName, SchemaAuthorTable)));
            if (kind == ProviderKind.Sqlite)
                await ExecuteAsync(connection, $"DETACH DATABASE {provider.Escape(SchemaName)}");
            else if (kind == ProviderKind.SqlServer)
                await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{SchemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(SchemaName)}");
            else
                await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(SchemaName)}");
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

    private static async Task TeardownCompositeRoleForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeRoleTransferTable, provider.Escape(CompositeRoleTransferTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeRoleAccountTable, provider.Escape(CompositeRoleAccountTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSingleColumnAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SingleAlternateChildTable, provider.Escape(SingleAlternateChildTable)));
            await ExecuteAsync(connection, DropTable(kind, SingleAlternateParentTable, provider.Escape(SingleAlternateParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownNullableAlternateKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, NullableAlternateChildTable, provider.Escape(NullableAlternateChildTable)));
            await ExecuteAsync(connection, DropTable(kind, NullableAlternateParentTable, provider.Escape(NullableAlternateParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, UniqueDependentProfileTable, provider.Escape(UniqueDependentProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, UniqueDependentParentTable, provider.Escape(UniqueDependentParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownOptionalUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, OptionalUniqueProfileTable, provider.Escape(OptionalUniqueProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, OptionalUniqueParentTable, provider.Escape(OptionalUniqueParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownRoleNamedUniqueDependentForeignKeysAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, RoleOneProfileTable, provider.Escape(RoleOneProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, RoleOneParentTable, provider.Escape(RoleOneParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSharedPrimaryKeyForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, SharedPkProfileTable, provider.Escape(SharedPkProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, SharedPkParentTable, provider.Escape(SharedPkParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, CompositeUniqueDependentProfileTable, provider.Escape(CompositeUniqueDependentProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, CompositeUniqueDependentParentTable, provider.Escape(CompositeUniqueDependentParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownOptionalCompositeUniqueDependentForeignKeyAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, OptionalCompositeUniqueDependentProfileTable, provider.Escape(OptionalCompositeUniqueDependentProfileTable)));
            await ExecuteAsync(connection, DropTable(kind, OptionalCompositeUniqueDependentParentTable, provider.Escape(OptionalCompositeUniqueDependentParentTable)));
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

    private static async Task TeardownStringBinaryFacetsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, StringBinaryFacetTable, provider.Escape(StringBinaryFacetTable)));
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

    private static async Task TeardownSqlServerNonQueryRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{RoutineNonQueryName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineNonQueryName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerTableValuedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{RoutineTableValuedParameterName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(RoutineTableValuedParameterName)}");
            await ExecuteAsync(connection,
                $"IF TYPE_ID(N'dbo.{RoutineTableTypeName}') IS NOT NULL DROP TYPE {provider.Escape("dbo")}.{provider.Escape(RoutineTableTypeName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerFunctionRoutinesAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerTableValuedFunctionName}', N'IF') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerTableValuedFunctionName)}");
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerScalarFunctionName}', N'FN') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(SqlServerScalarFunctionName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSequenceAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var qualifiedName = kind == ProviderKind.SqlServer
                ? provider.Escape("dbo") + "." + provider.Escape(SequenceName)
                : provider.Escape("public") + "." + provider.Escape(SequenceName);
            var sql = kind switch
            {
                ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{SequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {qualifiedName}",
                ProviderKind.Postgres => $"DROP SEQUENCE IF EXISTS {qualifiedName}",
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffolding live test only targets SQL Server and PostgreSQL.")
            };
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

    private static async Task TeardownPostgresTypedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresTypedRoutineName)}(integer[], uuid)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresOverloadedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var routine = provider.Escape("public") + "." + provider.Escape(PostgresOverloadedRoutineName);
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {routine}(integer)");
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {routine}(text)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresQuotedParameterRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresQuotedParameterRoutineName)}(integer, text)");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlUnsignedRoutineAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(MySqlUnsignedRoutineName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresDomainColumnAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresDomainTable, Qualified(provider, "public", PostgresDomainTable)));
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", PostgresDomainStatusName)}");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", PostgresDomainScoreArrayName)}");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", PostgresDomainScoreName)}");
            await ExecuteAsync(connection, $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", PostgresDomainName)}");
            await ExecuteAsync(connection, $"DROP TYPE IF EXISTS {Qualified(provider, "public", PostgresEnumName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerAliasTypeColumnAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, "dbo." + SqlServerAliasTypeTable, Qualified(provider, "dbo", SqlServerAliasTypeTable)));
            await ExecuteAsync(connection, $"IF TYPE_ID(N'dbo.{SqlServerAliasBinaryTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", SqlServerAliasBinaryTypeName)}");
            await ExecuteAsync(connection, $"IF TYPE_ID(N'dbo.{SqlServerAliasDecimalTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", SqlServerAliasDecimalTypeName)}");
            await ExecuteAsync(connection, $"IF TYPE_ID(N'dbo.{SqlServerAliasTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", SqlServerAliasTypeName)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownPostgresTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresTypedColumnTable, Qualified(provider, "public", PostgresTypedColumnTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.MySql, MySqlTypedColumnTable, provider.Escape(MySqlTypedColumnTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlUnsafeSetColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.MySql, MySqlUnsafeSetColumnTable, provider.Escape(MySqlUnsafeSetColumnTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlUnsignedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.MySql, MySqlUnsignedColumnTable, provider.Escape(MySqlUnsignedColumnTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownProviderSpecificColumnDiagnosticsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var table = kind == ProviderKind.SqlServer
                ? SqlServerQualified(provider, ProviderSpecificColumnDiagnosticsTable)
                : kind == ProviderKind.Postgres
                    ? Qualified(provider, "public", ProviderSpecificColumnDiagnosticsTable)
                    : provider.Escape(ProviderSpecificColumnDiagnosticsTable);
            await ExecuteAsync(connection, DropTable(kind, ProviderSpecificColumnDiagnosticsTable, table));
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

    private static async Task TeardownKeylessDependentRelationshipAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, KeylessDependentTable, provider.Escape(KeylessDependentTable)));
            await ExecuteAsync(connection, DropTable(kind, KeylessDependentParentTable, provider.Escape(KeylessDependentParentTable)));
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

    private static async Task TeardownPostgresMaterializedViewAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, $"DROP MATERIALIZED VIEW IF EXISTS {provider.Escape(PostgresMaterializedView)}");
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, WarningTable, provider.Escape(WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerWarningSynonym}', N'SN') IS NOT NULL DROP SYNONYM {SqlServerQualified(provider, SqlServerWarningSynonym)}");
            await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, WarningTable, SqlServerQualified(provider, WarningTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownSqlServerProcedureSynonymAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerProcedureSynonym}', N'SN') IS NOT NULL DROP SYNONYM {SqlServerQualified(provider, SqlServerProcedureSynonym)}");
            await ExecuteAsync(connection,
                $"IF OBJECT_ID(N'dbo.{SqlServerSynonymProcedure}', N'P') IS NOT NULL DROP PROCEDURE {SqlServerQualified(provider, SqlServerSynonymProcedure)}");
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task TeardownMySqlEventDiagnosticsAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            await ExecuteAsync(connection, $"DROP EVENT IF EXISTS {provider.Escape(MySqlEventDiagnosticsName)}");
            await ExecuteAsync(connection, DropTable(ProviderKind.MySql, MySqlEventDiagnosticsName, provider.Escape(MySqlEventDiagnosticsName)));
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

    private static string StripDefaultSchemaArguments(string generatedCode)
        => generatedCode
            .Replace(", schema: \"dbo\"", string.Empty, StringComparison.Ordinal)
            .Replace(", schema: \"public\"", string.Empty, StringComparison.Ordinal);

    private static bool LastTableNameEquals(string? tableName, string expected)
        => string.Equals(
            tableName?.Split('.').LastOrDefault(),
            expected,
            StringComparison.OrdinalIgnoreCase);

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

    private static string IdentityPrimaryKeyColumn(ProviderKind kind, string escapedColumnName) => kind switch
    {
        ProviderKind.SqlServer => $"{escapedColumnName} INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
        ProviderKind.Postgres => $"{escapedColumnName} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
        ProviderKind.MySql => $"{escapedColumnName} INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        _ => $"{escapedColumnName} INTEGER PRIMARY KEY AUTOINCREMENT"
    };

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

    private static string IdentityColumnTableSql(ProviderKind kind, DatabaseProvider provider)
    {
        var table = provider.Escape(DynamicIdentityTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        return kind switch
        {
            ProviderKind.SqlServer => $"CREATE TABLE {table} ({id} INT IDENTITY(1,1) NOT NULL PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)",
            ProviderKind.Postgres => $"CREATE TABLE {table} ({id} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)",
            ProviderKind.MySql => $"CREATE TABLE {table} ({id} INT NOT NULL AUTO_INCREMENT PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)",
            _ => $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {name} {TextType(kind, 40)} NOT NULL)"
        };
    }

    private static string DynamicCompositeKeyTableSql(ProviderKind kind, DatabaseProvider provider)
    {
        var table = provider.Escape(DynamicCompositeKeyTable);
        var tenantId = provider.Escape("TenantId");
        var localId = provider.Escape("LocalId");
        var payload = provider.Escape("Payload");

        return $"CREATE TABLE {table} ({tenantId} {IntType(kind)} NOT NULL, {localId} {IntType(kind)} NOT NULL, {payload} {TextType(kind, 40)} NOT NULL, PRIMARY KEY ({localId}, {tenantId}))";
    }

    private sealed class LiveRoutineOutputRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
