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
    private const string NullableBridgeStudentTable = "ScaffoldLiveNullableBridgeStudent";
    private const string NullableBridgeCourseTable = "ScaffoldLiveNullableBridgeCourse";
    private const string NullableBridgeStudentCourseTable = "ScaffoldLiveNullableBridgeStudentCourse";
    private const string NullableBridgeStudentCourseStudentFkName = "FK_ScaffoldLiveNullableBridge_Student";
    private const string NullableBridgeStudentCourseCourseFkName = "FK_ScaffoldLiveNullableBridge_Course";
    private const string NullableBridgeStudentCourseUniqueIndexName = "UX_ScaffoldLiveNullableBridge_Pair";
    private const string ProviderOwnedBridgeAuthorTable = "ScaffoldLiveOwnedBridgeAuthor";
    private const string ProviderOwnedBridgeBookTable = "ScaffoldLiveOwnedBridgeBook";
    private const string ProviderOwnedBridgeAuthorBookTable = "ScaffoldLiveOwnedBridgeAuthorBook";
    private const string ProviderOwnedBridgeAuthorBookAuthorFkName = "FK_ScaffoldLiveOwnedBridge_Author";
    private const string ProviderOwnedBridgeAuthorBookBookFkName = "FK_ScaffoldLiveOwnedBridge_Book";
    private const string ProviderOwnedBridgeTrigger = "TR_ScaffoldLiveOwnedBridge_Audit";
    private const string ProviderOwnedBridgePostgresFunction = "fn_scaffold_live_owned_bridge_audit";
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
    private const string FeatureOwnedDefaultName = "DF_ScaffoldLiveFeatureOwned_Status";
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
    private const string MySqlOnUpdateTimestampTable = "ScaffoldLiveOnUpdateTimestamp";
    private const string DynamicComputedTable = "ScaffoldLiveDynamicComputed";
    private const string DynamicIdentityTable = "ScaffoldLiveDynamicIdentity";
    private const string DynamicCompositeKeyTable = "ScaffoldLiveDynamicCompositeKey";
    private const string DecimalPrecisionTable = "ScaffoldLiveDecimalPrecision";
    private const string StringBinaryFacetTable = "ScaffoldLiveStringBinaryFacets";
    private const string TemporalStoreTypeTable = "ScaffoldLiveTemporalStoreTypes";
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

    private static string SqlServerQualified(DatabaseProvider provider, string tableName)
        => provider.Escape("dbo") + "." + provider.Escape(tableName);

    private static string Qualified(DatabaseProvider provider, string schemaName, string tableName)
        => provider.Escape(schemaName) + "." + provider.Escape(tableName);

    private static string DefaultSchemaTableFilter(ProviderKind kind, string tableName) => kind switch
    {
        ProviderKind.SqlServer => "dbo." + tableName,
        ProviderKind.Postgres => "public." + tableName,
        _ => tableName
    };

    private static string DefaultScaffoldEntityName(string tableName)
        => ScaffoldNameHelper.Singularize(ScaffoldNameHelper.ToScaffoldClrName(tableName, useDatabaseNames: false));

    private static string DefaultScaffoldEntityPath(string directory, string tableName)
        => Path.Combine(directory, DefaultScaffoldEntityName(tableName) + ".cs");

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

    private sealed class LiveRoutineOutputRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
