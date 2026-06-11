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
