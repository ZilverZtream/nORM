#nullable enable
using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
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

    private static async Task TeardownTemporalStoreTypeTableAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        try
        {
            var rawName = DefaultSchemaTableFilter(kind, TemporalStoreTypeTable);
            var escapedName = kind switch
            {
                ProviderKind.SqlServer => SqlServerQualified(provider, TemporalStoreTypeTable),
                ProviderKind.Postgres => Qualified(provider, "public", TemporalStoreTypeTable),
                _ => provider.Escape(TemporalStoreTypeTable)
            };
            await ExecuteAsync(connection, DropTable(kind, rawName, escapedName));
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
                $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(PostgresTypedRoutineName)}(integer[], numeric[], character varying[], uuid)");
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

}
