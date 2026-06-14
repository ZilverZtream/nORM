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

public sealed partial class LiveProviderScaffoldingParityTests
{
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
}
