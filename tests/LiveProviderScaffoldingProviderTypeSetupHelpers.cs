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
    private static async Task SetupPostgresTypedColumnTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownPostgresTypedColumnTableAsync(connection, provider);

        var table = Qualified(provider, "public", PostgresTypedColumnTable);
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL, {provider.Escape("Ratings")} numeric(10,2)[] NULL, {provider.Escape("Aliases")} varchar(32)[] NULL)");
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
}
