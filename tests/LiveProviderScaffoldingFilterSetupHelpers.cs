#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string SchemaFilterSchemaName = "scaffold_live_filter_schema";
    private const string SchemaFilterSchemaTable = "ScaffoldLiveFilterSchemaTable";
    private const string SchemaFilterExplicitTable = "ScaffoldLiveFilterExplicit";
    private const string SchemaFilterSkippedTable = "ScaffoldLiveFilterSkipped";
    private const string MissingSchemaFilterTable = "ScaffoldLiveMissingSchemaFilter";

    private static async Task SetupSchemaAndTableFilterUnionAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string? scratchDatabase)
    {
        await TeardownSchemaAndTableFilterUnionAsync(connection, provider, kind, scratchDatabase, originalDatabase: null);

        if (kind == ProviderKind.MySql)
        {
            if (string.IsNullOrWhiteSpace(scratchDatabase))
                throw new ArgumentException("MySQL runtime schema-filter tests require a scratch database.", nameof(scratchDatabase));

            await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
            await ExecuteAsync(connection, $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
            connection.ChangeDatabase(scratchDatabase);
        }
        else if (kind == ProviderKind.Sqlite)
        {
            try { await ExecuteAsync(connection, $"DETACH DATABASE {provider.Escape(SchemaFilterSchemaName)}"); } catch { }
            await ExecuteAsync(connection, $"ATTACH DATABASE ':memory:' AS {provider.Escape(SchemaFilterSchemaName)}");
        }
        else
        {
            await CreateSchemaFilterSchemaAsync(connection, provider, kind);
        }

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var schemaTable = SchemaFilterEscapedSchemaTable(provider, kind);

        await ExecuteAsync(connection,
            $"CREATE TABLE {schemaTable} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {provider.Escape(SchemaFilterExplicitTable)} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");

        if (kind != ProviderKind.MySql)
        {
            await ExecuteAsync(connection,
                $"CREATE TABLE {provider.Escape(SchemaFilterSkippedTable)} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        }
    }

    private static async Task TeardownSchemaAndTableFilterUnionAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string? scratchDatabase,
        string? originalDatabase)
    {
        try
        {
            if (kind == ProviderKind.MySql)
            {
                if (!string.IsNullOrWhiteSpace(originalDatabase))
                    connection.ChangeDatabase(originalDatabase);
                if (!string.IsNullOrWhiteSpace(scratchDatabase))
                    await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
                return;
            }

            await ExecuteAsync(connection, DropTable(kind, SchemaFilterSkippedTable, provider.Escape(SchemaFilterSkippedTable)));
            await ExecuteAsync(connection, DropTable(kind, SchemaFilterExplicitTable, provider.Escape(SchemaFilterExplicitTable)));
            await ExecuteAsync(connection, DropTable(kind, SchemaFilterSchemaName + "." + SchemaFilterSchemaTable, SchemaFilterEscapedSchemaTable(provider, kind)));

            if (kind == ProviderKind.Sqlite)
                await ExecuteAsync(connection, $"DETACH DATABASE {provider.Escape(SchemaFilterSchemaName)}");
            else
                await DropSchemaFilterSchemaAsync(connection, provider, kind);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static async Task SetupMissingSchemaFilterTableAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownMissingSchemaFilterTableAsync(connection, provider, kind);

        var table = MissingSchemaFilterEscapedTable(provider, kind);
        var id = provider.Escape("Id");
        await ExecuteAsync(connection, $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY)");
    }

    private static async Task TeardownMissingSchemaFilterTableAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, DefaultSchemaTableFilter(kind, MissingSchemaFilterTable), MissingSchemaFilterEscapedTable(provider, kind)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string SchemaFilterRequest(ProviderKind kind, string? scratchDatabase)
        => kind == ProviderKind.MySql ? scratchDatabase! : SchemaFilterSchemaName;

    private static string SchemaFilterEscapedSchemaTable(DatabaseProvider provider, ProviderKind kind) => kind switch
    {
        ProviderKind.MySql => provider.Escape(SchemaFilterSchemaTable),
        _ => Qualified(provider, SchemaFilterSchemaName, SchemaFilterSchemaTable)
    };

    private static string MissingSchemaFilterEscapedTable(DatabaseProvider provider, ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => SqlServerQualified(provider, MissingSchemaFilterTable),
        ProviderKind.Postgres => Qualified(provider, "public", MissingSchemaFilterTable),
        _ => provider.Escape(MissingSchemaFilterTable)
    };

    private static async Task CreateSchemaFilterSchemaAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        if (kind == ProviderKind.SqlServer)
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{SchemaFilterSchemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(SchemaFilterSchemaName)}')");
        else
            await ExecuteAsync(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(SchemaFilterSchemaName)}");
    }

    private static async Task DropSchemaFilterSchemaAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        if (kind == ProviderKind.SqlServer)
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{SchemaFilterSchemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(SchemaFilterSchemaName)}");
        else
            await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(SchemaFilterSchemaName)}");
    }
}
