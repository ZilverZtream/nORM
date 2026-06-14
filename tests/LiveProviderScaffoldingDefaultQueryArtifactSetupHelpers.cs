#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string DefaultQueryArtifactSchemaName = "scaffold_live_default_query";
    private const string DefaultQueryArtifactBaseTable = "ScaffoldLiveDefaultQueryBase";
    private const string DefaultQueryArtifactView = "ScaffoldLiveDefaultQueryReport";

    private static async Task SetupDefaultQueryArtifactDiscoveryAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string? scratchDatabase)
    {
        await TeardownDefaultQueryArtifactDiscoveryAsync(connection, provider, kind, scratchDatabase, originalDatabase: null);

        if (kind == ProviderKind.MySql)
        {
            if (string.IsNullOrWhiteSpace(scratchDatabase))
                throw new ArgumentException("MySQL default query-artifact tests require a scratch database.", nameof(scratchDatabase));

            await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {provider.Escape(scratchDatabase)}");
            await ExecuteAsync(connection, $"CREATE DATABASE {provider.Escape(scratchDatabase)}");
            connection.ChangeDatabase(scratchDatabase);
        }
        else if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            await CreateDefaultQueryArtifactSchemaAsync(connection, provider, kind);
        }

        var table = DefaultQueryArtifactEscapedBaseTable(provider, kind);
        var view = DefaultQueryArtifactEscapedView(provider, kind);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection, $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");

        if (kind == ProviderKind.SqlServer)
        {
            await ExecuteAsync(connection,
                "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(DefaultQueryArtifactSchemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(DefaultQueryArtifactView));
            await ExecuteAsync(connection,
                "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(DefaultQueryArtifactSchemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(DefaultQueryArtifactView) + ", @level2type=N'COLUMN', @level2name=N'Name'");
        }
        else if (kind == ProviderKind.Postgres)
        {
            await ExecuteAsync(connection, $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}");
            await ExecuteAsync(connection, $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
        }
    }

    private static async Task TeardownDefaultQueryArtifactDiscoveryAsync(
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

            await ExecuteAsync(connection, DropView(kind, DefaultQueryArtifactRawView(kind), DefaultQueryArtifactEscapedView(provider, kind)));
            await ExecuteAsync(connection, DropTable(kind, DefaultQueryArtifactRawBaseTable(kind), DefaultQueryArtifactEscapedBaseTable(provider, kind)));

            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
                await DropDefaultQueryArtifactSchemaAsync(connection, provider, kind);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string DefaultQueryArtifactEscapedBaseTable(DatabaseProvider provider, ProviderKind kind)
        => kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, DefaultQueryArtifactSchemaName, DefaultQueryArtifactBaseTable)
            : provider.Escape(DefaultQueryArtifactBaseTable);

    private static string DefaultQueryArtifactEscapedView(DatabaseProvider provider, ProviderKind kind)
        => kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, DefaultQueryArtifactSchemaName, DefaultQueryArtifactView)
            : provider.Escape(DefaultQueryArtifactView);

    private static string DefaultQueryArtifactRawBaseTable(ProviderKind kind)
        => kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? DefaultQueryArtifactSchemaName + "." + DefaultQueryArtifactBaseTable
            : DefaultQueryArtifactBaseTable;

    private static string DefaultQueryArtifactRawView(ProviderKind kind)
        => kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? DefaultQueryArtifactSchemaName + "." + DefaultQueryArtifactView
            : DefaultQueryArtifactView;

    private static async Task CreateDefaultQueryArtifactSchemaAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        if (kind == ProviderKind.SqlServer)
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{DefaultQueryArtifactSchemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(DefaultQueryArtifactSchemaName)}')");
        else
            await ExecuteAsync(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(DefaultQueryArtifactSchemaName)}");
    }

    private static async Task DropDefaultQueryArtifactSchemaAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        if (kind == ProviderKind.SqlServer)
            await ExecuteAsync(connection, $"IF SCHEMA_ID(N'{DefaultQueryArtifactSchemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(DefaultQueryArtifactSchemaName)}");
        else
            await ExecuteAsync(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(DefaultQueryArtifactSchemaName)}");
    }
}
