#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string DatabaseCommentsTable = "ScaffoldLiveDatabaseComment";
    private const string DatabaseCommentsTableComment = "Table <summary> & description";
    private const string DatabaseCommentsColumnComment = "Name <tag> & details";

    private static async Task SetupDatabaseCommentsAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownDatabaseCommentsAsync(connection, provider, kind);

        var table = DatabaseCommentsEscapedTable(provider, kind);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");

        switch (kind)
        {
            case ProviderKind.MySql:
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL COMMENT {SqlLiteral(DatabaseCommentsColumnComment)}) COMMENT={SqlLiteral(DatabaseCommentsTableComment)}");
                break;
            case ProviderKind.SqlServer:
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
                await ExecuteAsync(connection,
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(DatabaseCommentsTableComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(DatabaseCommentsTable));
                await ExecuteAsync(connection,
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(DatabaseCommentsColumnComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(DatabaseCommentsTable) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
                await ExecuteAsync(connection, $"COMMENT ON TABLE {table} IS {SqlLiteral(DatabaseCommentsTableComment)}");
                await ExecuteAsync(connection, $"COMMENT ON COLUMN {table}.{name} IS {SqlLiteral(DatabaseCommentsColumnComment)}");
                break;
            case ProviderKind.Sqlite:
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static async Task TeardownDatabaseCommentsAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, DefaultSchemaTableFilter(kind, DatabaseCommentsTable), DatabaseCommentsEscapedTable(provider, kind)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static string DatabaseCommentsEscapedTable(DatabaseProvider provider, ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => SqlServerQualified(provider, DatabaseCommentsTable),
        ProviderKind.Postgres => Qualified(provider, "public", DatabaseCommentsTable),
        _ => provider.Escape(DatabaseCommentsTable)
    };

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string SqlServerLiteral(string value) => "N" + SqlLiteral(value);
}
