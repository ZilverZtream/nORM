#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupProviderOwnedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string authorFkName,
        string bookFkName,
        string triggerName,
        string functionName)
    {
        CleanupProviderOwnedBridgeManyToMany(connection, provider, kind, authorTable, bookTable, authorBookTable, triggerName, functionName);

        var author = kind == ProviderKind.SqlServer ? SqlServerQualified(provider, authorTable) : provider.Escape(authorTable);
        var book = kind == ProviderKind.SqlServer ? SqlServerQualified(provider, bookTable) : provider.Escape(bookTable);
        var authorBook = kind == ProviderKind.SqlServer ? SqlServerQualified(provider, authorBookTable) : provider.Escape(authorBookTable);
        var id = provider.Escape("Id");
        var authorId = provider.Escape("AuthorId");
        var bookId = provider.Escape("BookId");
        var name = provider.Escape("Name");
        var title = provider.Escape("Title");
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {author} ({id} {intType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {book} ({id} {intType} NOT NULL PRIMARY KEY, {title} {text} NOT NULL)",
            $"CREATE TABLE {authorBook} ({authorId} {intType} NOT NULL, {bookId} {intType} NOT NULL, PRIMARY KEY ({authorId}, {bookId}), " +
            $"CONSTRAINT {provider.Escape(authorFkName)} FOREIGN KEY ({authorId}) REFERENCES {author} ({id}), " +
            $"CONSTRAINT {provider.Escape(bookFkName)} FOREIGN KEY ({bookId}) REFERENCES {book} ({id}))");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, triggerName)}} ON {{authorBook}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                Execute(connection, $$"""
                    CREATE FUNCTION {{provider.Escape(functionName)}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        RETURN NEW;
                    END
                    $$
                    """);
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {authorBook} FOR EACH ROW EXECUTE FUNCTION {provider.Escape(functionName)}()");
                break;
            case ProviderKind.MySql:
                // No-op self-assignment: MySqlConnector client-side parses @name tokens as
                // command parameters unless AllowUserVariables=true, so the trigger body
                // must avoid session user variables to run on any connection string.
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {authorBook} FOR EACH ROW SET NEW.{authorId} = NEW.{authorId}");
                break;
            case ProviderKind.Sqlite:
                Execute(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(triggerName)}} AFTER INSERT ON {{authorBook}}
                    BEGIN
                        SELECT 1;
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupProviderOwnedBridgeManyToMany(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string authorTable,
        string bookTable,
        string authorBookTable,
        string triggerName,
        string functionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{triggerName}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, triggerName)}",
                    DropTable(kind, "dbo." + authorBookTable, SqlServerQualified(provider, authorBookTable)),
                    DropTable(kind, "dbo." + bookTable, SqlServerQualified(provider, bookTable)),
                    DropTable(kind, "dbo." + authorTable, SqlServerQualified(provider, authorTable)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)} ON {provider.Escape(authorBookTable)}",
                    $"DROP FUNCTION IF EXISTS {provider.Escape(functionName)}()",
                    DropTable(kind, authorBookTable, provider.Escape(authorBookTable)),
                    DropTable(kind, bookTable, provider.Escape(bookTable)),
                    DropTable(kind, authorTable, provider.Escape(authorTable)));
                break;
            case ProviderKind.MySql:
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)}",
                    DropTable(kind, authorBookTable, provider.Escape(authorBookTable)),
                    DropTable(kind, bookTable, provider.Escape(bookTable)),
                    DropTable(kind, authorTable, provider.Escape(authorTable)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }
}
