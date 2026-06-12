#nullable enable

using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupUseDatabaseNames(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string customerTable,
        string orderLineTable)
    {
        CleanupUseDatabaseNames(connection, provider, customerTable, orderLineTable);

        var customer = provider.Escape(customerTable);
        var orderLine = provider.Escape(orderLineTable);
        var customerId = provider.Escape("customer_id");
        var orderLineId = provider.Escape("order_line_id");
        var displayName = provider.Escape("display_name");
        var sku = provider.Escape("SKU");
        var classColumn = provider.Escape("class");
        var hasSpace = provider.Escape("has space");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {customer} ({customerId} int NOT NULL PRIMARY KEY, {displayName} {text} NOT NULL)",
            $"CREATE TABLE {orderLine} ({orderLineId} int NOT NULL PRIMARY KEY, {customerId} int NOT NULL, {sku} {text} NOT NULL, {classColumn} {text} NULL, {hasSpace} {text} NULL, " +
            $"FOREIGN KEY ({customerId}) REFERENCES {customer} ({customerId}))");
    }

    private static void CleanupUseDatabaseNames(
        DbConnection connection,
        DatabaseProvider provider,
        string customerTable,
        string orderLineTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(orderLineTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(customerTable)}");
    }

    private static void SetupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        CleanupIdentifierCollisionScaffold(connection, provider, kind, invalidIdentifierTable, collisionDashTable, collisionUnderscoreTable);

        var invalid = provider.Escape(invalidIdentifierTable);
        var dash = provider.Escape(collisionDashTable);
        var underscore = provider.Escape(collisionUnderscoreTable);
        var id = provider.Escape("Id");
        var invalidLeadingDigit = provider.Escape("1st-name");
        var hasSpace = provider.Escape("has space");
        var firstNameDash = provider.Escape("first-name");
        var firstNameUnderscore = provider.Escape("first_name");
        var toString = provider.Escape("ToString");
        var equals = provider.Escape("Equals");
        var value = provider.Escape("Value");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {invalid} ({id} {idType} NOT NULL PRIMARY KEY, {invalidLeadingDigit} {text} NOT NULL, {hasSpace} {idType} NULL, {firstNameDash} {text} NOT NULL, {firstNameUnderscore} {text} NOT NULL, {toString} {text} NOT NULL, {equals} {text} NOT NULL)",
            $"CREATE TABLE {dash} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)",
            $"CREATE TABLE {underscore} ({id} {idType} NOT NULL PRIMARY KEY, {value} {text} NOT NULL)");
    }

    private static void CleanupIdentifierCollisionScaffold(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string invalidIdentifierTable,
        string collisionDashTable,
        string collisionUnderscoreTable)
    {
        Execute(connection,
            DropTable(kind, collisionUnderscoreTable, provider.Escape(collisionUnderscoreTable)),
            DropTable(kind, collisionDashTable, provider.Escape(collisionDashTable)),
            DropTable(kind, invalidIdentifierTable, provider.Escape(invalidIdentifierTable)));
    }
}
