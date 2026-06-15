#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static void SetupRequiredAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupRequiredAlternateKeyRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {code} {text40} NOT NULL, {name} {text80} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({code})",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentCode} {text40} NOT NULL, {notes} {text80} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static void CleanupRequiredAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupNullableAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupNullableAlternateKeyRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var parentCode = provider.Escape("ParentCode");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} int NOT NULL PRIMARY KEY, {code} {text} NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({code})",
            $"CREATE TABLE {child} ({id} int NOT NULL PRIMARY KEY, {parentCode} {text} NOT NULL, {notes} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentCode}) REFERENCES {parent} ({code}))");
    }

    private static void CleanupNullableAlternateKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} int NOT NULL PRIMARY KEY, {parentId} int NULL, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET NULL ON UPDATE CASCADE)");
    }

    private static void SetupRestrictReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE RESTRICT ON UPDATE CASCADE)");
    }

    private static void SetupSetDefaultReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string fkName,
        string defaultName)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind == ProviderKind.SqlServer ? "nvarchar(80)" : "text";
        var defaultClause = kind == ProviderKind.SqlServer
            ? $"CONSTRAINT {provider.Escape(defaultName)} DEFAULT (0)"
            : "DEFAULT 0";

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL {defaultClause}, {name} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)");
    }

    private static void CleanupReferentialActionRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string childTable,
        string parentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static string ExpectedCascadeForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string constraintName)
        => $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, \"{constraintName}\", false);";

    private static string ExpectedReferentialForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string onDelete,
        string onUpdate,
        string constraintName)
        => $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate}, \"{constraintName}\");";

    private static void SetupUnnamedForeignKeyRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable)
    {
        CleanupReferentialActionRelationship(connection, provider, childTable, parentTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {name} {text} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static void SetupCompositePrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string parentPkName,
        string fkName)
    {
        CleanupCompositePrimaryKeyForeignKey(connection, provider, parentTable, childTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var parentPk = provider.Escape(parentPkName);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");
        var notes = provider.Escape("Notes");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({tenantId} {idType} NOT NULL, {orderNo} {idType} NOT NULL, {name} {text} NOT NULL, CONSTRAINT {parentPk} PRIMARY KEY ({tenantId}, {orderNo}))",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {orderNo} {idType} NOT NULL, {notes} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {orderNo}) REFERENCES {parent} ({tenantId}, {orderNo}))");
    }

    private static void CleanupCompositePrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeForeignKeyToUniqueIndex(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string childTable,
        string indexName,
        string fkName)
    {
        CleanupCompositeForeignKeyToUniqueIndex(connection, provider, parentTable, childTable);

        var parent = provider.Escape(parentTable);
        var child = provider.Escape(childTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var externalNo = provider.Escape("ExternalNo");
        var name = provider.Escape("Name");
        var eventName = provider.Escape("EventName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text40 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(40)",
            ProviderKind.MySql => "varchar(40)",
            _ => "text"
        };
        var text80 = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {externalNo} {text40} NOT NULL, {name} {text80} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {parent} ({tenantId}, {externalNo})",
            $"CREATE TABLE {child} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {externalNo} {text40} NOT NULL, {eventName} {text80} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {externalNo}) REFERENCES {parent} ({tenantId}, {externalNo}))");
    }

    private static void CleanupCompositeForeignKeyToUniqueIndex(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string childTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(childTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeRoleForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string accountTable,
        string transferTable,
        string accountIndex,
        string primaryFkName,
        string backupFkName)
    {
        CleanupCompositeRoleForeignKeys(connection, provider, accountTable, transferTable);

        var account = provider.Escape(accountTable);
        var transfer = provider.Escape(transferTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var primaryAccountNo = provider.Escape("PrimaryAccountNo");
        var backupAccountNo = provider.Escape("BackupAccountNo");
        var name = provider.Escape("Name");
        var amount = provider.Escape("Amount");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {account} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {accountNo} int NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(accountIndex)} ON {account} ({tenantId}, {accountNo})",
            $"CREATE TABLE {transfer} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {primaryAccountNo} int NOT NULL, {backupAccountNo} int NOT NULL, {amount} int NOT NULL, " +
            $"CONSTRAINT {provider.Escape(primaryFkName)} FOREIGN KEY ({tenantId}, {primaryAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}), " +
            $"CONSTRAINT {provider.Escape(backupFkName)} FOREIGN KEY ({tenantId}, {backupAccountNo}) REFERENCES {account} ({tenantId}, {accountNo}))");
    }

    private static void CleanupCompositeRoleForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string accountTable,
        string transferTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(transferTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(accountTable)}");
    }

    private static void SetupKeylessDependentRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string dependentTable,
        string fkName)
    {
        CleanupKeylessDependentRelationship(connection, provider, parentTable, dependentTable);

        var parent = provider.Escape(parentTable);
        var dependent = provider.Escape(dependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var payload = provider.Escape("Payload");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY)",
            $"CREATE TABLE {dependent} ({parentId} {idType} NOT NULL, {payload} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({parentId}) REFERENCES {parent} ({id}))");
    }

    private static void CleanupKeylessDependentRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string dependentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(dependentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

}
