#nullable enable

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
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

    private static void SetupRoleNamedOneToOneForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string primaryFkName,
        string backupFkName,
        string primaryIndexName,
        string backupIndexName)
    {
        CleanupRoleNamedOneToOneForeignKeys(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var primaryAccountId = provider.Escape("PrimaryAccountId");
        var backupAccountId = provider.Escape("BackupAccountId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {primaryAccountId} {idType} NOT NULL, {backupAccountId} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(primaryFkName)} FOREIGN KEY ({primaryAccountId}) REFERENCES {parent} ({id}), " +
            $"CONSTRAINT {provider.Escape(backupFkName)} FOREIGN KEY ({backupAccountId}) REFERENCES {parent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(primaryIndexName)} ON {profile} ({primaryAccountId})",
            $"CREATE UNIQUE INDEX {provider.Escape(backupIndexName)} ON {profile} ({backupAccountId})");
    }

    private static void CleanupRoleNamedOneToOneForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupRequiredAndOptionalUniqueDependentForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string requiredParentTable,
        string requiredProfileTable,
        string optionalParentTable,
        string optionalProfileTable,
        string requiredFkName,
        string optionalFkName,
        string requiredIndexName,
        string optionalIndexName)
    {
        CleanupRequiredAndOptionalUniqueDependentForeignKeys(
            connection,
            provider,
            requiredParentTable,
            requiredProfileTable,
            optionalParentTable,
            optionalProfileTable);

        var requiredParent = provider.Escape(requiredParentTable);
        var requiredProfile = provider.Escape(requiredProfileTable);
        var optionalParent = provider.Escape(optionalParentTable);
        var optionalProfile = provider.Escape(optionalProfileTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {requiredParent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {requiredProfile} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(requiredFkName)} FOREIGN KEY ({parentId}) REFERENCES {requiredParent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(requiredIndexName)} ON {requiredProfile} ({parentId})",
            $"CREATE TABLE {optionalParent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {optionalProfile} ({id} {idType} NOT NULL PRIMARY KEY, {parentId} {idType} NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(optionalFkName)} FOREIGN KEY ({parentId}) REFERENCES {optionalParent} ({id}))",
            $"CREATE UNIQUE INDEX {provider.Escape(optionalIndexName)} ON {optionalProfile} ({parentId})");
    }

    private static void CleanupRequiredAndOptionalUniqueDependentForeignKeys(
        DbConnection connection,
        DatabaseProvider provider,
        string requiredParentTable,
        string requiredProfileTable,
        string optionalParentTable,
        string optionalProfileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(requiredProfileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(requiredParentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(optionalProfileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(optionalParentTable)}");
    }

    private static void SetupSharedPrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string fkName)
    {
        CleanupSharedPrimaryKeyForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({id}) REFERENCES {parent} ({id}))");
    }

    private static void CleanupSharedPrimaryKeyForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string parentIndexName,
        string profileIndexName,
        string fkName)
    {
        CleanupCompositeUniqueDependentForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(parentIndexName)} ON {parent} ({tenantId}, {accountNo})",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))",
            $"CREATE UNIQUE INDEX {provider.Escape(profileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static void CleanupCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
    }

    private static void SetupOptionalCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string parentTable,
        string profileTable,
        string parentIndexName,
        string profileIndexName,
        string fkName)
    {
        CleanupOptionalCompositeUniqueDependentForeignKey(connection, provider, parentTable, profileTable);

        var parent = provider.Escape(parentTable);
        var profile = provider.Escape(profileTable);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var accountNo = provider.Escape("AccountNo");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {parent} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NOT NULL, {name} {text} NOT NULL)",
            $"CREATE UNIQUE INDEX {provider.Escape(parentIndexName)} ON {parent} ({tenantId}, {accountNo})",
            $"CREATE TABLE {profile} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} {idType} NOT NULL, {accountNo} {idType} NULL, {displayName} {text} NOT NULL, " +
            $"CONSTRAINT {provider.Escape(fkName)} FOREIGN KEY ({tenantId}, {accountNo}) REFERENCES {parent} ({tenantId}, {accountNo}))",
            $"CREATE UNIQUE INDEX {provider.Escape(profileIndexName)} ON {profile} ({tenantId}, {accountNo})");
    }

    private static void CleanupOptionalCompositeUniqueDependentForeignKey(
        DbConnection connection,
        DatabaseProvider provider,
        string parentTable,
        string profileTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(profileTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(parentTable)}");
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
