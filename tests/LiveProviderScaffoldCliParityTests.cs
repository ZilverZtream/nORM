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

[Trait("Category", "LiveProvider")]
[Collection("LiveProviderScaffolding")]
public sealed partial class LiveProviderScaffoldCliParityTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(2);

    private static (DbConnection Connection, DatabaseProvider Provider, string ConnectionString, string CliProvider)? OpenLive(ProviderKind kind, ref string? sqliteFile)
    {
        if (kind == ProviderKind.Sqlite)
        {
            sqliteFile = Path.Combine(Path.GetTempPath(), "norm_live_cli_scaffold_" + Guid.NewGuid().ToString("N") + ".db");
            var sqliteConnectionString = "Data Source=" + sqliteFile;
            var connection = new SqliteConnection(sqliteConnectionString);
            connection.Open();
            return (connection, new SqliteProvider(), sqliteConnectionString, "sqlite");
        }

        var connectionString = kind switch
        {
            ProviderKind.SqlServer => LiveProviderEnvironment.GetConnectionString("sqlserver"),
            ProviderKind.Postgres => LiveProviderEnvironment.GetConnectionString("postgres"),
            ProviderKind.MySql => LiveProviderEnvironment.GetConnectionString("mysql"),
            _ => null
        };
        if (string.IsNullOrEmpty(connectionString))
            return null;

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is null)
            return null;

        return kind switch
        {
            ProviderKind.SqlServer => (live.Value.Connection, live.Value.Provider, connectionString, "sqlserver"),
            ProviderKind.Postgres => (live.Value.Connection, live.Value.Provider, connectionString, "postgres"),
            ProviderKind.MySql => (live.Value.Connection, live.Value.Provider, connectionString, "mysql"),
            _ => null
        };
    }

    private static DbConnection Reopen(ProviderKind kind, string connectionString)
    {
        if (kind == ProviderKind.Sqlite)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            return connection;
        }

        var live = LiveProviderFactory.OpenLive(kind);
        if (live is not null)
            return live.Value.Connection;

        throw new InvalidOperationException($"Live provider {kind} is no longer available for cleanup.");
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

    private static void SetupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string checkName,
        string defaultName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var createdAt = provider.Escape("CreatedAt");
        var nameLength = provider.Escape("NameLength");
        var check = provider.Escape(checkName);
        var defaultConstraint = provider.Escape(defaultName);

        var createSql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) COLLATE Latin1_General_BIN2 NOT NULL CONSTRAINT {defaultConstraint} DEFAULT ('new'), {nameLength} AS (LEN({name})) PERSISTED, CONSTRAINT {check} CHECK (LEN({name}) > 0))",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) COLLATE \"C\" NOT NULL DEFAULT 'new', {createdAt} timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'utc'), {nameLength} integer GENERATED ALWAYS AS (char_length({name})) STORED, CONSTRAINT {check} CHECK (char_length({name}) > 0))",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'new', {nameLength} int GENERATED ALWAYS AS (CHAR_LENGTH({name})) STORED, CONSTRAINT {check} CHECK (CHAR_LENGTH({name}) > 0))",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {name} TEXT COLLATE NOCASE NOT NULL DEFAULT 'new', {nameLength} INTEGER GENERATED ALWAYS AS (length({name})) VIRTUAL, CONSTRAINT {check} CHECK (length({name}) > 0))",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, createSql);
    }

    private static void SetupUnnamedCheckConstraintMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} int NOT NULL, CHECK ({amount} > 0))");
    }

    private static void SetupUnnamedUniqueConstraintIndex(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupFeatureOwnedMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {code} {text} NOT NULL UNIQUE, {name} {text} NOT NULL)");
    }

    private static void CleanupFeatureOwnedMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupDatabaseComments(connection, provider, kind, tableName);

        const string tableComment = "Table <summary> & description";
        const string columnComment = "Name <tag> & details";
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL COMMENT {SqlLiteral(columnComment)}) COMMENT={SqlLiteral(tableComment)}");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(tableComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral(columnComment) + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(tableName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"COMMENT ON TABLE {table} IS {SqlLiteral(tableComment)}",
                    $"COMMENT ON COLUMN {table}.{name} IS {SqlLiteral(columnComment)}");
                break;
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupDatabaseComments(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        Execute(connection, $"DROP TABLE IF EXISTS {table}");
    }

    private static void SetupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupScalarFacets(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BLOB"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
    }

    private static void CleanupScalarFacets(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string nameIndex,
        string uniqueIndex)
    {
        CleanupIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var tenantId = provider.Escape("TenantId");
        var orderNo = provider.Escape("OrderNo");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {tenantId} int NOT NULL, {orderNo} int NOT NULL, {name} {text} NOT NULL)",
            $"CREATE INDEX {provider.Escape(nameIndex)} ON {table} ({name})",
            $"CREATE UNIQUE INDEX {provider.Escape(uniqueIndex)} ON {table} ({tenantId}, {orderNo})");
    }

    private static void CleanupIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string partialIndex,
        string includedIndex,
        string descendingIndex)
    {
        CleanupProviderIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var includedValue = provider.Escape("IncludedValue");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var activeType = kind == ProviderKind.SqlServer
            ? "bit"
            : kind == ProviderKind.Postgres
                ? "boolean"
                : "int";
        var activePredicate = kind == ProviderKind.SqlServer
            ? $"{active} = 1"
            : kind == ProviderKind.Postgres
                ? $"{active} = TRUE"
                : $"{active} = 1";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {active} {activeType} NOT NULL, {includedValue} int NOT NULL)",
            $"CREATE INDEX {provider.Escape(descendingIndex)} ON {table} ({name} DESC)");

        if (kind != ProviderKind.MySql)
            Execute(connection, $"CREATE INDEX {provider.Escape(partialIndex)} ON {table} ({name}) WHERE {activePredicate}");

        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            Execute(connection, $"CREATE INDEX {provider.Escape(includedIndex)} ON {table} ({name}) INCLUDE ({includedValue})");
    }

    private static void CleanupProviderIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static void SetupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        CleanupTriggerDiagnostics(connection, provider, kind, tableName, triggerName, functionName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var touched = provider.Escape("Touched");
        var intType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";

        Execute(connection, $"CREATE TABLE {table} ({id} {intType} NOT NULL PRIMARY KEY, {touched} {intType} NOT NULL)");

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection, $$"""
                    CREATE TRIGGER {{SqlServerQualified(provider, triggerName)}} ON {{table}}
                    AFTER INSERT AS
                    BEGIN
                        SET NOCOUNT ON;
                        UPDATE target
                        SET {{touched}} = 1
                        FROM {{table}} AS target
                        INNER JOIN inserted AS source ON source.{{id}} = target.{{id}};
                    END
                    """);
                break;
            case ProviderKind.Postgres:
                Execute(connection, $$"""
                    CREATE FUNCTION {{provider.Escape(functionName)}}() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        NEW."Touched" := 1;
                        RETURN NEW;
                    END
                    $$
                    """);
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {provider.Escape(functionName)}()");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TRIGGER {provider.Escape(triggerName)} BEFORE INSERT ON {table} FOR EACH ROW SET NEW.{touched} = 1");
                break;
            case ProviderKind.Sqlite:
                Execute(connection, $$"""
                    CREATE TRIGGER {{provider.Escape(triggerName)}} AFTER INSERT ON {{table}}
                    BEGIN
                        UPDATE {{table}} SET {{touched}} = 1 WHERE {{id}} = NEW.{{id}};
                    END
                    """);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupTriggerDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string triggerName,
        string functionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{triggerName}', N'TR') IS NOT NULL DROP TRIGGER {SqlServerQualified(provider, triggerName)}",
                    DropTable(kind, "dbo." + tableName, SqlServerQualified(provider, tableName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)} ON {provider.Escape(tableName)}",
                    $"DROP FUNCTION IF EXISTS {provider.Escape(functionName)}()",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            case ProviderKind.MySql:
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"DROP TRIGGER IF EXISTS {provider.Escape(triggerName)}",
                    DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void SetupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupProviderSpecificColumnDiagnostics(connection, provider, kind, tableName);

        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        var id = provider.Escape("Id");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var providerColumnSql = kind switch
        {
            ProviderKind.SqlServer => $"{provider.Escape("Location")} geometry NULL",
            ProviderKind.Postgres => $"{provider.Escape("Address")} inet NULL",
            ProviderKind.MySql => $"{provider.Escape("Location")} POINT NULL",
            ProviderKind.Sqlite => $"{provider.Escape("Location")} GEOMETRY NULL",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        Execute(connection, $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {providerColumnSql})");
    }

    private static void CleanupProviderSpecificColumnDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind == ProviderKind.SqlServer
            ? SqlServerQualified(provider, tableName)
            : kind == ProviderKind.Postgres
                ? Qualified(provider, "public", tableName)
                : provider.Escape(tableName);
        Execute(connection, DropTable(kind, tableName, table));
    }

    private static void SetupSafeProviderSpecificColumns(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupSafeProviderSpecificColumns(connection, provider, kind, tableName);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var id = provider.Escape("Id");

        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} UUID NOT NULL, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("XmlPayload")} XML NULL)");
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {provider.Escape("XmlPayload")} xml NOT NULL)");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL)");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {provider.Escape("Payload")} JSON NOT NULL, {provider.Escape("FiscalYear")} YEAR NOT NULL, {provider.Escape("Status")} ENUM('draft','paid','cancelled') NOT NULL, {provider.Escape("Flags")} SET('read','write','admin') NOT NULL)");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.");
        }
    }

    private static void CleanupSafeProviderSpecificColumns(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, tableName),
            ProviderKind.Postgres => Qualified(provider, "public", tableName),
            _ => provider.Escape(tableName)
        };
        var rawName = kind == ProviderKind.SqlServer ? "dbo." + tableName : tableName;
        Execute(connection, DropTable(kind, rawName, table));
    }

    private static void SetupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        CleanupWritableProviderSpecificDiagnostics(connection, provider, kind, tableName, typeName, decimalTypeName, binaryTypeName, arrayTypeName, enumTypeName, enumDomainName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
            {
                var table = Qualified(provider, "dbo", tableName);
                var aliasType = Qualified(provider, "dbo", typeName);
                var decimalAliasType = Qualified(provider, "dbo", decimalTypeName);
                var binaryAliasType = Qualified(provider, "dbo", binaryTypeName);
                Execute(connection,
                    $"CREATE TYPE {aliasType} FROM nvarchar(320) NOT NULL",
                    $"CREATE TYPE {decimalAliasType} FROM decimal(18,4) NOT NULL",
                    $"CREATE TYPE {binaryAliasType} FROM varbinary(64) NOT NULL",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("Email")} {aliasType} NOT NULL, {provider.Escape("Amount")} {decimalAliasType} NOT NULL, {provider.Escape("Token")} {binaryAliasType} NOT NULL)");
                break;
            }
            case ProviderKind.Postgres:
            {
                var table = Qualified(provider, "public", tableName);
                var domain = Qualified(provider, "public", typeName.ToLowerInvariant());
                var scoreDomain = Qualified(provider, "public", decimalTypeName.ToLowerInvariant());
                var scoreArrayDomain = Qualified(provider, "public", arrayTypeName);
                var statusEnum = Qualified(provider, "public", enumTypeName);
                var statusDomain = Qualified(provider, "public", enumDomainName);
                Execute(connection,
                    $"CREATE DOMAIN {domain} AS varchar(320) CHECK (VALUE LIKE '%@%')",
                    $"CREATE DOMAIN {scoreDomain} AS numeric(18,4) CHECK (VALUE >= 0)",
                    $"CREATE DOMAIN {scoreArrayDomain} AS integer[]",
                    $"CREATE TYPE {statusEnum} AS ENUM ('draft', 'active', 'archived')",
                    $"CREATE DOMAIN {statusDomain} AS {statusEnum}",
                    $"CREATE TABLE {table} ({provider.Escape("Id")} integer NOT NULL PRIMARY KEY, {provider.Escape("Email")} {domain} NOT NULL, {provider.Escape("Score")} {scoreDomain} NOT NULL, {provider.Escape("Scores")} {scoreArrayDomain} NOT NULL, {provider.Escape("Status")} {statusDomain} NOT NULL)");
                break;
            }
            case ProviderKind.MySql:
            {
                var table = provider.Escape(tableName);
                Execute(connection,
                    $"CREATE TABLE {table} ({provider.Escape("Id")} INT NOT NULL PRIMARY KEY, {provider.Escape("UnsignedCount")} INT UNSIGNED NOT NULL, {provider.Escape("UnsignedTotal")} BIGINT UNSIGNED NOT NULL, {provider.Escape("UnsignedAmount")} DECIMAL(18,4) UNSIGNED NOT NULL)");
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
        }
    }

    private static void CleanupWritableProviderSpecificDiagnostics(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string typeName,
        string decimalTypeName,
        string binaryTypeName,
        string arrayTypeName,
        string enumTypeName,
        string enumDomainName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    DropTable(kind, "dbo." + tableName, Qualified(provider, "dbo", tableName)),
                    $"IF TYPE_ID(N'dbo.{binaryTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", binaryTypeName)}",
                    $"IF TYPE_ID(N'dbo.{decimalTypeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", decimalTypeName)}",
                    $"IF TYPE_ID(N'dbo.{typeName}') IS NOT NULL DROP TYPE {Qualified(provider, "dbo", typeName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    DropTable(kind, "public." + tableName, Qualified(provider, "public", tableName)),
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", enumDomainName)}",
                    $"DROP TYPE IF EXISTS {Qualified(provider, "public", enumTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", arrayTypeName)}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", decimalTypeName.ToLowerInvariant())}",
                    $"DROP DOMAIN IF EXISTS {Qualified(provider, "public", typeName.ToLowerInvariant())}");
                break;
            case ProviderKind.MySql:
                Execute(connection, DropTable(kind, tableName, provider.Escape(tableName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Writable provider-specific diagnostics are only exposed by providers with alias/domain/unsigned DDL.");
        }
    }

    private static void SetupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string keylessTable)
    {
        CleanupKeylessWarning(connection, provider, keylessTable);

        var table = provider.Escape(keylessTable);
        var status = provider.Escape("Status");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({status} {text} NOT NULL)");
    }

    private static void CleanupKeylessWarning(
        DbConnection connection,
        DatabaseProvider provider,
        string keylessTable)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(keylessTable)}");
    }

    private static void SetupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName)
    {
        CleanupEfStyleAliasTable(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        Execute(connection, $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)");
    }

    private static void CleanupEfStyleAliasTable(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }

    private static string EfProviderPackageName(ProviderKind kind)
        => kind switch
        {
            ProviderKind.SqlServer => "Microsoft.EntityFrameworkCore.SqlServer",
            ProviderKind.Postgres => "Npgsql.EntityFrameworkCore.PostgreSQL",
            ProviderKind.MySql => "Pomelo.EntityFrameworkCore.MySql",
            _ => "Microsoft.EntityFrameworkCore.Sqlite"
        };

    private static void SetupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        CleanupRoutineStub(connection, provider, kind, routineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT AS SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Routine <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'PROCEDURE', @level1name=" + SqlServerLiteral(routineName));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(tenantId integer) RETURNS TABLE(\"Id\" integer, \"Name\" text) LANGUAGE SQL AS $$ SELECT tenantId, 'ok'::text $$",
                    $"COMMENT ON FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(integer) IS {SqlLiteral("Routine <summary> & description")}");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT) COMMENT {SqlLiteral("Routine <summary> & description")} SELECT tenantId AS Id, 'ok' AS Name");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName)
    {
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}",
            ProviderKind.Postgres => $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer)",
            ProviderKind.MySql => $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine scaffold CLI test only targets providers with routine catalogs.")
        });
    }

    private static void SetupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        CleanupAdvancedRoutineStub(connection, provider, kind, routineName, tableFunctionName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)} (@customerId INT) RETURNS INT AS BEGIN RETURN @customerId + 7; END",
                    $"CREATE FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)} (@tenantId INT) RETURNS TABLE AS RETURN SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape("public")}.{provider.Escape(routineName)}(ids integer[], trace_id uuid) RETURNS integer LANGUAGE SQL AS $$ SELECT COALESCE(array_length(ids, 1), 0) $$");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE FUNCTION {provider.Escape(routineName)}(customer_id INT UNSIGNED, max_id BIGINT UNSIGNED, {provider.Escape("rank")} SMALLINT UNSIGNED, flag TINYINT UNSIGNED) RETURNS INT DETERMINISTIC NO SQL RETURN customer_id");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void CleanupAdvancedRoutineStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string tableFunctionName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{tableFunctionName}', N'IF') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(tableFunctionName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'FN') IS NOT NULL DROP FUNCTION {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.Postgres:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape("public")}.{provider.Escape(routineName)}(integer[], uuid)");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP FUNCTION IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Advanced routine scaffold CLI test only targets providers with routine catalogs.");
        }
    }

    private static void SetupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        CleanupRoutineOutputAndNonQueryWrappers(connection, provider, kind, routineName, nonQueryRoutineName);

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)} @tenantId INT, @total DECIMAL(18,2) OUTPUT, @message NVARCHAR(32) OUTPUT AS BEGIN SET @total = 12.34; SET @message = N'ok'; SELECT @tenantId AS Id, CAST('ok' AS nvarchar(20)) AS Name; END",
                    $"CREATE PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)} @tenantId INT, @status NVARCHAR(32) OUTPUT AS BEGIN SET NOCOUNT ON; SET @status = N'ok'; DECLARE @ignored INT = @tenantId; END");
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE PROCEDURE {provider.Escape(routineName)}(IN tenantId INT, OUT total DECIMAL(18,2), INOUT message VARCHAR(32)) BEGIN SET total = 12.34; SET message = CONCAT(COALESCE(message, ''), 'ok'); SELECT tenantId AS Id, 'ok' AS Name; END");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void CleanupRoutineOutputAndNonQueryWrappers(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string routineName,
        string nonQueryRoutineName)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{nonQueryRoutineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(nonQueryRoutineName)}",
                    $"IF OBJECT_ID(N'dbo.{routineName}', N'P') IS NOT NULL DROP PROCEDURE {provider.Escape("dbo")}.{provider.Escape(routineName)}");
                break;
            case ProviderKind.MySql:
                Execute(connection, $"DROP PROCEDURE IF EXISTS {provider.Escape(routineName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Routine output CLI test targets SQL Server and MySQL.");
        }
    }

    private static void SetupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        CleanupSequenceStub(connection, provider, kind, sequenceName);

        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            ProviderKind.Postgres => $"CREATE SEQUENCE {qualifiedName} AS bigint START WITH 100 INCREMENT BY 1",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Sequence <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'SEQUENCE', @level1name=" + SqlServerLiteral(sequenceName),
            ProviderKind.Postgres => $"COMMENT ON SEQUENCE {qualifiedName} IS {SqlLiteral("Sequence <summary> & description")}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void CleanupSequenceStub(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string sequenceName)
    {
        var qualifiedName = kind == ProviderKind.SqlServer
            ? provider.Escape("dbo") + "." + provider.Escape(sequenceName)
            : provider.Escape("public") + "." + provider.Escape(sequenceName);
        Execute(connection, kind switch
        {
            ProviderKind.SqlServer => $"IF OBJECT_ID(N'dbo.{sequenceName}', N'SO') IS NOT NULL DROP SEQUENCE {qualifiedName}",
            ProviderKind.Postgres => $"DROP SEQUENCE IF EXISTS {qualifiedName}",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Sequence scaffold CLI test only targets SQL Server and PostgreSQL.")
        });
    }

    private static void SetupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string dependentTable)
    {
        CleanupFilteredRelationship(connection, provider, principalTable, dependentTable);

        var principal = provider.Escape(principalTable);
        var dependent = provider.Escape(dependentTable);
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
            $"CREATE TABLE {principal} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
            $"CREATE TABLE {dependent} ({id} int NOT NULL PRIMARY KEY, {parentId} int NOT NULL, {name} {text} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {principal} ({id}))");
    }

    private static void CleanupFilteredRelationship(
        DbConnection connection,
        DatabaseProvider provider,
        string principalTable,
        string dependentTable)
    {
        Execute(connection,
            $"DROP TABLE IF EXISTS {provider.Escape(dependentTable)}",
            $"DROP TABLE IF EXISTS {provider.Escape(principalTable)}");
    }

    private static void SetupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        CleanupViewQueryArtifact(connection, provider, kind, baseTable, viewName);

        var table = provider.Escape(baseTable);
        var view = provider.Escape(viewName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void AssertViewQueryArtifactCommentDocumentation(ProviderKind kind, string viewCode)
    {
        if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
        {
            Assert.Contains("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// Name &lt;view&gt; &amp; details", viewCode, StringComparison.Ordinal);
            Assert.Contains("/// <remarks>Maps to column Name</remarks>", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Name <view> & details", viewCode, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("/// Maps to column Name", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// View &lt;summary&gt; &amp; description", viewCode, StringComparison.Ordinal);
            Assert.DoesNotContain("/// VIEW", viewCode, StringComparison.Ordinal);
        }
    }

    private static void CleanupViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        Execute(connection,
            DropView(kind, viewName, provider.Escape(viewName)),
            $"DROP TABLE IF EXISTS {provider.Escape(baseTable)}");
    }

    private static void SetupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        CleanupProviderQueryArtifact(connection, provider, kind, baseName, artifactName);

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection,
                    $"CREATE VIRTUAL TABLE {provider.Escape(artifactName)} USING fts5({provider.Escape("Content")})");
                break;
            case ProviderKind.SqlServer:
            {
                var baseTable = Qualified(provider, "dbo", baseName);
                var synonym = Qualified(provider, "dbo", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} int NOT NULL PRIMARY KEY, {name} nvarchar(80) NOT NULL)",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=" + SqlServerLiteral(baseName) + ", @level2type=N'COLUMN', @level2name=N'Name'",
                    $"CREATE SYNONYM {synonym} FOR {baseTable}");
                break;
            }
            case ProviderKind.Postgres:
            {
                var baseTable = Qualified(provider, "public", baseName);
                var matView = Qualified(provider, "public", artifactName);
                Execute(connection,
                    $"CREATE TABLE {baseTable} ({id} integer NOT NULL PRIMARY KEY, {name} text NOT NULL)",
                    $"CREATE MATERIALIZED VIEW {matView} AS SELECT {id}, {name} FROM {baseTable}",
                    $"COMMENT ON MATERIALIZED VIEW {matView} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {matView}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            }
            case ProviderKind.MySql:
                Execute(connection,
                    $"CREATE TABLE {provider.Escape(baseName)} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) NOT NULL)",
                    $"CREATE VIEW {provider.Escape(artifactName)} AS SELECT {id}, {name} FROM {provider.Escape(baseName)}");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, PostgreSQL, and MySQL.");
        }
    }

    private static void CleanupProviderQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseName,
        string artifactName)
    {
        switch (kind)
        {
            case ProviderKind.Sqlite:
                Execute(connection, DropTable(kind, artifactName, provider.Escape(artifactName)));
                break;
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"IF OBJECT_ID(N'dbo.{artifactName}', N'SN') IS NOT NULL DROP SYNONYM {Qualified(provider, "dbo", artifactName)}",
                    DropTable(kind, "dbo." + baseName, Qualified(provider, "dbo", baseName)));
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"DROP MATERIALIZED VIEW IF EXISTS {Qualified(provider, "public", artifactName)}",
                    DropTable(kind, "public." + baseName, Qualified(provider, "public", baseName)));
                break;
            case ProviderKind.MySql:
                Execute(connection,
                    DropView(kind, artifactName, provider.Escape(artifactName)),
                    DropTable(kind, baseName, provider.Escape(baseName)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Provider query artifact CLI test targets SQLite, SQL Server, PostgreSQL, and MySQL.");
        }
    }

    private static void SetupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        CleanupDefaultDiscoveryViewQueryArtifact(connection, provider, kind, schemaName, baseTable, viewName);

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NULL EXEC(N'CREATE SCHEMA {provider.Escape(schemaName)}')");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"CREATE SCHEMA IF NOT EXISTS {provider.Escape(schemaName)}");

        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var text = kind switch
        {
            ProviderKind.SqlServer => "nvarchar(80)",
            ProviderKind.MySql => "varchar(80)",
            _ => "text"
        };

        switch (kind)
        {
            case ProviderKind.SqlServer:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("View <summary> & description") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName),
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=" + SqlServerLiteral("Name <view> & details") + ", @level0type=N'SCHEMA', @level0name=" + SqlServerLiteral(schemaName) + ", @level1type=N'VIEW', @level1name=" + SqlServerLiteral(viewName) + ", @level2type=N'COLUMN', @level2name=N'Name'");
                break;
            case ProviderKind.Postgres:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}",
                    $"COMMENT ON VIEW {view} IS {SqlLiteral("View <summary> & description")}",
                    $"COMMENT ON COLUMN {view}.{name} IS {SqlLiteral("Name <view> & details")}");
                break;
            default:
                Execute(connection,
                    $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL)",
                    $"CREATE VIEW {view} AS SELECT {id}, {name} FROM {table}");
                break;
        }
    }

    private static void CleanupDefaultDiscoveryViewQueryArtifact(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string schemaName,
        string baseTable,
        string viewName)
    {
        var table = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, baseTable)
            : provider.Escape(baseTable);
        var view = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? Qualified(provider, schemaName, viewName)
            : provider.Escape(viewName);
        var rawTable = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + baseTable
            : baseTable;
        var rawView = kind is ProviderKind.SqlServer or ProviderKind.Postgres
            ? schemaName + "." + viewName
            : viewName;

        Execute(connection,
            DropView(kind, rawView, view),
            DropTable(kind, rawTable, table));

        if (kind == ProviderKind.SqlServer)
            Execute(connection, $"IF SCHEMA_ID(N'{schemaName}') IS NOT NULL DROP SCHEMA {provider.Escape(schemaName)}");
        else if (kind == ProviderKind.Postgres)
            Execute(connection, $"DROP SCHEMA IF EXISTS {provider.Escape(schemaName)}");
    }

    private static string DropView(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'V') IS NOT NULL DROP VIEW {escapedName}"
        : $"DROP VIEW IF EXISTS {escapedName}";

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
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, cascadeDelete: false);"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, \"{constraintName}\", false);";

    private static string ExpectedReferentialForeignKey(
        ProviderKind kind,
        string foreignKeySelector,
        string principalKeySelector,
        string onDelete,
        string onUpdate,
        string constraintName)
        => kind == ProviderKind.Sqlite
            ? $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate});"
            : $".HasForeignKey({foreignKeySelector}, {principalKeySelector}, {onDelete}, {onUpdate}, \"{constraintName}\");";

    private static void AssertPossibleJoinPayloadDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                if (reason.GetString() == "payload-columns")
                    return;
            }
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic for {tableName}.");
    }

    private static void AssertPossibleJoinPayloadDiagnosticWithoutCompositeForeignKey(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            var hasPayloadReason = false;
            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                if (reasonText == "payload-columns")
                {
                    hasPayloadReason = true;
                    continue;
                }

                Assert.NotEqual("composite-foreign-key", reasonText);
            }

            if (hasPayloadReason)
                return;
        }

        Assert.Fail($"Expected possible many-to-many payload diagnostic without composite-FK rejection for {tableName}.");
    }

    private static void AssertPossibleManyToManyDiagnosticReason(string warningJsonPath, string tableName, params string[] expectedReasons)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("possibleManyToManyJoinTables").EnumerateArray())
        {
            if (!LastTableNameEquals(item.GetProperty("table").GetString(), tableName))
                continue;

            foreach (var reason in item.GetProperty("reasons").EnumerateArray())
            {
                var reasonText = reason.GetString();
                foreach (var expectedReason in expectedReasons)
                {
                    if (reasonText == expectedReason)
                        return;
                }
            }
        }

        Assert.Fail($"Expected possible many-to-many diagnostic for {tableName} with one of: {string.Join(", ", expectedReasons)}.");
    }

    private static void AssertRelationshipDependentKeyDiagnostic(string warningJsonPath, string tableName)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        foreach (var item in warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "RelationshipDependentKey" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("suggestedAction").GetString()?.Contains("primary key", StringComparison.OrdinalIgnoreCase) == true)
            {
                return;
            }
        }

        Assert.Fail($"Expected relationship dependent-key diagnostic for {tableName}.");
    }

    private static void AssertTriggerDiagnostic(string warningJsonPath, string tableName, string triggerName, ProviderKind kind)
    {
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        JsonElement? triggerDiagnostic = null;

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("code").GetString() == "SCF110" &&
                item.GetProperty("category").GetString() == "database-object" &&
                item.GetProperty("kind").GetString() == "Trigger" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == triggerName)
            {
                Assert.False(triggerDiagnostic.HasValue, "Expected exactly one trigger diagnostic.");
                triggerDiagnostic = item;
            }
        }

        Assert.True(triggerDiagnostic.HasValue, "Expected trigger diagnostic SCF110 in scaffold warning JSON.");
        var metadata = triggerDiagnostic.Value.GetProperty("metadata");
        Assert.Equal("Trigger", metadata.GetProperty("providerObjectKind").GetString());
        Assert.True(LastTableNameEquals(metadata.GetProperty("table").GetString(), tableName));
        Assert.Equal(triggerName, metadata.GetProperty("triggerName").GetString());
        Assert.True(metadata.GetProperty("providerOwnedDdl").GetBoolean());
        Assert.False(metadata.GetProperty("generatedModelConfigurationSupported").GetBoolean());
        Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
        Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
        Assert.Equal("provider-owned-trigger", metadata.GetProperty("reason").GetString());

        if (kind == ProviderKind.Sqlite)
        {
            Assert.True(metadata.GetProperty("definitionAvailable").GetBoolean());
            Assert.Contains("CREATE TRIGGER", metadata.GetProperty("triggerSql").GetString(), StringComparison.OrdinalIgnoreCase);
        }
    }

    private static void AssertProviderSpecificColumnDiagnostic(string warningJsonPath, string columnName, string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.True(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-column-type", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertWritableProviderSpecificColumnDiagnostic(
        string warningJsonPath,
        string tableName,
        string columnName,
        string expectedDetail)
    {
        Assert.True(File.Exists(warningJsonPath), $"Expected scaffold warning JSON at {warningJsonPath}.");
        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");

        foreach (var item in providerOwned.EnumerateArray())
        {
            if (item.GetProperty("kind").GetString() == "ProviderSpecificColumnType" &&
                item.GetProperty("code").GetString() == "SCF104" &&
                LastTableNameEquals(item.GetProperty("table").GetString(), tableName) &&
                item.GetProperty("name").GetString() == columnName &&
                item.GetProperty("detail").GetString()?.Contains(expectedDetail, StringComparison.OrdinalIgnoreCase) == true)
            {
                var metadata = item.GetProperty("metadata");
                Assert.False(metadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.True(metadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-specific-ddl", metadata.GetProperty("reason").GetString());
                Assert.Contains("provider-specific type", item.GetProperty("suggestedAction").GetString(), StringComparison.OrdinalIgnoreCase);
                return;
            }
        }

        Assert.Fail($"Expected writable provider-specific column diagnostic for {columnName} containing {expectedDetail}.");
    }

    private static void AssertFeatureMetadataHasNoProviderOwnedDiagnostics(string warningJsonPath, string tableName)
    {
        if (!File.Exists(warningJsonPath))
            return;

        using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
        var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures");
        foreach (var item in providerOwned.EnumerateArray())
        {
            var kind = item.GetProperty("kind").GetString();
            var table = item.GetProperty("table").GetString();
            Assert.False(
                LastTableNameEquals(table, tableName) &&
                kind is "Default" or "CheckConstraint" or "Computed" or "Collation",
                $"Expected {tableName} {kind} metadata to be promoted instead of reported as provider-owned diagnostics.");
        }
    }

    private static bool LastTableNameEquals(string? actual, string expected)
    {
        if (string.IsNullOrEmpty(actual))
            return false;

        var candidate = actual;
        var dot = candidate.LastIndexOf('.');
        if (dot >= 0 && dot < candidate.Length - 1)
            candidate = candidate[(dot + 1)..];

        candidate = candidate.Trim('[', ']', '"', '`');
        return string.Equals(candidate, expected, StringComparison.OrdinalIgnoreCase);
    }

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static string SqlServerQualified(DatabaseProvider provider, string name)
        => provider.Escape("dbo") + "." + provider.Escape(name);

    private static string Qualified(DatabaseProvider provider, string schemaName, string tableName)
        => provider.Escape(schemaName) + "." + provider.Escape(tableName);

    private static string ConnectionStringWithDatabase(string connectionString, string databaseName)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        builder["Database"] = databaseName;
        return builder.ConnectionString;
    }

    private static void Execute(DbConnection connection, params string[] statements)
    {
        foreach (var statement in statements)
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    private static void WriteConsumerProject(string root, string output)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(Path.Combine(output, "CliLiveScaffolded.csproj"), $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <ImplicitUsings>enable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProject(string root, string projectPath, string? userSecretsId = null)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        var userSecretsProperty = string.IsNullOrWhiteSpace(userSecretsId)
            ? string.Empty
            : $"{Environment.NewLine}                <UserSecretsId>{userSecretsId}</UserSecretsId>";
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <RootNamespace>Live.Project.Namespace</RootNamespace>
                <Nullable>disable</Nullable>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>{{userSecretsProperty}}
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProjectWithoutMetadata(string root, string projectPath)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static void WriteLiveScaffoldProjectWithAssemblyMetadata(string root, string projectPath, string userSecretsId)
    {
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
        Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
        File.WriteAllText(projectPath, $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <AssemblyName>Project.Assembly.Namespace</AssemblyName>
                <Nullable>disable</Nullable>
                <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                <ImplicitUsings>disable</ImplicitUsings>
              </PropertyGroup>
              <ItemGroup>
                <Reference Include="nORM">
                  <HintPath>{{normAssembly}}</HintPath>
                </Reference>
              </ItemGroup>
            </Project>
            """, Encoding.UTF8);
    }

    private static string GetUserSecretsFilePathForLiveTest(string userSecretsId)
    {
        if (OperatingSystem.IsWindows())
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            return Path.Combine(appData, "Microsoft", "UserSecrets", userSecretsId, "secrets.json");
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".microsoft", "usersecrets", userSecretsId, "secrets.json");
    }

    private static CliResult RunCli(
        string arguments,
        string workingDirectory,
        IReadOnlyDictionary<string, string?>? environment = null)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}.");

        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory, environment);
    }

    private static void RunDotNet(string arguments, string workingDirectory)
    {
        var result = RunProcess("dotnet", arguments, workingDirectory);
        Assert.True(result.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
    }

    private static CliResult RunProcess(
        string fileName,
        string arguments,
        string workingDirectory,
        IReadOnlyDictionary<string, string?>? environment = null)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        if (environment is not null)
        {
            foreach (var entry in environment)
            {
                if (entry.Value is null)
                    startInfo.Environment.Remove(entry.Key);
                else
                    startInfo.Environment[entry.Key] = entry.Value;
            }
        }

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(ProcessTimeout))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // The process may exit between timeout detection and Kill.
            }

            process.WaitForExit();
            var timedOutStdout = stdoutTask.GetAwaiter().GetResult();
            var timedOutStderr = stderrTask.GetAwaiter().GetResult();
            throw new TimeoutException(
                $"{fileName} {arguments} did not exit within {ProcessTimeout.TotalSeconds:N0} seconds.{Environment.NewLine}STDOUT:{Environment.NewLine}{timedOutStdout}{Environment.NewLine}STDERR:{Environment.NewLine}{timedOutStderr}");
        }

        process.WaitForExit();
        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        return new CliResult(process.ExitCode, stdout, stderr);
    }

    private static string IdentifierSuffix()
    {
        var value = Guid.NewGuid().ToString("N");
        return "A" + value[..6] + "A";
    }

    private static string Quote(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string SqlServerLiteral(string value) => "N" + SqlLiteral(value);

    private static string StripDefaultSchemaArguments(string generatedCode)
        => generatedCode
            .Replace(", schema: \"dbo\"", string.Empty, StringComparison.Ordinal)
            .Replace(", schema: \"public\"", string.Empty, StringComparison.Ordinal);

    private static string IdentityPrimaryKeyColumn(ProviderKind kind, string escapedColumnName) => kind switch
    {
        ProviderKind.SqlServer => $"{escapedColumnName} INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
        ProviderKind.Postgres => $"{escapedColumnName} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
        ProviderKind.MySql => $"{escapedColumnName} INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        _ => $"{escapedColumnName} INTEGER PRIMARY KEY AUTOINCREMENT"
    };

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; failed deletion only leaves a temp directory.
        }
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
