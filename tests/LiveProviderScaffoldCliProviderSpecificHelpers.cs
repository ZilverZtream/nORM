#nullable enable

using System;
using System.Data.Common;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
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
                    $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {provider.Escape("TraceId")} uuid NOT NULL, {provider.Escape("Scores")} integer[] NULL, {provider.Escape("Tags")} text[] NULL, {provider.Escape("Ratings")} numeric(10,2)[] NULL, {provider.Escape("Aliases")} varchar(32)[] NULL)");
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
}
