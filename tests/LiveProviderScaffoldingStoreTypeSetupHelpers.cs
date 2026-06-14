#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task SetupDecimalPrecisionAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await ExecuteAsync(connection, DropTable(kind, DecimalPrecisionTable, provider.Escape(DecimalPrecisionTable)));

        var table = provider.Escape(DecimalPrecisionTable);
        var id = provider.Escape("Id");
        var amount = provider.Escape("Amount");
        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {amount} DECIMAL(28,6) NOT NULL)");
    }

    private static async Task SetupStringBinaryFacetsAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind, bool alternate)
    {
        await ExecuteAsync(connection, DropTable(kind, StringBinaryFacetTable, provider.Escape(StringBinaryFacetTable)));

        var table = provider.Escape(StringBinaryFacetTable);
        var id = provider.Escape("Id");
        var code = provider.Escape("Code");
        var fixedCode = provider.Escape("FixedCode");
        var token = provider.Escape("Token");
        var (codeType, fixedCodeType, tokenType) = kind switch
        {
            ProviderKind.SqlServer when alternate => ("NVARCHAR(40)", "NCHAR(12)", "VARBINARY(16)"),
            ProviderKind.SqlServer => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Postgres when alternate => ("CHAR(40)", "VARCHAR(12)", "BYTEA"),
            ProviderKind.Postgres => ("VARCHAR(40)", "CHAR(12)", "BYTEA"),
            ProviderKind.MySql when alternate => ("CHAR(40)", "VARCHAR(12)", "VARBINARY(16)"),
            ProviderKind.MySql => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            ProviderKind.Sqlite when alternate => ("CHAR(40)", "VARCHAR(12)", "VARBINARY(16)"),
            ProviderKind.Sqlite => ("VARCHAR(40)", "CHAR(12)", "BINARY(16)"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {code} {codeType} NOT NULL, {fixedCode} {fixedCodeType} NOT NULL, {token} {tokenType} NOT NULL)");
    }

    private static async Task SetupTemporalStoreTypeTableAsync(DbConnection connection, DatabaseProvider provider, ProviderKind kind)
    {
        await TeardownTemporalStoreTypeTableAsync(connection, provider, kind);

        var table = kind switch
        {
            ProviderKind.SqlServer => SqlServerQualified(provider, TemporalStoreTypeTable),
            ProviderKind.Postgres => Qualified(provider, "public", TemporalStoreTypeTable),
            _ => provider.Escape(TemporalStoreTypeTable)
        };
        var id = provider.Escape("Id");
        var businessDate = provider.Escape("BusinessDate");
        var startsAt = provider.Escape("StartsAt");
        var createdAt = provider.Escape("CreatedAt");
        var offsetAt = provider.Escape("OffsetAt");
        var sql = kind switch
        {
            ProviderKind.SqlServer =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time NULL, {createdAt} datetime2 NOT NULL, {offsetAt} datetimeoffset NULL)",
            ProviderKind.Postgres =>
                $"CREATE TABLE {table} ({id} integer NOT NULL PRIMARY KEY, {businessDate} date NOT NULL, {startsAt} time without time zone NULL, {createdAt} timestamp without time zone NOT NULL, {offsetAt} timestamp with time zone NULL)",
            ProviderKind.MySql =>
                $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL)",
            ProviderKind.Sqlite =>
                $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {businessDate} DATE NOT NULL, {startsAt} TIME NULL, {createdAt} DATETIME NOT NULL, {offsetAt} DATETIMEOFFSET NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported live provider kind.")
        };

        await ExecuteAsync(connection, sql);
    }
}
