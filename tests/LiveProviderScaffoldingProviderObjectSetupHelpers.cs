#nullable enable
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private static async Task SetupSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        await TeardownSqlServerNativeTemporalTableAsync(connection, provider);

        var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
        var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var validFrom = provider.Escape("ValidFrom");
        var validTo = provider.Escape("ValidTo");

        await ExecuteAsync(connection, $$"""
            CREATE TABLE {{table}} (
                {{id}} INT NOT NULL PRIMARY KEY,
                {{name}} NVARCHAR(80) NOT NULL,
                {{validFrom}} DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                {{validTo}} DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                PERIOD FOR SYSTEM_TIME ({{validFrom}}, {{validTo}})
            ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {{history}}))
            """);
    }

    private static async Task TeardownSqlServerNativeTemporalTableAsync(DbConnection connection, DatabaseProvider provider)
    {
        try
        {
            var table = SqlServerQualified(provider, SqlServerTemporalBaseTable);
            var history = SqlServerQualified(provider, SqlServerTemporalHistoryTable);
            await ExecuteAsync(connection, $$"""
                IF OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}', N'U') IS NOT NULL
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM sys.tables
                        WHERE object_id = OBJECT_ID(N'dbo.{{SqlServerTemporalBaseTable}}')
                          AND temporal_type = 2
                    )
                    BEGIN
                        ALTER TABLE {{table}} SET (SYSTEM_VERSIONING = OFF);
                    END;

                    DROP TABLE {{table}};
                END;

                IF OBJECT_ID(N'dbo.{{SqlServerTemporalHistoryTable}}', N'U') IS NOT NULL
                    DROP TABLE {{history}};
                """);
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
