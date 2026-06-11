#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    // Live provider-owned object scaffold parity tests.

    [Fact]
    public async Task ScaffoldAsync_reports_sqlserver_native_temporal_tables_and_marks_them_read_only()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerNativeTemporalTableAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_temporal_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqlServerTemporalContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { "dbo." + SqlServerTemporalBaseTable, "dbo." + SqlServerTemporalHistoryTable },
                        OverwriteFiles = false
                    });

                var baseCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerTemporalBaseTable + ".cs"));
                var historyCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerTemporalHistoryTable + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", baseCode, StringComparison.Ordinal);
                Assert.Contains("[ReadOnlyEntity]", historyCode, StringComparison.Ordinal);
                var baseTemporal = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalBaseTable &&
                    item.GetProperty("code").GetString() == "SCF115");
                var baseTemporalMetadata = baseTemporal.GetProperty("metadata");
                Assert.Equal("TemporalTable", baseTemporalMetadata.GetProperty("providerObjectKind").GetString());
                Assert.True(baseTemporalMetadata.GetProperty("providerNativeTemporal").GetBoolean());
                Assert.False(baseTemporalMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
                Assert.True(baseTemporalMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(baseTemporalMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-native-temporal", baseTemporalMetadata.GetProperty("reason").GetString());
                Assert.Equal("system-versioned", baseTemporalMetadata.GetProperty("temporalType").GetString());
                Assert.Contains(SqlServerTemporalHistoryTable, baseTemporalMetadata.GetProperty("historyTable").GetString(), StringComparison.OrdinalIgnoreCase);
                var historyTemporal = Assert.Single(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "TemporalTable" &&
                    item.GetProperty("table").GetString() == "dbo." + SqlServerTemporalHistoryTable &&
                    item.GetProperty("detail").GetString()!.Contains("history table", StringComparison.OrdinalIgnoreCase));
                var historyTemporalMetadata = historyTemporal.GetProperty("metadata");
                Assert.Equal("history", historyTemporalMetadata.GetProperty("temporalType").GetString());
                Assert.True(historyTemporalMetadata.GetProperty("providerNativeTemporal").GetBoolean());
                Assert.False(historyTemporalMetadata.GetProperty("generatedTemporalConfigurationSupported").GetBoolean());
                Assert.True(historyTemporalMetadata.GetProperty("readOnlyEntity").GetBoolean());
                Assert.False(historyTemporalMetadata.GetProperty("generatedWritesSupported").GetBoolean());
                Assert.Equal("provider-native-temporal", historyTemporalMetadata.GetProperty("reason").GetString());

                var dynamicBaseType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + SqlServerTemporalBaseTable);
                var dynamicHistoryType = await new DynamicEntityTypeGenerator().GenerateEntityTypeAsync(connection, "dbo." + SqlServerTemporalHistoryTable);
                Assert.NotNull(dynamicBaseType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());
                Assert.NotNull(dynamicHistoryType.GetCustomAttributes(typeof(nORM.Configuration.ReadOnlyEntityAttribute), inherit: true).SingleOrDefault());

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerNativeTemporalTableAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_rejects_sqlserver_procedure_synonym_as_entity_filter()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupSqlServerProcedureSynonymAsync(connection, provider);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_proc_synonym_" + Guid.NewGuid().ToString("N"));
            try
            {
                var ex = await Assert.ThrowsAsync<nORM.Core.NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldSqlServerProcedureSynonymContext",
                        new ScaffoldOptions
                        {
                            Tables = new[] { "dbo." + SqlServerProcedureSynonym },
                            EmitQueryArtifacts = true,
                            OverwriteFiles = false
                        }));

                Assert.Contains("matched database object", ex.Message, StringComparison.Ordinal);
                Assert.Contains("Synonym dbo." + SqlServerProcedureSynonym, ex.Message, StringComparison.Ordinal);
                Assert.Contains("does not emit as entity classes", ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSqlServerProcedureSynonymAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_reports_mysql_event_diagnostics_on_live_provider()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.MySql);
        if (Skip.If(live is null, "Live provider MySQL not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            try
            {
                await SetupMySqlEventDiagnosticsAsync(connection, provider);
            }
            catch (Exception ex)
            {
                await TeardownMySqlEventDiagnosticsAsync(connection, provider);
                if (Skip.If(true, $"MySQL EVENT privilege is not available in this live database: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_mysql_event_diag_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldMySqlEventDiagnosticsContext",
                    new ScaffoldOptions { Tables = new[] { MySqlEventDiagnosticsName }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, MySqlEventDiagnosticsName + ".cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains("public int Id { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "Event" &&
                    item.GetProperty("code").GetString() == "SCF205" &&
                    item.GetProperty("category").GetString() == "routine" &&
                    item.GetProperty("name").GetString() == MySqlEventDiagnosticsName &&
                    item.GetProperty("suggestedAction").GetString()!.Contains("scheduled event", StringComparison.OrdinalIgnoreCase));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMySqlEventDiagnosticsAsync(connection, provider);
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_emits_sqlite_virtual_table_as_read_only_query_artifact()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Sqlite);
        if (Skip.If(live is null, "Live provider SQLite not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, SqliteVirtualTable, provider.Escape(SqliteVirtualTable)));
            try
            {
                await ExecuteAsync(connection,
                    $"CREATE VIRTUAL TABLE {provider.Escape(SqliteVirtualTable)} USING fts5({provider.Escape("Content")})");
            }
            catch (Exception ex)
            {
                if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
            }

            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlite_virtual_table_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldSqliteVirtualTableContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { SqliteVirtualTable },
                        EmitQueryArtifacts = true,
                        OverwriteFiles = false
                    });

                var virtualCode = await File.ReadAllTextAsync(Path.Combine(dir, SqliteVirtualTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldSqliteVirtualTableContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
                var skippedObjects = warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray().ToArray();

                Assert.Contains("[ReadOnlyEntity]", virtualCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{SqliteVirtualTable}>", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    item.GetProperty("table").GetString() == SqliteVirtualTable);
                Assert.Contains(skippedObjects, item =>
                    item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                    item.GetProperty("code").GetString() == "SCF207" &&
                    item.GetProperty("name").GetString()!.StartsWith(SqliteVirtualTable + "_", StringComparison.Ordinal));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.Sqlite, SqliteVirtualTable, provider.Escape(SqliteVirtualTable)));
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_postgres_serial_primary_key_does_not_emit_default_or_owned_sequence_warnings()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Postgres);
        if (Skip.If(live is null, "Live provider Postgres not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresSerialTable, provider.Escape(PostgresSerialTable)));
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pg_serial_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection, $"CREATE TABLE {provider.Escape(PostgresSerialTable)} ({provider.Escape("Id")} SERIAL PRIMARY KEY, {provider.Escape("Name")} {TextType(ProviderKind.Postgres, 40)} NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPostgresSerialContext",
                    new ScaffoldOptions { Tables = new[] { PostgresSerialTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, PostgresSerialTable + ".cs"));
                Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Identity)]", entityCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.Postgres, PostgresSerialTable, provider.Escape(PostgresSerialTable)));
            }
        }
    }

    [Fact]
    public async Task ScaffoldAsync_marks_sqlserver_rowversion_as_timestamp_and_database_generated()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.SqlServer);
        if (Skip.If(live is null, "Live provider SQL Server not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, SqlServerRowVersionTable, provider.Escape(SqlServerRowVersionTable)));
            var table = provider.Escape(SqlServerRowVersionTable);
            var id = provider.Escape("Id");
            var name = provider.Escape("Name");
            var rowVersion = provider.Escape("RowVersion");
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_sqlserver_rowversion_" + Guid.NewGuid().ToString("N"));
            try
            {
                await ExecuteAsync(connection,
                    $"CREATE TABLE {table} ({id} INT NOT NULL PRIMARY KEY, {name} NVARCHAR(80) NOT NULL, {rowVersion} rowversion NOT NULL)");

                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldRowVersionContext",
                    new ScaffoldOptions { Tables = new[] { SqlServerRowVersionTable }, OverwriteFiles = false });

                var entityCode = await File.ReadAllTextAsync(Path.Combine(dir, SqlServerRowVersionTable + ".cs"));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldRowVersionContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

                Assert.Contains("[Timestamp]", entityCode, StringComparison.Ordinal);
                Assert.Contains("[DatabaseGenerated(DatabaseGeneratedOption.Computed)]", entityCode, StringComparison.Ordinal);
                Assert.Contains("public byte[] RowVersion { get; set; } = Array.Empty<byte>();", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain(".Property(e => e.RowVersion).HasComputedColumnSql", contextCode, StringComparison.Ordinal);
                Assert.Contains(providerOwned, item =>
                    item.GetProperty("kind").GetString() == "RowVersion" &&
                    item.GetProperty("code").GetString() == "SCF108" &&
                    item.GetProperty("table").GetString()!.EndsWith(SqlServerRowVersionTable, StringComparison.Ordinal));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await ExecuteAsync(connection, DropTable(ProviderKind.SqlServer, SqlServerRowVersionTable, provider.Escape(SqlServerRowVersionTable)));
            }
        }
    }

}
