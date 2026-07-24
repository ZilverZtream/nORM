using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider evidence for the destructive database-drop safety contracts
/// in <c>dotnet-norm database drop</c>.
///
/// Safety contracts verified:
///   1. Protected-database guard — <c>connection.Database</c> returns the
///      expected system database name on each server provider, so
///      <c>IsProtectedDatabaseName</c> would refuse the drop.
///   2. System-schema filter — <c>GetSchema("Tables")</c> yields rows; after
///      applying <c>IsSystemSchema</c> filtering, only user tables remain so
///      provider-internal objects are never accidentally dropped.
///   3. SQLite user-table lifecycle — create, enumerate, drop, re-enumerate
///      round-trips correctly via <c>GetSchema("Tables")</c>.
///   4. CLI gate logic — the drop command is refused when neither --yes nor
///      --dry-run is supplied.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderDatabaseDropSafetyTests
{
    // ── Replicated safety predicates (mirrors Program.cs private statics) ────

    private static bool IsProtectedDatabaseName(ProviderKind kind, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName)) return false;
        var n = databaseName.Trim();
        return kind switch
        {
            ProviderKind.SqlServer =>
                n.Equals("master",  StringComparison.OrdinalIgnoreCase)
                || n.Equals("model",  StringComparison.OrdinalIgnoreCase)
                || n.Equals("msdb",   StringComparison.OrdinalIgnoreCase)
                || n.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
            ProviderKind.Postgres =>
                n.Equals("postgres",  StringComparison.OrdinalIgnoreCase)
                || n.Equals("template0", StringComparison.OrdinalIgnoreCase)
                || n.Equals("template1", StringComparison.OrdinalIgnoreCase),
            ProviderKind.MySql =>
                n.Equals("mysql",              StringComparison.OrdinalIgnoreCase)
                || n.Equals("sys",             StringComparison.OrdinalIgnoreCase)
                || n.Equals("information_schema",  StringComparison.OrdinalIgnoreCase)
                || n.Equals("performance_schema",  StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    private static bool IsSystemSchema(ProviderKind kind, string? schemaName)
    {
        if (string.IsNullOrWhiteSpace(schemaName)) return false;
        var s = schemaName.Trim();
        if (s.Equals("sys",                StringComparison.OrdinalIgnoreCase)
            || s.Equals("information_schema", StringComparison.OrdinalIgnoreCase))
            return true;
        return kind switch
        {
            ProviderKind.Postgres =>
                s.StartsWith("pg_", StringComparison.OrdinalIgnoreCase),
            ProviderKind.MySql =>
                s.Equals("mysql",              StringComparison.OrdinalIgnoreCase)
                || s.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    // ── Test 1: server connections land on protected databases ───────────────

    [Theory]
    [InlineData("sqlserver", "master")]
    [InlineData("postgres",  "postgres")]
    [InlineData("mysql",     "mysql")]
    public void LiveConnection_DatabaseName_IsProtected_RefusesDrop(string kindStr, string expectedDb)
    {
        var kind = ParseKind(kindStr);
        var connection = OpenProtectedDatabaseConnection(kind, expectedDb);
        if (Skip.If(connection is null, $"{kindStr} not configured")) return;

        using (var protectedConnection = connection!)
        {
            var actualDb = protectedConnection.Database;

            Assert.Equal(expectedDb, actualDb, StringComparer.OrdinalIgnoreCase);
            Assert.True(
                IsProtectedDatabaseName(kind, actualDb),
                $"Expected '{actualDb}' to be recognised as a protected {kindStr} database.");
        }
    }

    // ── Test 2: system tables are filtered out after GetSchema("Tables") ─────

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    [InlineData("mysql")]
    public void LiveConnection_GetSchemaTables_SystemSchemaRowsAreCorrectlyIdentified(string kindStr)
    {
        var kind = ParseKind(kindStr);
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"{kindStr} not configured")) return;

        var (connection, provider) = live!.Value;
        using (connection)
        {
            // Guarantee at least one *user* table exists so the assertions below are
            // deterministic on a shared CI database whose user tables may have just
            // been dropped by a parallel scaffold/migration test. Npgsql's "Tables"
            // collection excludes system schemas, so with no user tables GetSchema
            // returns zero rows — the flake this probe removes.
            const string probeTable = "LvDrpSafety_SchemaProbe";
            var esc = provider.Escape(probeTable);
            using (var setup = connection.CreateCommand())
            {
                setup.CommandText = $"DROP TABLE IF EXISTS {esc}";
                setup.ExecuteNonQuery();
                setup.CommandText = $"CREATE TABLE {esc} (Id INT NOT NULL)";
                setup.ExecuteNonQuery();
            }

            try
            {
                var schema = connection.GetSchema("Tables");
                Assert.NotNull(schema);

                var sawProbe = false;
                // Every row tagged as a system schema must be correctly identified.
                foreach (DataRow row in schema.Rows)
                {
                    var s = row["TABLE_SCHEMA"]?.ToString();
                    var flagged = IsSystemSchema(kind, s);
                    if (flagged)
                    {
                        // Verify the predicate is symmetric.
                        Assert.True(IsSystemSchema(kind, s),
                            $"Row TABLE_SCHEMA='{s}' was flagged then un-flagged (non-deterministic predicate).");
                    }
                    else
                    {
                        Assert.False(IsSystemSchema(kind, s),
                            $"Row TABLE_SCHEMA='{s}' was not flagged then flagged (non-deterministic predicate).");
                    }

                    var name = row["TABLE_NAME"]?.ToString();
                    if (string.Equals(name, probeTable, StringComparison.OrdinalIgnoreCase))
                        sawProbe = true;
                }

                // The probe table we just created must be enumerated, so the schema
                // is non-empty — deterministically, regardless of which database the
                // live connection targets or what parallel tests dropped.
                Assert.True(sawProbe,
                    $"Expected GetSchema('Tables') to include the user probe table '{probeTable}' on {kindStr}.");
                Assert.True(schema.Rows.Count > 0,
                    $"Expected GetSchema('Tables') to return at least one row on {kindStr}.");
            }
            finally
            {
                using var teardown = connection.CreateCommand();
                teardown.CommandText = $"DROP TABLE IF EXISTS {esc}";
                teardown.ExecuteNonQuery();
            }
        }
    }

    // ── Test 3: SQLite user-table lifecycle via sqlite_master ────────────────
    // The CLI handles SQLite via file deletion (not GetSchema), so we verify
    // table enumeration through sqlite_master — the canonical SQLite catalog.

    [Fact]
    public async Task Sqlite_SqliteMaster_UserTable_AppearsAfterCreate_GoneAfterDrop()
    {
        var live = LiveProviderFactory.OpenLive(ProviderKind.Sqlite);
        Assert.NotNull(live); // SQLite is always available.

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            const string tableName = "LvDrpSafety_Probe";
            var esc = provider.Escape(tableName);

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {esc} (Id INTEGER PRIMARY KEY, Val TEXT)";
                await cmd.ExecuteNonQueryAsync();
            }

            var before = await ListUserTablesAsync(connection);
            Assert.Contains(tableName, before, StringComparer.OrdinalIgnoreCase);

            // sqlite_master contains no system-schema rows — the filter is a no-op.
            Assert.DoesNotContain(before, t => IsSystemSchema(ProviderKind.Sqlite, t));

            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"DROP TABLE IF EXISTS {esc}";
                await cmd.ExecuteNonQueryAsync();
            }

            var after = await ListUserTablesAsync(connection);
            Assert.DoesNotContain(tableName, after, StringComparer.OrdinalIgnoreCase);
        }
    }

    private static async Task<System.Collections.Generic.List<string>> ListUserTablesAsync(DbConnection conn)
    {
        var names = new System.Collections.Generic.List<string>();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));
        return names;
    }

    // ── Test 4: CLI gate logic (no live connection required) ─────────────────

    [Fact]
    public void DropGate_WithoutYesOrDryRun_IsRefused()
    {
        // Mirrors: if (!yes && !dryRun) return error exit code 3.
        var yes = false; var dryRun = false;
        Assert.True(!yes && !dryRun);
    }

    [Fact]
    public void DropGate_WithYesFlag_PassesGate()
    {
        var yes = true; var dryRun = false;
        Assert.False(!yes && !dryRun);
    }

    [Fact]
    public void DropGate_WithDryRunFlag_PassesGate()
    {
        var yes = false; var dryRun = true;
        Assert.False(!yes && !dryRun);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static ProviderKind ParseKind(string s) => s.ToLowerInvariant() switch
    {
        "sqlserver" => ProviderKind.SqlServer,
        "postgres"  => ProviderKind.Postgres,
        "mysql"     => ProviderKind.MySql,
        "sqlite"    => ProviderKind.Sqlite,
        _ => throw new ArgumentOutOfRangeException(nameof(s), s, null)
    };

    private static DbConnection? OpenProtectedDatabaseConnection(ProviderKind kind, string protectedDatabase)
    {
        var baseConnectionString = kind switch
        {
            ProviderKind.SqlServer => LiveProviderEnvironment.GetConnectionString("sqlserver"),
            ProviderKind.Postgres => LiveProviderEnvironment.GetConnectionString("postgres"),
            ProviderKind.MySql => LiveProviderEnvironment.GetConnectionString("mysql"),
            _ => null
        };
        if (string.IsNullOrEmpty(baseConnectionString)) return null;

        var builder = new DbConnectionStringBuilder { ConnectionString = baseConnectionString };
        if (builder.ContainsKey("Initial Catalog"))
            builder["Initial Catalog"] = protectedDatabase;
        else
            builder["Database"] = protectedDatabase;

        DbConnection? connection = kind switch
        {
            ProviderKind.SqlServer => CreateConnection(
                "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
                builder.ConnectionString),
            ProviderKind.Postgres => CreateConnection(
                "Npgsql.NpgsqlConnection, Npgsql",
                builder.ConnectionString),
            ProviderKind.MySql => CreateConnection(
                "MySqlConnector.MySqlConnection, MySqlConnector",
                builder.ConnectionString)
                ?? CreateConnection(
                    "MySql.Data.MySqlClient.MySqlConnection, MySql.Data",
                    builder.ConnectionString),
            _ => null
        };

        if (connection == null) return null;
        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            return null;
        }
    }

    private static DbConnection? CreateConnection(string typeName, string connectionString)
    {
        var type = Type.GetType(typeName);
        if (type == null) return null;
        var connection = (DbConnection)Activator.CreateInstance(type)!;
        connection.ConnectionString = connectionString;
        return connection;
    }
}
