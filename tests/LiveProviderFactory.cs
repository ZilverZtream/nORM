using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Providers;

namespace nORM.Tests;

/// <summary>
/// Opens a real provider connection when the corresponding <c>NORM_TEST_*</c> environment
/// variable is configured, otherwise returns null. Tests that need server semantics call
/// <see cref="OpenLive"/> and early-return when the connection is unavailable; the release
/// gate enforces minimum configured providers separately via <c>NORM_MIN_LIVE_PROVIDERS</c>.
///
/// Distinct from <see cref="TestBase.CreateProvider"/>, which returns a SQLite-backed
/// dialect-only provider for cross-dialect SQL-shape tests. Use that path for SQL text shape
/// tests; use this helper when you must observe real server behavior (transactions, identity
/// retrieval, type coercion, JSON, savepoints, etc.).
/// </summary>
internal static class LiveProviderFactory
{
    public static (DbConnection Connection, DatabaseProvider Provider)? OpenLive(ProviderKind kind)
    {
        var connectionString = kind switch
        {
            ProviderKind.SqlServer => LiveProviderEnvironment.GetConnectionString("sqlserver"),
            ProviderKind.Postgres => LiveProviderEnvironment.GetConnectionString("postgres"),
            ProviderKind.MySql => LiveProviderEnvironment.GetConnectionString("mysql"),
            ProviderKind.Sqlite => "Data Source=:memory:",
            _ => null
        };
        if (string.IsNullOrEmpty(connectionString)) return null;

        DbConnection connection;
        DatabaseProvider provider;
        try
        {
            switch (kind)
            {
                case ProviderKind.SqlServer:
                    connection = CreateProviderConnection(
                        "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
                        connectionString);
                    provider = new SqlServerProvider();
                    break;
                case ProviderKind.Postgres:
                    connection = CreateProviderConnection(
                        "Npgsql.NpgsqlConnection, Npgsql",
                        connectionString);
                    provider = new PostgresProvider();
                    break;
                case ProviderKind.MySql:
                    connection = CreateProviderConnection(
                        "MySqlConnector.MySqlConnection, MySqlConnector",
                        connectionString)
                        ?? CreateProviderConnection(
                            "MySql.Data.MySqlClient.MySqlConnection, MySql.Data",
                            connectionString)!;
                    provider = new MySqlProvider();
                    break;
                case ProviderKind.Sqlite:
                    connection = new SqliteConnection(connectionString);
                    provider = new SqliteProvider();
                    break;
                default:
                    return null;
            }
            connection.Open();
            return (connection, provider);
        }
        catch
        {
            return null;
        }
    }

    private static DbConnection CreateProviderConnection(string typeName, string connectionString)
    {
        var type = Type.GetType(typeName);
        if (type == null) return null!;
        var instance = (DbConnection)Activator.CreateInstance(type)!;
        instance.ConnectionString = connectionString;
        return instance;
    }

    /// <summary>True when the given provider has a configured live connection in the environment.</summary>
    public static bool IsConfigured(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => LiveProviderEnvironment.IsConfigured("sqlserver"),
        ProviderKind.Postgres => LiveProviderEnvironment.IsConfigured("postgres"),
        ProviderKind.MySql => LiveProviderEnvironment.IsConfigured("mysql"),
        ProviderKind.Sqlite => true,
        _ => false
    };
}
