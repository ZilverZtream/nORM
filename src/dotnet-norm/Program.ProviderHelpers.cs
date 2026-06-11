using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Providers;

partial class Program
{
    static DbConnection CreateConnection(string provider, string connectionString)
    {
        try
        {
            return NormalizeProviderName(provider) switch
            {
                "sqlserver" => new SqlConnection(connectionString),
                "sqlite" => new SqliteConnection(connectionString),
                "postgres" => CreateConnectionFromType(new[] { "Npgsql.NpgsqlConnection, Npgsql" }, "PostgreSQL", connectionString),
                "mysql" => CreateConnectionFromType(new[] { "MySql.Data.MySqlClient.MySqlConnection, MySql.Data", "MySqlConnector.MySqlConnection, MySqlConnector" }, "MySQL", connectionString),
                _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
            };
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or TypeLoadException)
        {
            throw new InvalidOperationException($"Failed to create connection: {ex.Message}", ex);
        }
    }

    static DbConnection CreateConnectionFromType(string[] typeNames, string friendly, string connString)
    {
        foreach (var name in typeNames)
        {
            var type = Type.GetType(name);
            if (type != null)
                return (DbConnection)Activator.CreateInstance(type, connString)!;
        }

        throw new InvalidOperationException($"{friendly} provider assembly not found. Ensure the appropriate package is referenced.");
    }

    static DatabaseProvider CreateProvider(string provider) =>
        NormalizeProviderName(provider) switch
        {
            "sqlserver" => new SqlServerProvider(),
            "sqlite" => new SqliteProvider(),
            "postgres" => new PostgresProvider(),
            "mysql" => new MySqlProvider(),
            _ => throw new ArgumentException($"Unsupported provider '{provider}'.")
        };

    static bool IsSystemSchema(string provider, string? schemaName)
    {
        if (string.IsNullOrWhiteSpace(schemaName))
            return false;

        var s = schemaName.Trim();
        // Universal system schemas present in multiple providers.
        if (s.Equals("sys", StringComparison.OrdinalIgnoreCase)
            || s.Equals("information_schema", StringComparison.OrdinalIgnoreCase))
            return true;

        return provider.ToLowerInvariant() switch
        {
            // PostgreSQL system schemas: pg_catalog, pg_toast, pg_temp_*, pg_toast_temp_*, pg_*.
            "postgres" or "postgresql" =>
                s.StartsWith("pg_", StringComparison.OrdinalIgnoreCase),
            // MySQL: the mysql system database tables appear under the "mysql" schema.
            "mysql" =>
                s.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                || s.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    static bool IsProtectedDatabaseName(string provider, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            return false;

        var normalized = databaseName.Trim();
        return provider.ToLowerInvariant() switch
        {
            "sqlserver" => normalized.Equals("master", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("model", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("msdb", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("tempdb", StringComparison.OrdinalIgnoreCase),
            "postgres" or "postgresql" => normalized.Equals("postgres", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("template0", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("template1", StringComparison.OrdinalIgnoreCase),
            "mysql" => normalized.Equals("mysql", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("sys", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("information_schema", StringComparison.OrdinalIgnoreCase)
                || normalized.Equals("performance_schema", StringComparison.OrdinalIgnoreCase),
            _ => false
        };
    }

    static int Fail(Exception ex, int exitCode = 1)
    {
        Console.Error.WriteLine($"Error: {RedactConnectionStrings(ex.Message)}");
        return exitCode;
    }
}
