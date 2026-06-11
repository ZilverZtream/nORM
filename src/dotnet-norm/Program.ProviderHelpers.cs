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

    static int Fail(Exception ex, int exitCode = 1)
    {
        Console.Error.WriteLine($"Error: {RedactConnectionStrings(ex.Message)}");
        return exitCode;
    }
}
