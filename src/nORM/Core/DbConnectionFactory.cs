using System;
using System.Collections.Concurrent;
using System.Data.Common;
using nORM.Internal;
using nORM.Providers;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace nORM.Core;

internal static class DbConnectionFactory
{
    private static readonly ConcurrentDictionary<Type, Func<string, DbConnection>> _factories = new();

    public static DbConnection Create(string connectionString, DatabaseProvider provider)
    {
        var providerName = provider switch
        {
            SqlServerProvider => "sqlserver",
            SqliteProvider => "sqlite",
            PostgresProvider => "postgres",
            MySqlProvider => "mysql",
            _ => provider.GetType().Name
        };

        NormValidator.ValidateConnectionString(connectionString, providerName);

        var factory = _factories.GetOrAdd(provider.GetType(), t =>
        {
            if (t == typeof(SqlServerProvider))
                return cs => new SqlConnection(cs);
            if (t == typeof(SqliteProvider))
                return cs => new SqliteConnection(cs);
            if (typeof(PostgresProvider).IsAssignableFrom(t))
            {
                var type = Type.GetType("Npgsql.NpgsqlConnection, Npgsql");
                if (type == null)
                    throw new InvalidOperationException("Npgsql package is required for PostgreSQL support. Please install the Npgsql NuGet package.");
                return cs => (DbConnection)Activator.CreateInstance(type, cs)!;
            }
            if (typeof(MySqlProvider).IsAssignableFrom(t))
            {
                var type = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") ??
                           Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data");
                if (type == null)
                    throw new InvalidOperationException("MySQL package is required for MySQL support. Please install MySqlConnector or MySql.Data.");
                return cs => (DbConnection)Activator.CreateInstance(type, cs)!;
            }

            throw new NotSupportedException($"Unsupported provider type: {t.Name}");
        });

        return factory(connectionString);
    }
}

