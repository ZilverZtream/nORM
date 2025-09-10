using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Internal;
using nORM.Providers;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace nORM.Core;

internal static class DbConnectionFactory
{
    private static readonly ConcurrentDictionary<Type, Func<string, DbConnection>> _factories = new();

    /// <summary>
    /// Creates a provider-specific <see cref="DbConnection"/> instance for the supplied
    /// connection string and database provider.
    /// </summary>
    /// <param name="connectionString">The connection string used to create the connection.</param>
    /// <param name="provider">The provider describing the type of database to connect to.</param>
    /// <returns>An open <see cref="DbConnection"/> appropriate for the specified provider.</returns>
    /// <exception cref="ArgumentException">Thrown when the connection string is not valid for the provider.</exception>
    /// <exception cref="NotSupportedException">Thrown when the provider type is not supported.</exception>
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
                return CreateConnectionFactory(type);
            }
            if (typeof(MySqlProvider).IsAssignableFrom(t))
            {
                var type = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector") ??
                           Type.GetType("MySql.Data.MySqlClient.MySqlConnection, MySql.Data");
                if (type == null)
                    throw new InvalidOperationException("MySQL package is required for MySQL support. Please install MySqlConnector or MySql.Data.");
                return CreateConnectionFactory(type);
            }

            throw new NotSupportedException($"Unsupported provider type: {t.Name}");
        });

        return factory(connectionString);
    }

    private static Func<string, DbConnection> CreateConnectionFactory(Type connectionType)
    {
        var constructor = connectionType.GetConstructor(new[] { typeof(string) });
        if (constructor == null)
            throw new InvalidOperationException($"{connectionType.Name} must have a constructor with a single string parameter.");

        var csParam = Expression.Parameter(typeof(string), "cs");
        var newExpr = Expression.New(constructor, csParam);
        return Expression.Lambda<Func<string, DbConnection>>(newExpr, csParam).Compile();
    }
}

