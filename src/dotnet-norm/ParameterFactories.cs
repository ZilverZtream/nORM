using System;
using System.Data.Common;
using nORM.Providers;

#nullable enable

namespace nORM.Providers
{
    internal sealed class NpgsqlParameterFactory : IDbParameterFactory
    {
        private readonly Type? _paramType = Type.GetType("Npgsql.NpgsqlParameter, Npgsql");

        public DbParameter CreateParameter(string name, object? value)
        {
            if (_paramType == null)
                throw new InvalidOperationException("Npgsql package is required for PostgreSQL support. Please install the Npgsql NuGet package.");
            return (DbParameter)Activator.CreateInstance(_paramType, name, value ?? DBNull.Value)!;
        }
    }

    internal sealed class MySqlParameterFactory : IDbParameterFactory
    {
        private readonly Type? _paramType =
            Type.GetType("MySqlConnector.MySqlParameter, MySqlConnector") ??
            Type.GetType("MySql.Data.MySqlClient.MySqlParameter, MySql.Data");

        public DbParameter CreateParameter(string name, object? value)
        {
            if (_paramType == null)
                throw new InvalidOperationException("MySQL package is required for MySQL support. Please install either MySqlConnector or MySql.Data.");
            return (DbParameter)Activator.CreateInstance(_paramType, name, value ?? DBNull.Value)!;
        }
    }
}
