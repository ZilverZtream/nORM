using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Providers;

namespace nORM.Tests;

internal sealed class SqliteParameterFactory : IDbParameterFactory
{
    public DbParameter CreateParameter(string name, object? value)
        => new SqliteParameter(name, value ?? DBNull.Value);
}
