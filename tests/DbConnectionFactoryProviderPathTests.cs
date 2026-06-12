using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class DbConnectionFactoryProviderPathTests
{
    [Fact]
    public void Create_PostgresProvider_HandlesInstalledOrMissingNpgsql()
    {
        try
        {
            using var connection = DbConnectionFactory.Create(
                "Host=localhost;Database=test;", new PostgresProvider(new SqliteParameterFactory()));
            Assert.Contains("Npgsql", connection.GetType().FullName, StringComparison.OrdinalIgnoreCase);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Contains("Npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Create_MySqlProvider_HandlesInstalledOrMissingMySqlConnector()
    {
        try
        {
            using var connection = DbConnectionFactory.Create(
                "Server=localhost;Database=test;", new MySqlProvider(new SqliteParameterFactory()));
            Assert.Contains("MySql", connection.GetType().FullName, StringComparison.OrdinalIgnoreCase);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}

//             + ExecuteReader/NonQuery routes through _context (interceptor path)
// Covers: PostgresMigrationRunner lines 279-289 (DisposeAsync with context),
//         SqlServerMigrationRunner lines 293-304 (DisposeAsync with context),
//         ExecuteNonQueryAsync/ExecuteReaderAsync when _context != null
