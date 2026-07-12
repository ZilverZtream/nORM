using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Bulk-inserting entities whose only column is a DB-generated identity key must insert one row
/// per entity via DEFAULT VALUES. Postgres returned 0 without inserting anything (a silent no-op),
/// because its COPY / multi-row VALUES path cannot express a zero-column insert.
/// </summary>
[Xunit.Trait("Category", "ProviderParity")]
[Xunit.Trait("Category", "BulkProviderParity")]
public class BulkInsertAllGeneratedParityTests
{
    [Table("BagAutoOnly")]
    public class BagAutoOnly
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
    }

    private static (DbConnection? Cn, DatabaseProvider? Provider, string? Skip) Open(string kind) => kind switch
    {
        "sqlite" => (OpenSqlite(), new SqliteProvider(), null),
        "sqlserver" => LiveProviderEnvironment.GetConnectionString("sqlserver") is { Length: > 0 } cs
            ? (OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs), new SqlServerProvider(), null)
            : (null, null, "sqlserver not configured"),
        "mysql" => LiveProviderEnvironment.GetConnectionString("mysql") is { Length: > 0 } cs
            ? (OpenReflected("MySqlConnector.MySqlConnection, MySqlConnector", cs), new MySqlProvider(new SqliteParameterFactory()), null)
            : (null, null, "mysql not configured"),
        "postgres" => LiveProviderEnvironment.GetConnectionString("postgres") is { Length: > 0 } cs
            ? (OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs), new PostgresProvider(new SqliteParameterFactory()), null)
            : (null, null, "postgres not configured"),
        _ => (null, null, "unknown"),
    };

    private static DbConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static DbConnection OpenReflected(string typeName, string cs)
    {
        var type = Type.GetType(typeName) ?? throw new InvalidOperationException($"Driver '{typeName}' not loadable.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static string CreateDdl(string kind) => kind switch
    {
        "sqlite" => "CREATE TABLE BagAutoOnly (Id INTEGER PRIMARY KEY AUTOINCREMENT)",
        "sqlserver" => "IF OBJECT_ID('BagAutoOnly','U') IS NOT NULL DROP TABLE BagAutoOnly; CREATE TABLE BagAutoOnly (Id INT IDENTITY(1,1) PRIMARY KEY)",
        "mysql" => "DROP TABLE IF EXISTS BagAutoOnly; CREATE TABLE BagAutoOnly (Id INT AUTO_INCREMENT PRIMARY KEY)",
        "postgres" => "DROP TABLE IF EXISTS \"BagAutoOnly\"; CREATE TABLE \"BagAutoOnly\" (\"Id\" SERIAL PRIMARY KEY)",
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkInsert_all_generated_entity_inserts_a_row_per_entity(string kind)
    {
        var (cn, provider, skip) = Open(kind);
        if (skip != null) { Assert.True(true, skip); return; }
        using var _cn = cn!;
        Exec(cn!, CreateDdl(kind));

        await using var ctx = new DbContext(cn!, provider!);
        await ctx.BulkInsertAsync(new[] { new BagAutoOnly(), new BagAutoOnly(), new BagAutoOnly() });

        var count = ctx.Query<BagAutoOnly>().Count();
        Assert.Equal(3, count);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Insert_single_all_generated_entity_persists(string kind)
    {
        var (cn, provider, skip) = Open(kind);
        if (skip != null) { Assert.True(true, skip); return; }
        using var _cn = cn!;
        Exec(cn!, CreateDdl(kind));

        await using var ctx = new DbContext(cn!, provider!);
        var entity = new BagAutoOnly();
        await ctx.InsertAsync(entity);

        Assert.True(entity.Id > 0); // DB-generated key hydrated
        Assert.Equal(1, ctx.Query<BagAutoOnly>().Count());
    }
}
