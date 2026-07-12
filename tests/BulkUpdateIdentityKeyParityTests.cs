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
/// Bulk-updating rows whose primary key is a DB-generated identity must actually update them.
/// The SqlServer/MySQL temp-table staging excluded DB-generated columns, so the identity key was
/// never staged and the UPDATE join (target.key = staging.key) matched nothing — a silent 0-row
/// lost update. SQLite/Postgres were already correct.
/// </summary>
[Xunit.Trait("Category", "ProviderParity")]
[Xunit.Trait("Category", "BulkProviderParity")]
public class BulkUpdateIdentityKeyParityTests
{
    [Table("BukIdent")]
    public class BukIdent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
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
        "sqlite" => "CREATE TABLE BukIdent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INT NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('BukIdent','U') IS NOT NULL DROP TABLE BukIdent; CREATE TABLE BukIdent (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "mysql" => "DROP TABLE IF EXISTS BukIdent; CREATE TABLE BukIdent (Id INT AUTO_INCREMENT PRIMARY KEY, Name VARCHAR(200) NOT NULL, Value INT NOT NULL)",
        "postgres" => "DROP TABLE IF EXISTS \"BukIdent\"; CREATE TABLE \"BukIdent\" (\"Id\" SERIAL PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL, \"Value\" INT NOT NULL)",
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
    public async Task BulkUpdate_identity_keyed_rows_actually_updates(string kind)
    {
        var (cn, provider, skip) = Open(kind);
        if (skip != null) { Assert.True(true, skip); return; } // skip unconfigured providers
        using var _cn = cn!;
        Exec(cn!, CreateDdl(kind));

        await using var ctx = new DbContext(cn!, provider!);

        // Insert three rows; identity keys are DB-assigned and hydrated back.
        for (int i = 1; i <= 3; i++)
            await ctx.InsertAsync(new BukIdent { Name = "n" + i, Value = i });

        var items = ctx.Query<BukIdent>().OrderBy(x => x.Id).ToList();
        Assert.Equal(3, items.Count);
        Assert.All(items, it => Assert.True(it.Id > 0));
        foreach (var it in items) it.Value += 100;

        var affected = await ctx.BulkUpdateAsync(items);

        // Read the ACTUAL persisted values via raw SQL — Query<T> would return the tracked
        // (in-memory, already-modified) instances and mask a lost update.
        Assert.Equal(new[] { 101, 102, 103 }, ReadValues(cn!, kind));
        Assert.Equal(3, affected);
    }

    private static int[] ReadValues(DbConnection cn, string kind)
    {
        var sql = kind == "postgres"
            ? "SELECT \"Value\" FROM \"BukIdent\" ORDER BY \"Id\""
            : "SELECT Value FROM BukIdent ORDER BY Id";
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var list = new System.Collections.Generic.List<int>();
        while (r.Read()) list.Add(Convert.ToInt32(r.GetValue(0)));
        return list.ToArray();
    }
}
