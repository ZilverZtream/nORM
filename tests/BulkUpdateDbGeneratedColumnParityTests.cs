using System;
using System.Collections.Generic;
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
/// Bulk-updating a row must not clobber a DB-generated NON-key column. The SqlServer/MySQL
/// temp-table update built its SET clause from every non-key column (including DB-generated ones)
/// but staged only non-DB-generated columns, so the DB-generated column sat NULL in the staging
/// table and the UPDATE join wrote that NULL back over the committed value — silent data loss.
/// The correct SET set is UpdateColumns (non-key, non-timestamp, non-db-generated). SQLite/Postgres
/// stage the in-memory value and were already safe for loaded entities.
/// </summary>
[Xunit.Trait("Category", "ProviderParity")]
[Xunit.Trait("Category", "BulkProviderParity")]
public class BulkUpdateDbGeneratedColumnParityTests
{
    [Table("BukGenCol")]
    public class BukGenCol
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        // DB-generated non-key column: nORM omits it on INSERT, the DB DEFAULT fills it (777),
        // and it must survive a bulk update that only touches Name.
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int? Tag { get; set; }
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
        "sqlite" => "CREATE TABLE BukGenCol (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Tag INT DEFAULT 777)",
        "sqlserver" => "IF OBJECT_ID('BukGenCol','U') IS NOT NULL DROP TABLE BukGenCol; CREATE TABLE BukGenCol (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(200) NOT NULL, Tag INT DEFAULT 777)",
        "mysql" => "DROP TABLE IF EXISTS BukGenCol; CREATE TABLE BukGenCol (Id INT AUTO_INCREMENT PRIMARY KEY, Name VARCHAR(200) NOT NULL, Tag INT DEFAULT 777)",
        "postgres" => "DROP TABLE IF EXISTS \"BukGenCol\"; CREATE TABLE \"BukGenCol\" (\"Id\" SERIAL PRIMARY KEY, \"Name\" VARCHAR(200) NOT NULL, \"Tag\" INT DEFAULT 777)",
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
    public async Task BulkUpdate_preserves_db_generated_non_key_columns(string kind)
    {
        var (cn, provider, skip) = Open(kind);
        if (skip != null) { Assert.True(true, skip); return; } // skip unconfigured providers
        using var _cn = cn!;
        Exec(cn!, CreateDdl(kind));

        await using var ctx = new DbContext(cn!, provider!);

        // Insert three rows; Tag is DB-generated, so the DB DEFAULT (777) fills it.
        for (int i = 1; i <= 3; i++)
            await ctx.InsertAsync(new BukGenCol { Name = "n" + i });

        var items = ctx.Query<BukGenCol>().OrderBy(x => x.Id).ToList();
        Assert.Equal(3, items.Count);
        Assert.All(items, it => Assert.Equal(777, it.Tag)); // DB default landed and hydrated back
        foreach (var it in items) it.Name = it.Name + "-x"; // change ONLY a normal column

        var affected = await ctx.BulkUpdateAsync(items);
        Assert.Equal(3, affected);

        // Read ACTUAL persisted values via raw SQL — the DB-generated Tag must be unchanged (777),
        // not NULLed by the bulk update; Name must reflect the update.
        var rows = ReadRows(cn!, kind);
        Assert.Equal(new[] { "n1-x", "n2-x", "n3-x" }, rows.Select(r => r.Name).ToArray());
        Assert.All(rows, r =>
        {
            Assert.False(r.Tag is DBNull, "DB-generated Tag column was NULLed by BulkUpdate (silent data loss).");
            Assert.Equal(777, Convert.ToInt32(r.Tag));
        });
    }

    private static List<(string Name, object Tag)> ReadRows(DbConnection cn, string kind)
    {
        var sql = kind == "postgres"
            ? "SELECT \"Name\", \"Tag\" FROM \"BukGenCol\" ORDER BY \"Id\""
            : "SELECT Name, Tag FROM BukGenCol ORDER BY Id";
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var list = new List<(string, object)>();
        while (r.Read()) list.Add((r.GetString(0), r.GetValue(1)));
        return list.ToArray().Select(t => (t.Item1, t.Item2)).ToList();
    }
}
