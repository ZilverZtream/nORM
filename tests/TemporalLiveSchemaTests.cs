using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that IntrospectTableColumnsAsync returns live column info,
/// and that GenerateCreateHistoryTableSql uses live types when available.
/// </summary>
public class TemporalLiveSchemaTests
{
    [Table("TlsProduct")]
    private class TlsProduct
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    [Fact]
    public async Task IntrospectTableColumns_ReturnsLiveTypes()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE TlsProduct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price REAL NOT NULL)";
        cmd.ExecuteNonQuery();

        var provider = new SqliteProvider();
        var cols = await provider.IntrospectTableColumnsAsync(cn, "TlsProduct");

        Assert.Equal(3, cols.Count);
        var id = cols.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(id);
        Assert.Equal("INTEGER", id!.SqlType, StringComparer.OrdinalIgnoreCase);
        // SQLite's PRAGMA table_info reports notnull=0 for INTEGER PRIMARY KEY (implicit NOT NULL
        // is handled by the PRIMARY KEY constraint, not the NOT NULL flag in PRAGMA)

        var name = cols.FirstOrDefault(c => c.Name == "Name");
        Assert.NotNull(name);
        Assert.Equal("TEXT", name!.SqlType, StringComparer.OrdinalIgnoreCase);
        Assert.False(name.IsNullable);  // TEXT NOT NULL → notnull=1

        var price = cols.FirstOrDefault(c => c.Name == "Price");
        Assert.NotNull(price);
        Assert.Equal("REAL", price!.SqlType, StringComparer.OrdinalIgnoreCase);
        Assert.False(price.IsNullable);  // REAL NOT NULL → notnull=1
    }

    [Fact]
    public async Task IntrospectTableColumns_ReturnsEmpty_WhenTableAbsent()
    {
        using var cn = OpenMemory();
        var provider = new SqliteProvider();
        var cols = await provider.IntrospectTableColumnsAsync(cn, "NonExistentTable");
        Assert.Empty(cols);
    }

    [Fact]
    public void HistoryDdl_UsesLiveTypes_WhenProvided()
    {
        var provider = new SqliteProvider();
        var ctx = new DbContext(new SqliteConnection("Data Source=:memory:"), provider);
        var mapping = ctx.GetMapping(typeof(TlsProduct));

        // Provide custom live columns with different types
        var liveColumns = new List<DatabaseProvider.LiveColumnInfo>
        {
            new("Id", "INTEGER", false),
            new("Name", "TEXT", false),
            new("Price", "REAL", false),  // custom type matching live schema
        };

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);

        Assert.Contains("REAL", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TEXT", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INTEGER", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HistoryDdl_FallsBackToClrTypes_WhenLiveColumnsNull()
    {
        var provider = new SqliteProvider();
        var ctx = new DbContext(new SqliteConnection("Data Source=:memory:"), provider);
        var mapping = ctx.GetMapping(typeof(TlsProduct));

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, null);

        // Should still produce valid DDL with CLR-derived types
        Assert.Contains("CREATE TABLE", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__VersionId", ddl);
        Assert.Contains("__ValidFrom", ddl);
    }

    [Fact]
    public void HistoryDdl_FallsBackToClrTypes_WhenLiveColumnsEmpty()
    {
        var provider = new SqliteProvider();
        var ctx = new DbContext(new SqliteConnection("Data Source=:memory:"), provider);
        var mapping = ctx.GetMapping(typeof(TlsProduct));

        var ddl = provider.GenerateCreateHistoryTableSql(mapping, Array.Empty<DatabaseProvider.LiveColumnInfo>());

        Assert.Contains("CREATE TABLE", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HistoryDdl_ExecutesWithoutErrors_OnSqlite()
    {
        using var cn = OpenMemory();
        using var createCmd = cn.CreateCommand();
        createCmd.CommandText = "CREATE TABLE TlsProduct (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price REAL NOT NULL)";
        createCmd.ExecuteNonQuery();

        var provider = new SqliteProvider();
        var ctx = new DbContext(cn, provider);
        var mapping = ctx.GetMapping(typeof(TlsProduct));

        // Introspect live schema
        var liveColumns = await provider.IntrospectTableColumnsAsync(cn, "TlsProduct");

        // Generate history DDL using live columns
        var ddl = provider.GenerateCreateHistoryTableSql(mapping, liveColumns);

        // Execute DDL — should not throw
        using var ddlCmd = cn.CreateCommand();
        ddlCmd.CommandText = ddl;
        ddlCmd.ExecuteNonQuery();

        // Verify history table was created
        using var checkCmd = cn.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='TlsProduct_History'";
        var count = Convert.ToInt64(checkCmd.ExecuteScalar());
        Assert.Equal(1, count);
    }
}
