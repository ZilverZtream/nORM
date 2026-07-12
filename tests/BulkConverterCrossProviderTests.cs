using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Every provider's bulk insert/update path must apply the column's <see cref="IValueConverter"/>
/// (ConvertToProvider) before writing, exactly like the non-bulk paths. Binding the raw model value
/// and then applying ConvertFromProvider on read silently corrupts converter-mapped columns. Verified
/// live against SqlServer/MySQL/Postgres where configured (a negating int converter is type-safe for
/// the typed bulk-copy channels).
/// </summary>
[Xunit.Trait("Category", "LiveProvider")]
public class BulkConverterCrossProviderTests
{
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    [Table("bcvrow")]
    private class BcvRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    private static DbConnection? Open(string kind)
    {
        var cs = LiveProviderEnvironment.GetConnectionString(kind);
        if (string.IsNullOrEmpty(cs)) return null;
        var typeName = kind switch
        {
            "sqlserver" => "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
            "mysql" => "MySqlConnector.MySqlConnection, MySqlConnector",
            "postgres" => "Npgsql.NpgsqlConnection, Npgsql",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var type = Type.GetType(typeName) ?? throw new InvalidOperationException($"Driver '{typeName}' not loaded.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static DatabaseProvider ProviderFor(string kind) => kind switch
    {
        "sqlserver" => new SqlServerProvider(),
        "mysql" => new MySqlProvider(new SqliteParameterFactory()),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, sql); // quotes Id/Score per provider (Postgres/MySQL)
        cmd.ExecuteNonQuery();
    }

    private static int RawScore(DbConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, $"SELECT Score FROM bcvrow WHERE Id = {id}");
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkInsert_applies_value_converter(string kind)
    {
        using var cn = Open(kind);
        if (cn == null) return; // provider not configured — skip

        Exec(cn, "DROP TABLE IF EXISTS bcvrow");
        Exec(cn, "CREATE TABLE bcvrow (Id INT PRIMARY KEY, Score INT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BcvRow>().Property(r => r.Score).HasConversion(new NegatingConverter())
        };
        using var ctx = new DbContext(cn, ProviderFor(kind), opts);

        await ctx.BulkInsertAsync(new[] { new BcvRow { Id = 1, Score = 100 } });

        // Converter must run on write: DB stores -100; the round-trip yields 100.
        Assert.Equal(-100, RawScore(cn, 1));                   // BUG: 100 — raw model value stored
        var read = ctx.Query<BcvRow>().First(r => r.Id == 1);
        Assert.Equal(100, read.Score);                         // BUG: -100 — ConvertFromProvider applied to un-converted value

        Exec(cn, "DROP TABLE IF EXISTS bcvrow");
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task BulkInsert_large_batch_applies_value_converter(string kind)
    {
        using var cn = Open(kind);
        if (cn == null) return; // provider not configured — skip

        Exec(cn, "DROP TABLE IF EXISTS bcvrow");
        Exec(cn, "CREATE TABLE bcvrow (Id INT PRIMARY KEY, Score INT NOT NULL)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<BcvRow>().Property(r => r.Score).HasConversion(new NegatingConverter())
        };
        using var ctx = new DbContext(cn, ProviderFor(kind), opts);

        // 600 rows exceeds SqlServer's small-batch threshold, exercising the SqlBulkCopy / MySqlBulkCopy
        // / COPY channels (not just the batched-INSERT fallback).
        var rows = Enumerable.Range(1, 600).Select(i => new BcvRow { Id = i, Score = i }).ToArray();
        await ctx.BulkInsertAsync(rows);

        // The bulk-copy channel must have applied the converter to every row (DB stores -Id).
        Assert.Equal(-1, RawScore(cn, 1));
        Assert.Equal(-300, RawScore(cn, 300));
        Assert.Equal(-600, RawScore(cn, 600));
        // Read-side conversion (ConvertFromProvider) is covered by the single-row test above.

        Exec(cn, "DROP TABLE IF EXISTS bcvrow");
    }
}
