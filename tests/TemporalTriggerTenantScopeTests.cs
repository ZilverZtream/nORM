using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A temporal trigger closes the prior open history row(s) for the mutated key. The history table is
/// not itself primary-key-unique (its key includes the validity range), so matching on the entity key
/// alone can close a DIFFERENT tenant's still-open history row. The close must also be scoped by the
/// tenant column, keeping temporal writes tenant-isolated like every other write path (SEC-MT).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TemporalTriggerTenantScopeTests
{
    private class TtRow
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void Update_trigger_closes_only_the_same_tenants_open_history_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider()); // default TenantColumnName = "TenantId"
        var mapping = ctx.GetMapping(typeof(TtRow));
        Assert.NotNull(mapping.TenantColumn);                      // TenantId is recognised as the tenant column
        Assert.DoesNotContain(mapping.KeyColumns, k => k.Name == "TenantId"); // and is not part of the key

        Exec(cn, "CREATE TABLE TtRow (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL)");
        Exec(cn, "CREATE TABLE TtRow_History (__ValidFrom TEXT, __ValidTo TEXT, __Operation TEXT, " +
                 "Id INTEGER, TenantId INTEGER, Name TEXT)");
        Exec(cn, new SqliteProvider().GenerateTemporalTriggersSql(mapping));

        // Tenant 1 has a live row Id=1 (the AFTER INSERT trigger opens its history row).
        Exec(cn, "INSERT INTO TtRow (Id, TenantId, Name) VALUES (1, 1, 'tenant1')");
        // Tenant 2 also has an OPEN history row for the same entity key (the main table can only hold
        // one live Id=1, but the history table accumulates rows and enforces no key uniqueness).
        Exec(cn, "INSERT INTO TtRow_History (__ValidFrom, __ValidTo, __Operation, Id, TenantId, Name) " +
                 "VALUES ('2020-01-01', '9999-12-31', 'I', 1, 2, 'tenant2')");

        // Update tenant 1's live row. Its UPDATE trigger must close only tenant 1's open history row.
        Exec(cn, "UPDATE TtRow SET Name = 'tenant1-v2' WHERE Id = 1");

        int OpenCount(int tenant)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM TtRow_History WHERE Id = 1 AND TenantId = @t AND __ValidTo = '9999-12-31'";
            cmd.Parameters.AddWithValue("@t", tenant);
            return Convert.ToInt32(cmd.ExecuteScalar());
        }

        Assert.Equal(1, OpenCount(2)); // BUG (before fix): 0 — tenant 2's history was wrongly closed
        Assert.Equal(1, OpenCount(1)); // tenant 1 has exactly one open row (its new version)
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    [InlineData("mysql")]
    public void All_providers_scope_the_history_close_by_tenant(string kind)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(TtRow));

        DatabaseProvider provider = kind switch
        {
            "sqlite" => new SqliteProvider(),
            "sqlserver" => new SqlServerProvider(),
            "postgres" => new PostgresProvider(new SqliteParameterFactory()),
            "mysql" => new MySqlProvider(new SqliteParameterFactory()),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        var sql = provider.GenerateTemporalTriggersSql(mapping);
        // The generated triggers reference the tenant column in the history-close predicate.
        Assert.Contains("TenantId", sql);
    }
}
