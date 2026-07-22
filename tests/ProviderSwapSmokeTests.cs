using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
[Xunit.Trait("Category", TestCategory.ProviderParity)]
public class ProviderSwapSmokeTests
{
    [Table("PS_Customer")]
    private sealed class PsCustomer
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
        public string Email { get; set; } = "";
        public bool IsActive { get; set; }
    }

    [Table("PS_Order")]
    private sealed class PsOrder
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int TenantId { get; set; }
        public int CustomerId { get; set; }
        public decimal Total { get; set; }
        public string Label { get; set; } = "";
    }

    [Table("CQ_TakeUser")]
    private sealed class CqTakeUser
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task SameFeatureWorkflow_RunsAcrossProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        {
            ResetSchema(cn!, kind);

            var tenantOptions = new DbContextOptions
            {
                TenantProvider = new FixedTenantProvider(1),
                TenantColumnName = "TenantId"
            };

            // Tenant-2 ("foreign") fixture data is seeded through an unscoped admin context (no tenant
            // provider): under fail-closed tenant validation a tenant-1 context cannot insert another
            // tenant's rows. These exist only to prove tenant-1 isolation below.
            await using (var admin = new DbContext(cn!, provider!, new DbContextOptions(), ownsConnection: false))
            {
                admin.Add(new PsCustomer { Id = 2, TenantId = 2, Name = "Other", Email = "ops@other.test", IsActive = true });
                admin.Add(new PsOrder { Id = 20, TenantId = 2, CustomerId = 2, Total = 999m, Label = "foreign" });
                await admin.SaveChangesAsync();
            }

            await using (var ctx = new DbContext(cn!, provider!, tenantOptions, ownsConnection: false))
            {
                ctx.Add(new PsCustomer { Id = 1, TenantId = 1, Name = "Acme", Email = "ops@acme.test", IsActive = true });
                ctx.Add(new PsOrder { Id = 10, TenantId = 1, CustomerId = 1, Total = 25.50m, Label = "seed-a" });
                ctx.Add(new PsOrder { Id = 11, TenantId = 1, CustomerId = 1, Total = 75.25m, Label = "seed-b" });
                await ctx.SaveChangesAsync();

                var visibleCustomers = ctx.Query<PsCustomer>().OrderBy(x => x.Id).ToList();
                Assert.Single(visibleCustomers);
                Assert.Equal("Acme", visibleCustomers[0].Name);

                var compiled = Norm.CompileQuery((DbContext c, decimal minimum) =>
                    c.Query<PsOrder>().Where(x => x.Total >= minimum).OrderBy(x => x.Id));
                var compiledRows = await compiled(ctx, 50m);
                Assert.Single(compiledRows);
                Assert.Equal(11, compiledRows[0].Id);

                var rawRows = await ctx.FromSqlRawAsync<PsOrder>(
                    LiveProviderSql.Normalize(cn!, "SELECT Id, TenantId, CustomerId, Total, Label FROM PS_Order WHERE Id = 10"));
                Assert.Single(rawRows);
                Assert.Equal(25.50m, rawRows[0].Total);
            }

            await using (var ctx = new DbContext(cn!, provider!, null, ownsConnection: false))
            {
                await ctx.BulkInsertAsync(new[]
                {
                    new PsOrder { Id = 30, TenantId = 1, CustomerId = 1, Total = 10m, Label = "bulk-a" },
                    new PsOrder { Id = 31, TenantId = 1, CustomerId = 1, Total = 20m, Label = "bulk-b" }
                });

                await ctx.BulkUpdateAsync(new[]
                {
                    new PsOrder { Id = 30, TenantId = 1, CustomerId = 1, Total = 15m, Label = "bulk-a-updated" },
                    new PsOrder { Id = 31, TenantId = 1, CustomerId = 1, Total = 25m, Label = "bulk-b-updated" }
                });

                Assert.Equal(15m, Convert.ToDecimal(ReadScalar(cn!, "SELECT Total FROM PS_Order WHERE Id = 30")));

                await ctx.BulkDeleteAsync(new[]
                {
                    new PsOrder { Id = 31, TenantId = 1, CustomerId = 1, Total = 25m, Label = "bulk-b-updated" }
                });
                Assert.Equal(0L, CountRows(cn!, "PS_Order", "Id = 31"));
            }

            await using (var ctx = new DbContext(cn!, provider!, null, ownsConnection: false))
            {
                await using var tx = await ctx.Database.BeginTransactionAsync();
                Exec(cn!, "INSERT INTO PS_Order (Id, TenantId, CustomerId, Total, Label) VALUES (40, 1, 1, 1.00, 'rolled-back')", tx.Transaction);
                await tx.RollbackAsync();
                Assert.Equal(0L, CountRows(cn!, "PS_Order", "Id = 40"));
            }
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task CompiledQuery_TakeParameter_RunsAcrossProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!, null, ownsConnection: false))
        {
            ResetCompiledTakeSchema(cn!, kind);

            ctx.Add(new CqTakeUser { Id = 1, Name = "alpha", IsActive = true });
            ctx.Add(new CqTakeUser { Id = 2, Name = "beta", IsActive = true });
            ctx.Add(new CqTakeUser { Id = 3, Name = "inactive", IsActive = false });
            await ctx.SaveChangesAsync();

            var compiled = Norm.CompileQuery<nORM.Core.DbContext, int, CqTakeUser>(
                (c, take) => c.Query<CqTakeUser>()
                    .Where(u => u.IsActive == true)
                    .OrderBy(u => u.Id)
                    .Take(take));

            var two = await compiled(ctx, 2);
            Assert.Equal(new[] { 1, 2 }, two.Select(u => u.Id).ToArray());

            var one = await compiled(ctx, 1);
            Assert.Equal(new[] { 1 }, one.Select(u => u.Id).ToArray());
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    public async Task PreparedInsert_StringColumns_RunsAcrossProviders(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn) await using (var ctx = new DbContext(cn!, provider!, null, ownsConnection: false))
        {
            ResetCompiledTakeSchema(cn!, kind);

            await ctx.InsertAsync(new CqTakeUser { Id = 1, Name = "alpha", IsActive = true });
            await ctx.InsertAsync(new CqTakeUser { Id = 2, Name = "beta", IsActive = false });

            Assert.Equal(2L, CountRows(cn!, "CQ_TakeUser"));
            var rows = ctx.Query<CqTakeUser>().OrderBy(x => x.Id).ToList();
            Assert.Equal(new[] { "alpha", "beta" }, rows.Select(x => x.Name).ToArray());
        }
    }

    private static (DbConnection? Cn, DatabaseProvider? Provider, string? SkipReason) OpenLive(string kind)
    {
        if (kind == "sqlite")
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            return (cn, new SqliteProvider(), null);
        }

        var cs = LiveProviderEnvironment.GetConnectionString(kind);
        if (string.IsNullOrEmpty(cs))
            return (null, null, $"NORM_TEST_{kind.ToUpperInvariant()} not set.");

        return kind switch
        {
            "sqlserver" => (OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs), new SqlServerProvider(), null),
            "postgres" => (OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs), new PostgresProvider(new SqliteParameterFactory()), null),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
    }

    private static DbConnection OpenReflected(string typeName, string cs)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the driver package is installed.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static void ResetSchema(DbConnection cn, string kind)
    {
        if (kind == "sqlserver")
        {
            Exec(cn, "IF OBJECT_ID('PS_Order','U') IS NOT NULL DROP TABLE PS_Order");
            Exec(cn, "IF OBJECT_ID('PS_Customer','U') IS NOT NULL DROP TABLE PS_Customer");
        }
        else
        {
            Exec(cn, "DROP TABLE IF EXISTS PS_Order");
            Exec(cn, "DROP TABLE IF EXISTS PS_Customer");
        }

        Exec(cn, kind switch
        {
            "sqlite" => "CREATE TABLE PS_Customer (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, Email TEXT NOT NULL, IsActive INTEGER NOT NULL)",
            "sqlserver" => "CREATE TABLE PS_Customer (Id INT PRIMARY KEY, TenantId INT NOT NULL, Name NVARCHAR(200) NOT NULL, Email NVARCHAR(200) NOT NULL, IsActive BIT NOT NULL)",
            "postgres" => "CREATE TABLE PS_Customer (Id INT PRIMARY KEY, TenantId INT NOT NULL, Name VARCHAR(200) NOT NULL, Email VARCHAR(200) NOT NULL, IsActive BOOLEAN NOT NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        });

        Exec(cn, kind switch
        {
            "sqlite" => "CREATE TABLE PS_Order (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, CustomerId INTEGER NOT NULL, Total REAL NOT NULL, Label TEXT NOT NULL)",
            "sqlserver" => "CREATE TABLE PS_Order (Id INT PRIMARY KEY, TenantId INT NOT NULL, CustomerId INT NOT NULL, Total DECIMAL(18,4) NOT NULL, Label NVARCHAR(200) NOT NULL)",
            "postgres" => "CREATE TABLE PS_Order (Id INT PRIMARY KEY, TenantId INT NOT NULL, CustomerId INT NOT NULL, Total DECIMAL(18,4) NOT NULL, Label VARCHAR(200) NOT NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        });
    }

    private static void ResetCompiledTakeSchema(DbConnection cn, string kind)
    {
        var table = LiveProviderSql.Identifier(cn, "CQ_TakeUser");
        var id = LiveProviderSql.Identifier(cn, "Id");
        var name = LiveProviderSql.Identifier(cn, "Name");
        var isActive = LiveProviderSql.Identifier(cn, "IsActive");

        if (kind == "sqlserver")
            Exec(cn, "IF OBJECT_ID('CQ_TakeUser','U') IS NOT NULL DROP TABLE CQ_TakeUser");
        else
            Exec(cn, $"DROP TABLE IF EXISTS {table}");

        Exec(cn, kind switch
        {
            "sqlite" => $"CREATE TABLE {table} ({id} INTEGER PRIMARY KEY, {name} TEXT NOT NULL, {isActive} INTEGER NOT NULL)",
            "sqlserver" => $"CREATE TABLE {table} ({id} INT PRIMARY KEY, {name} NVARCHAR(200) NOT NULL, {isActive} BIT NOT NULL)",
            "postgres" => $"CREATE TABLE {table} ({id} INT PRIMARY KEY, {name} VARCHAR(200) NOT NULL, {isActive} BOOLEAN NOT NULL)",
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        });
    }

    private static void Exec(DbConnection cn, string sql, DbTransaction? tx = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = LiveProviderSql.Normalize(cn, sql);
        cmd.ExecuteNonQuery();
    }

    private static object? ReadScalar(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = LiveProviderSql.Normalize(cn, sql);
        return cmd.ExecuteScalar();
    }

    private static long CountRows(DbConnection cn, string table, string? predicate = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {LiveProviderSql.Identifier(cn, table)}" +
            (predicate == null ? string.Empty : " WHERE " + LiveProviderSql.Normalize(cn, predicate));
        return Convert.ToInt64(cmd.ExecuteScalar());
    }
}
