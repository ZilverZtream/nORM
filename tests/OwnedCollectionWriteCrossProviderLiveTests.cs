using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An owned collection persists with replace semantics — an owner's Modified save reconciles the owned child
/// table against the in-memory collection (delete orphans, re-insert). Existing live coverage only inserts
/// an owner with children once and then reads it back; this exercises the WRITE reconciliation on an already
/// persisted owner (remove/modify/add, and a multi-save snapshot re-capture) on every configured live
/// server, comparing the raw child table to a reference multiset so a dialect-specific delete/re-insert
/// defect surfaces as a dropped or duplicated row.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class OwnedCollectionWriteCrossProviderLiveTests
{
    [Table("OwxOrder")]
    private class Order
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    private class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static (string oTable, string lTable, string lineDdl, string orderDdl) Ddl(string kind)
    {
        var lineId = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var lineDdl = kind == "postgres"
            ? $"{lineId}, \"OrderId\" INT NOT NULL, \"Val\" INT NOT NULL"
            : $"{lineId}, OrderId INT NOT NULL, Val INT NOT NULL";
        var orderDdl = kind == "postgres"
            ? "\"Id\" INT PRIMARY KEY, \"Name\" VARCHAR(50) NOT NULL"
            : "Id INT PRIMARY KEY, Name VARCHAR(50) NOT NULL";
        return (kind == "postgres" ? "\"OwxOrder\"" : "OwxOrder",
                kind == "postgres" ? "\"OwxLine\"" : "OwxLine", lineDdl, orderDdl);
    }

    private static DbContext Ctx(Func<DbConnection> factory, DatabaseProvider provider) =>
        new DbContext(factory(), provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "OwxLine", foreignKey: "OrderId")
        });

    private static Order Load(DbContext ctx) =>
        ((INormQueryable<Order>)ctx.Query<Order>()).Include(o => o.Lines).ToList().Single();

    private static List<int> ChildVals(Func<DbConnection> factory, string lTable, string col)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {col} FROM {lTable}";
        using var r = cmd.ExecuteReader();
        var v = new List<int>();
        while (r.Read()) v.Add(Convert.ToInt32(r.GetValue(0)));
        return v;
    }

    private static List<int> Sorted(IEnumerable<int> xs) => xs.OrderBy(x => x).ToList();

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Owned_replace_edit_persists_exact_set_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var (oTable, lTable, lineDdl, orderDdl) = Ddl(kind);
        var valCol = kind == "postgres" ? "\"Val\"" : "Val";

        Exec(factory!, $"DROP TABLE IF EXISTS {lTable}");
        Exec(factory!, $"DROP TABLE IF EXISTS {oTable}");
        Exec(factory!, $"CREATE TABLE {oTable} ({orderDdl})");
        Exec(factory!, $"CREATE TABLE {lTable} ({lineDdl})");
        try
        {
            using (var ctx = Ctx(factory!, provider!))
            {
                ctx.Add(new Order { Id = 1, Name = "o1", Lines = { new Line { Val = 10 }, new Line { Val = 20 }, new Line { Val = 30 } } });
                await ctx.SaveChangesAsync();
            }
            Assert.Equal(new[] { 10, 20, 30 }, Sorted(ChildVals(factory!, lTable, valCol)));

            using (var ctx = Ctx(factory!, provider!))
            {
                var order = Load(ctx);
                order.Lines.RemoveAll(l => l.Val == 20); // orphan-delete
                order.Lines.Single(l => l.Val == 10).Val = 100; // modify wins
                order.Lines.Add(new Line { Val = 40 }); // insert
                await ctx.SaveChangesAsync();
            }
            Assert.Equal(new[] { 30, 40, 100 }, Sorted(ChildVals(factory!, lTable, valCol)));
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {lTable}");
            Exec(factory!, $"DROP TABLE IF EXISTS {oTable}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Owned_multi_save_recaptures_snapshot_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var (oTable, lTable, lineDdl, orderDdl) = Ddl(kind);
        var valCol = kind == "postgres" ? "\"Val\"" : "Val";

        Exec(factory!, $"DROP TABLE IF EXISTS {lTable}");
        Exec(factory!, $"DROP TABLE IF EXISTS {oTable}");
        Exec(factory!, $"CREATE TABLE {oTable} ({orderDdl})");
        Exec(factory!, $"CREATE TABLE {lTable} ({lineDdl})");
        try
        {
            // One long-lived context: three sequential edit+save cycles must each reconcile against what the
            // prior save persisted, not a stale snapshot.
            using var ctx = Ctx(factory!, provider!);
            ctx.Add(new Order { Id = 1, Name = "o1", Lines = { new Line { Val = 1 } } });
            await ctx.SaveChangesAsync();
            var order = Load(ctx);

            order.Lines.Add(new Line { Val = 2 });
            await ctx.SaveChangesAsync();
            Assert.Equal(new[] { 1, 2 }, Sorted(ChildVals(factory!, lTable, valCol)));

            order.Lines.RemoveAll(l => l.Val == 1);
            await ctx.SaveChangesAsync();
            Assert.Equal(new[] { 2 }, Sorted(ChildVals(factory!, lTable, valCol)));

            order.Lines.Single(l => l.Val == 2).Val = 22;
            order.Lines.Add(new Line { Val = 3 });
            await ctx.SaveChangesAsync();
            Assert.Equal(new[] { 3, 22 }, Sorted(ChildVals(factory!, lTable, valCol)));

            // A no-op save must not disturb the persisted set.
            await ctx.SaveChangesAsync();
            Assert.Equal(new[] { 3, 22 }, Sorted(ChildVals(factory!, lTable, valCol)));
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {lTable}");
            Exec(factory!, $"DROP TABLE IF EXISTS {oTable}");
        }
    }
}
