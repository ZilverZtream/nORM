using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Navigation subqueries — collection Count/Sum in a projection, Any in a predicate, and a safe-key ordered
/// First (a LIMIT-1 correlated subquery, which SQL Server must express with TOP / OFFSET-FETCH rather than
/// LIMIT) — carry real dialect risk. These were verified against a hand-resolved oracle on SQLite; this
/// pins the same results on every configured live server. An empty child set (order 3) exercises the
/// zero/NULL edges (COALESCE for Sum, NULL for First).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class NavigationCrossProviderLiveTests
{
    [Table("NcxOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public string Cust { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("NcxLine")]
    private class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    private static readonly Order[] Orders =
    {
        new Order { Id = 1, Cust = "ann" },
        new Order { Id = 2, Cust = "bob" },
        new Order { Id = 3, Cust = "cid" }, // no lines
    };
    private static readonly Line[] AllLines =
    {
        new Line { Id = 1, OrderId = 1, Sku = "aaa", Qty = 3 },
        new Line { Id = 2, OrderId = 1, Sku = "bbb", Qty = 1 },
        new Line { Id = 3, OrderId = 2, Sku = "ccc", Qty = 5 },
        new Line { Id = 4, OrderId = 2, Sku = "ddd", Qty = 1 },
    };
    private static IEnumerable<Line> LinesOf(Order o) => AllLines.Where(l => l.OrderId == o.Id);

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

    private void RunNav(string kind, Func<IQueryable<Order>, List<string>> normQuery, List<string> expected)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var oTable = kind == "postgres" ? "\"NcxOrder\"" : "NcxOrder";
        var lTable = kind == "postgres" ? "\"NcxLine\"" : "NcxLine";
        var oDdl = kind == "postgres"
            ? "\"Id\" INT PRIMARY KEY, \"Cust\" VARCHAR(50) NOT NULL"
            : "Id INT PRIMARY KEY, Cust VARCHAR(50) NOT NULL";
        var lDdl = kind == "postgres"
            ? "\"Id\" INT PRIMARY KEY, \"OrderId\" INT NOT NULL, \"Sku\" VARCHAR(50) NOT NULL, \"Qty\" INT NOT NULL"
            : "Id INT PRIMARY KEY, OrderId INT NOT NULL, Sku VARCHAR(50) NOT NULL, Qty INT NOT NULL";
        var oCols = kind == "postgres" ? "(\"Id\", \"Cust\")" : "(Id, Cust)";
        var lCols = kind == "postgres" ? "(\"Id\", \"OrderId\", \"Sku\", \"Qty\")" : "(Id, OrderId, Sku, Qty)";

        Exec(factory!, $"DROP TABLE IF EXISTS {lTable}");
        Exec(factory!, $"DROP TABLE IF EXISTS {oTable}");
        Exec(factory!, $"CREATE TABLE {oTable} ({oDdl})");
        Exec(factory!, $"CREATE TABLE {lTable} ({lDdl})");
        Exec(factory!, $"INSERT INTO {oTable} {oCols} VALUES (1,'ann'),(2,'bob'),(3,'cid')");
        Exec(factory!, $"INSERT INTO {lTable} {lCols} VALUES (1,1,'aaa',3),(2,1,'bbb',1),(3,2,'ccc',5),(4,2,'ddd',1)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!, new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Order>().HasKey(o => o.Id);
                    mb.Entity<Line>().HasKey(l => l.Id);
                    mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId);
                }
            });
            Assert.Equal(expected, normQuery(ctx.Query<Order>()));
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
    public void Nav_collection_count_in_projection_on_live_server(string kind)
        => RunNav(kind,
            q => q.OrderBy(o => o.Id).Select(o => new { o.Id, C = o.Lines.Count() }).ToList().Select(x => $"{x.Id}:{x.C}").ToList(),
            Orders.OrderBy(o => o.Id).Select(o => $"{o.Id}:{LinesOf(o).Count()}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_collection_sum_in_projection_on_live_server(string kind)
        => RunNav(kind,
            q => q.OrderBy(o => o.Id).Select(o => new { o.Id, S = o.Lines.Sum(l => l.Qty) }).ToList().Select(x => $"{x.Id}:{x.S}").ToList(),
            Orders.OrderBy(o => o.Id).Select(o => $"{o.Id}:{LinesOf(o).Sum(l => l.Qty)}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_collection_any_in_predicate_on_live_server(string kind)
        => RunNav(kind,
            q => q.Where(o => o.Lines.Any(l => l.Qty > 3)).OrderBy(o => o.Id).Select(o => o.Cust).ToList(),
            Orders.Where(o => LinesOf(o).Any(l => l.Qty > 3)).OrderBy(o => o.Id).Select(o => o.Cust).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_safe_key_ordered_first_in_projection_on_live_server(string kind)
        => RunNav(kind,
            q => q.OrderBy(o => o.Id).Select(o => new { o.Id, First = o.Lines.OrderBy(l => l.Qty).ThenBy(l => l.Id).Select(l => l.Sku).FirstOrDefault() })
                  .ToList().Select(x => $"{x.Id}:{x.First}").ToList(),
            Orders.OrderBy(o => o.Id).Select(o => $"{o.Id}:{LinesOf(o).OrderBy(l => l.Qty).ThenBy(l => l.Id).Select(l => l.Sku).FirstOrDefault()}").ToList());
}
