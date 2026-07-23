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
/// SelectMany flattening a navigation (a JOIN) and projecting a shaped child collection (a split query that
/// fetches the children in a second correlated statement) are distinct materialization paths from the
/// scalar nav-aggregate subqueries. Verified against a hand-resolved oracle on SQLite; this pins the same
/// results on every configured live server so a dialect-specific JOIN or split-query defect surfaces.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class CollectionProjectionCrossProviderLiveTests
{
    [Table("CpxOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public string Cust { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("CpxLine")]
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

        var oTable = kind == "postgres" ? "\"CpxOrder\"" : "CpxOrder";
        var lTable = kind == "postgres" ? "\"CpxLine\"" : "CpxLine";
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
    public void SelectMany_flatten_navigation_on_live_server(string kind)
        => RunNav(kind,
            q => q.SelectMany(o => o.Lines, (o, l) => new { o.Cust, l.Sku }).ToList()
                  .OrderBy(x => x.Cust, StringComparer.Ordinal).ThenBy(x => x.Sku, StringComparer.Ordinal).Select(x => $"{x.Cust}:{x.Sku}").ToList(),
            Orders.SelectMany(o => LinesOf(o), (o, l) => new { o.Cust, l.Sku })
                  .OrderBy(x => x.Cust, StringComparer.Ordinal).ThenBy(x => x.Sku, StringComparer.Ordinal).Select(x => $"{x.Cust}:{x.Sku}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void SelectMany_filtered_navigation_on_live_server(string kind)
        => RunNav(kind,
            q => q.SelectMany(o => o.Lines.Where(l => l.Qty > 1), (o, l) => new { o.Cust, l.Sku }).ToList()
                  .OrderBy(x => x.Cust, StringComparer.Ordinal).ThenBy(x => x.Sku, StringComparer.Ordinal).Select(x => $"{x.Cust}:{x.Sku}").ToList(),
            Orders.SelectMany(o => LinesOf(o).Where(l => l.Qty > 1), (o, l) => new { o.Cust, l.Sku })
                  .OrderBy(x => x.Cust, StringComparer.Ordinal).ThenBy(x => x.Sku, StringComparer.Ordinal).Select(x => $"{x.Cust}:{x.Sku}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Project_shaped_child_collection_on_live_server(string kind)
        => RunNav(kind,
            q => q.OrderBy(o => o.Id).Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() }).ToList()
                  .Select(x => $"{x.Id}:{string.Join(",", x.Skus.OrderBy(s => s, StringComparer.Ordinal))}").ToList(),
            Orders.OrderBy(o => o.Id)
                  .Select(o => $"{o.Id}:{string.Join(",", LinesOf(o).Select(l => l.Sku).OrderBy(s => s, StringComparer.Ordinal))}").ToList());
}
