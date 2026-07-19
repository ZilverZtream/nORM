using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live parity for shaping an owned (OwnsMany) or many-to-many collection into a projection on the
/// TRUE-ASYNC execution path. SQLite routes <c>ToListAsync</c> through the synchronous materialize path, so
/// the async split-query loaders (<c>LoadManyToManyProjectionAsync</c> / <c>LoadOwnedCollectionProjectionAsync</c>)
/// only run against SQL Server / PostgreSQL / MySQL — this exercises them directly, for bare, filtered, and
/// element-projected bindings.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderShapedCollectionProjectionAsyncTests
{
    [Table("SpaL_Post")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("SpaL_Tag")]
    private class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; }

    [Table("SpaL_Order")]
    private class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    private class Line { public int Id { get; set; } public int Amount { get; set; } }

    private class TagView { public string Label { get; set; } = ""; }
    private class LineView { public int Amount { get; set; } }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default: throw new ArgumentOutOfRangeException(nameof(kind));
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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Shaped_owned_and_m2m_projections_load_on_the_async_path(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;

        void Drop()
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpaL_PostTag")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpaL_Tag")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpaL_Post")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpaL_Line")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpaL_Order")}");
        }

        Drop();
        Exec(factory!, $"CREATE TABLE {T("SpaL_Post")} ({q}Id{q} INT PRIMARY KEY)");
        Exec(factory!, $"CREATE TABLE {T("SpaL_Tag")} ({q}Id{q} INT PRIMARY KEY, {q}Label{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("SpaL_PostTag")} ({q}PostId{q} INT NOT NULL, {q}TagId{q} INT NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("SpaL_Order")} ({q}Id{q} INT PRIMARY KEY)");
        Exec(factory!, $"CREATE TABLE {T("SpaL_Line")} ({q}Id{q} INT PRIMARY KEY, {q}OrderId{q} INT NOT NULL, {q}Amount{q} INT NOT NULL)");
        // Post 1 has two tags (x, y); Order 1 has two lines (10, 5) — two rows per collection so a filter that
        // keeps one is non-vacuous.
        Exec(factory!, $"INSERT INTO {T("SpaL_Post")} VALUES (1)");
        Exec(factory!, $"INSERT INTO {T("SpaL_Tag")} VALUES (1, 'x'), (2, 'y')");
        Exec(factory!, $"INSERT INTO {T("SpaL_PostTag")} VALUES (1, 1), (1, 2)");
        Exec(factory!, $"INSERT INTO {T("SpaL_Order")} VALUES (1)");
        Exec(factory!, $"INSERT INTO {T("SpaL_Line")} VALUES (1, 1, 10), (2, 1, 5)");
        try
        {
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Post>().HasKey(p => p.Id);
                    mb.Entity<Tag>().HasKey(t => t.Id);
                    mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("SpaL_PostTag", "PostId", "TagId");
                    mb.Entity<Order>().HasKey(o => o.Id);
                    mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "SpaL_Line", foreignKey: "OrderId");
                }
            };
            using var ctx = new DbContext(factory!(), provider!, opts);

            // Bare many-to-many.
            var m2mBare = await ((INormQueryable<Post>)ctx.Query<Post>())
                .Select(p => new { p.Id, Tags = p.Tags.ToList() }).ToListAsync();
            Assert.Equal(new[] { "x", "y" }, m2mBare.Single().Tags.Select(t => t.Label).OrderBy(l => l));

            // Filtered many-to-many (closure capture exercises the @cp parameter binding).
            var wantId = 1;
            var m2mFiltered = await ((INormQueryable<Post>)ctx.Query<Post>())
                .Select(p => new { p.Id, Tags = p.Tags.Where(t => t.Id == wantId).ToList() }).ToListAsync();
            Assert.Equal("x", m2mFiltered.Single().Tags.Single().Label);

            // Element-projected many-to-many.
            var m2mProjected = await ((INormQueryable<Post>)ctx.Query<Post>())
                .Select(p => new { p.Id, Tags = p.Tags.Select(t => new TagView { Label = t.Label }).ToList() }).ToListAsync();
            Assert.Equal(new[] { "x", "y" }, m2mProjected.Single().Tags.Select(t => t.Label).OrderBy(l => l));

            // Bare owned.
            var ownedBare = await ((INormQueryable<Order>)ctx.Query<Order>())
                .Select(o => new { o.Id, Lines = o.Lines.ToList() }).ToListAsync();
            Assert.Equal(new[] { 5, 10 }, ownedBare.Single().Lines.Select(l => l.Amount).OrderBy(a => a));

            // Filtered owned (closure capture).
            var minAmount = 6;
            var ownedFiltered = await ((INormQueryable<Order>)ctx.Query<Order>())
                .Select(o => new { o.Id, Lines = o.Lines.Where(l => l.Amount > minAmount).ToList() }).ToListAsync();
            Assert.Equal(10, ownedFiltered.Single().Lines.Single().Amount);

            // Element-projected owned.
            var ownedProjected = await ((INormQueryable<Order>)ctx.Query<Order>())
                .Select(o => new { o.Id, Lines = o.Lines.Select(l => new LineView { Amount = l.Amount }).ToList() }).ToListAsync();
            Assert.Equal(new[] { 5, 10 }, ownedProjected.Single().Lines.Select(l => l.Amount).OrderBy(a => a));
        }
        finally
        {
            Drop();
        }
    }
}
