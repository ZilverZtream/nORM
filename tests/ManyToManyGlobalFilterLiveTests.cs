using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live parity for right-side global filters in many-to-many eager loading:
/// soft-deleted related rows stay out of the loaded collections on every
/// configured server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ManyToManyGlobalFilterLiveTests
{
    [Table("M2mGfL_Post")]
    private class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mGfL_Tag")]
    private class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
        public bool Hidden { get; set; }
    }

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
    public void M2m_include_excludes_filtered_right_rows_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        var boolType = kind switch
        {
            "postgres" => "BOOLEAN",
            "sqlserver" => "BIT",
            _ => "TINYINT(1)",
        };
        var f = kind == "postgres" ? "FALSE" : "0";
        var t = kind == "postgres" ? "TRUE" : "1";
        Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_PostTag")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_Tag")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_Post")}");
        Exec(factory!, $"CREATE TABLE {T("M2mGfL_Post")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("M2mGfL_Tag")} ({q}Id{q} INT PRIMARY KEY, {q}Label{q} VARCHAR(50) NOT NULL, {q}Hidden{q} {boolType} NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("M2mGfL_PostTag")} ({q}PostId{q} INT NOT NULL, {q}TagId{q} INT NOT NULL)");
        Exec(factory!, $"INSERT INTO {T("M2mGfL_Post")} VALUES (1, 'hello')");
        Exec(factory!, $"INSERT INTO {T("M2mGfL_Tag")} VALUES (1, 'ok', {f}), (2, 'secret', {t})");
        Exec(factory!, $"INSERT INTO {T("M2mGfL_PostTag")} VALUES (1, 1), (1, 2)");
        try
        {
            var opts = new DbContextOptions();
            opts.AddGlobalFilter<Tag>(x => !x.Hidden);
            opts.OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mGfL_PostTag", "PostId", "TagId");
            };
            using var ctx = new DbContext(factory!(), provider!, opts);
            var post = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
            Assert.Single(post.Tags);
            Assert.Equal("ok", post.Tags[0].Label);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_PostTag")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_Tag")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("M2mGfL_Post")}");
        }
    }
}
