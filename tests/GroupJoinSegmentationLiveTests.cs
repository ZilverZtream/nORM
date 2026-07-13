using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live parity for per-outer-row GroupJoin segmentation and navigation-member join
/// keys: distinct outers sharing a key value must each yield their own result (with
/// the PK ORDER BY tiebreak accepted by every dialect, including when the key IS the
/// PK), and GroupJoin keys reaching through a reference navigation must group
/// correctly on every configured server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class GroupJoinSegmentationLiveTests
{
    [Table("GjSegL_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("GjSegL_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    [Table("GjSegL_Badge")]
    private class Badge
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
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
    public void GroupJoin_segmentation_and_nav_keys_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Dept")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Badge")}");
        Exec(factory!, $"CREATE TABLE {T("GjSegL_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("GjSegL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"CREATE TABLE {T("GjSegL_Badge")} ({q}Id{q} INT PRIMARY KEY, {q}Label{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"INSERT INTO {T("GjSegL_Dept")} VALUES (1, 'Eng'), (2, 'Ops')");
        Exec(factory!, $"INSERT INTO {T("GjSegL_Emp")} VALUES (1, 'ann', 1), (2, 'bob', 2), (3, 'ann', NULL), (4, 'dan', 1)");
        Exec(factory!, $"INSERT INTO {T("GjSegL_Badge")} VALUES (1, 'ann'), (2, 'ann'), (3, 'Eng')");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);

            // Two 'ann' outers must each own both 'ann' badges.
            var byName = ctx.Query<Emp>()
                .GroupJoin(ctx.Query<Badge>(), e => e.Name, b => b.Label, (e, bs) => new { e.Id, Count = bs.Count() })
                .ToList();
            Assert.Equal(4, byName.Count);
            Assert.Equal(2, byName.Single(r => r.Id == 1).Count);
            Assert.Equal(2, byName.Single(r => r.Id == 3).Count);
            Assert.Equal(0, byName.Single(r => r.Id == 2).Count);

            // Key IS the PK: the tiebreak must not duplicate the ORDER BY column.
            var byPk = ctx.Query<Emp>()
                .GroupJoin(ctx.Query<Badge>(), e => e.Id, b => b.Id, (e, bs) => new { e.Id, Count = bs.Count() })
                .ToList();
            Assert.Equal(4, byPk.Count);

            // Outer key through a navigation: grouped per emp, matched via dept title.
            var navOuter = ctx.Query<Emp>()
                .GroupJoin(ctx.Query<Badge>(), e => e.Dept!.Title, b => b.Label, (e, bs) => new { e.Id, Count = bs.Count() })
                .ToList();
            Assert.Equal(4, navOuter.Count);
            Assert.Equal(1, navOuter.Single(r => r.Id == 1).Count); // Eng badge
            Assert.Equal(0, navOuter.Single(r => r.Id == 2).Count); // Ops: none
            Assert.Equal(0, navOuter.Single(r => r.Id == 3).Count); // no dept
            Assert.Equal(1, navOuter.Single(r => r.Id == 4).Count);

            // Inner key through a navigation.
            var navInner = ctx.Query<Badge>()
                .GroupJoin(ctx.Query<Emp>(), b => b.Label, e => e.Dept!.Title, (b, es) => new { b.Id, Count = es.Count() })
                .ToList();
            Assert.Equal(3, navInner.Count);
            Assert.Equal(2, navInner.Single(r => r.Id == 3).Count); // Eng: ann+dan
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Dept")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("GjSegL_Badge")}");
        }
    }
}
