using System;
using System.Collections.Generic;
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
/// LINQ set operations and char comparisons are ordinal: Union keeps "abc" and "ABC" as distinct
/// values, Intersect/Except match byte-wise, and 'a' != 'A'. On MySQL / SQL Server default CI
/// collations the SQL equivalents fold case. Parity is asserted against LINQ-to-Objects on every
/// configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringSetOpOrdinalSemanticsLiveTests
{
    [Table("SetOpA_Test")]
    private class A
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("SetOpB_Test")]
    private class B
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
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

    // A: abc, ABC, xyz      B: ABC, xyz, q
    private static readonly A[] As = { new A { Id = 1, Name = "abc" }, new A { Id = 2, Name = "ABC" }, new A { Id = 3, Name = "xyz" } };
    private static readonly B[] Bs = { new B { Id = 1, Name = "ABC" }, new B { Id = 2, Name = "xyz" }, new B { Id = 3, Name = "q" } };

    private void Setup(string kind, out Func<DbConnection> factory, out DatabaseProvider provider, out string ta, out string tb, out bool skip)
    {
        var (f, p, reason) = OpenLive(kind);
        skip = reason != null;
        factory = f!; provider = p!;
        var q = kind == "postgres" ? "\"" : "";
        ta = $"{q}SetOpA_Test{q}"; tb = $"{q}SetOpB_Test{q}";
        if (skip) return;
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var nameCol = kind == "postgres" ? "\"Name\" VARCHAR(20) NOT NULL" : "Name VARCHAR(20) NOT NULL";
        var ins = kind == "postgres" ? "(\"Name\")" : "(Name)";
        Exec(factory, $"DROP TABLE IF EXISTS {ta}");
        Exec(factory, $"DROP TABLE IF EXISTS {tb}");
        Exec(factory, $"CREATE TABLE {ta} ({idCol}, {nameCol})");
        Exec(factory, $"CREATE TABLE {tb} ({idCol}, {nameCol})");
        Exec(factory, $"INSERT INTO {ta} {ins} VALUES ('abc'),('ABC'),('xyz')");
        Exec(factory, $"INSERT INTO {tb} {ins} VALUES ('ABC'),('xyz'),('q')");
    }

    private void RunParity(string kind, Func<IQueryable<A>, IQueryable<B>, List<string>> query)
    {
        Setup(kind, out var factory, out var provider, out var ta, out var tb, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            var expected = query(As.AsQueryable(), Bs.AsQueryable());
            var actual = query(ctx.Query<A>(), ctx.Query<B>());
            Assert.Equal(expected, actual);
        }
        finally
        {
            Exec(factory, $"DROP TABLE IF EXISTS {ta}");
            Exec(factory, $"DROP TABLE IF EXISTS {tb}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Union_of_string_projections_is_ordinal(string kind)
        // LINQ: {abc, ABC, xyz, q} — CI union would merge abc/ABC.
        => RunParity(kind, (a, b) => a.Select(x => x.Name).Union(b.Select(x => x.Name))
            .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Intersect_of_string_projections_is_ordinal(string kind)
        // LINQ: {ABC, xyz} — CI intersect would also include abc.
        => RunParity(kind, (a, b) => a.Select(x => x.Name).Intersect(b.Select(x => x.Name))
            .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Except_of_string_projections_is_ordinal(string kind)
        // LINQ: {abc} — CI except would remove abc too (it CI-matches B's ABC).
        => RunParity(kind, (a, b) => a.Select(x => x.Name).Except(b.Select(x => x.Name))
            .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Char_equality_is_ordinal(string kind)
    {
        Setup(kind, out var factory, out var provider, out var ta, out var tb, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            // LINQ: only "abc" and... 'a' == 'a' for "abc"; "ABC" starts with 'A' != 'a'.
            var expected = As.AsQueryable().Where(x => x.Name[0] == 'a').Select(x => x.Name)
                .OrderBy(x => x, StringComparer.Ordinal).ToList();
            var actual = ctx.Query<A>().Where(x => x.Name[0] == 'a').Select(x => x.Name)
                .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
            Assert.Equal(expected, actual);
        }
        finally
        {
            Exec(factory, $"DROP TABLE IF EXISTS {ta}");
            Exec(factory, $"DROP TABLE IF EXISTS {tb}");
        }
    }
}
