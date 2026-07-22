using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// An enum stored as a string via a value converter can have members that differ ONLY by case
/// (<c>Active</c> vs <c>active</c>). C# equality keeps them distinct, so <c>list.Contains(e.Kind)</c>
/// must match ordinally. MySQL / SQL Server default collations fold case, so a bare
/// <c>Kind IN ('Active')</c> silently ALSO matches rows storing 'active' — the tested expression's
/// CLR type is the enum (not string), so the ordinal-IN wrap must key off the converter's provider
/// type. Parity against LINQ-to-Objects on every configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class EnumStringInOrdinalLiveTests
{
    private enum Kind { Active, active, Pending }   // Active/active differ only by case (valid, distinct)

    private sealed class KindToStringConverter : ValueConverter<Kind, string>
    {
        public override object? ConvertToProvider(Kind v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Kind>(v);
    }

    [Table("EnumInOrd_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public Kind Kind { get; set; }
    }

    //  1 Active   2 active   3 Active   4 Pending
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Kind = Kind.Active }, new Row { Id = 2, Kind = Kind.active },
        new Row { Id = 3, Kind = Kind.Active }, new Row { Id = 4, Kind = Kind.Pending },
    };

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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Enum_string_In_list_is_ordinal_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var kindCol = kind == "postgres" ? "\"Kind\" VARCHAR(20) NOT NULL" : "Kind VARCHAR(20) NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Kind\")" : "(Kind)";
        var table = kind == "postgres" ? "\"EnumInOrd_Test\"" : "EnumInOrd_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {kindCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('Active'),('active'),('Active'),('Pending')");
        try
        {
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Row>().Property<Kind>(p => p.Kind).HasConversion(new KindToStringConverter())
            };
            using var ctx = new DbContext(factory!(), provider!, opts);

            var wanted = new[] { Kind.Active };   // must NOT match 'active' (Id 2)
            var expected = Reference.Where(r => wanted.Contains(r.Kind)).Select(r => r.Id).OrderBy(i => i).ToList();
            var actual = ctx.Query<Row>().Where(r => wanted.Contains(r.Kind)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();

            Assert.Equal(expected, actual);       // [1, 3] — a CI IN would wrongly also return 2
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
