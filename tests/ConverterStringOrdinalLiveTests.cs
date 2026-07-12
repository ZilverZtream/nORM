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
/// A value-converter column that stores strings (enum-to-name) must compare ordinally like every
/// other string comparison: Status == Active must bind 'Active' and NOT match rows storing
/// 'ACTIVE' on CI-collation providers (MySQL, SQL Server). The converter comparison path bypasses
/// the plain string-equality wrap, so it needs the same ordinal treatment.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ConverterStringOrdinalLiveTests
{
    private enum Status { Active = 1, Inactive = 2 }

    [Table("ConvOrd_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public Status State { get; set; }
    }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v, ignoreCase: true);
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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Converter_enum_equality_is_ordinal(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var q = kind == "postgres" ? "\"" : "";
        var table = $"{q}ConvOrd_Test{q}";
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var stateCol = kind == "postgres" ? "\"State\" VARCHAR(20) NOT NULL" : "State VARCHAR(20) NOT NULL";
        var ins = kind == "postgres" ? "(\"State\")" : "(State)";
        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {stateCol})");
        // Row 1 stores the converter's canonical form; row 2 stores a case variant that the
        // converter would never write. Ordinal equality must match only row 1.
        Exec(factory!, $"INSERT INTO {table} {ins} VALUES ('Active'),('ACTIVE')");
        try
        {
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Row>().Property<Status>(p => p.State).HasConversion(new EnumToNameConverter())
            };
            using var ctx = new DbContext(factory!(), provider!, opts);
            var ids = ctx.Query<Row>().Where(r => r.State == Status.Active).Select(r => r.Id)
                .ToList().OrderBy(i => i).ToList();
            Assert.Equal(new[] { 1 }, ids);
        }
        finally { Exec(factory!, $"DROP TABLE IF EXISTS {table}"); }
    }
}
