using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for cross-provider DDL type-mapping parity (provider-mobility matrix cell).
///
/// Every CLR type family verified by the write-path matrix maps to a documented provider type in
/// EVERY migration SQL generator - asserted end-to-end through <c>GenerateSql</c> over a table
/// containing one column per family, so a type silently missing from a generator's map (which would
/// fall back to a default type) or a mapping change is caught. Decimal precision/scale and bounded
/// string/binary lengths remain column-configurable; the pinned values are the unconfigured defaults.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DdlTypeMappingParityContractTests
{
    private static readonly (string Suffix, Type Clr)[] Families =
    {
        ("int", typeof(int)), ("long", typeof(long)), ("short", typeof(short)),
        ("byte", typeof(byte)), ("sbyte", typeof(sbyte)), ("ushort", typeof(ushort)),
        ("uint", typeof(uint)), ("ulong", typeof(ulong)),
        ("bool", typeof(bool)), ("decimal", typeof(decimal)),
        ("double", typeof(double)), ("float", typeof(float)),
        ("stringv", typeof(string)), ("charv", typeof(char)),
        ("guid", typeof(Guid)), ("blob", typeof(byte[])),
        ("dt", typeof(DateTime)), ("dto", typeof(DateTimeOffset)),
        ("don", typeof(DateOnly)), ("ton", typeof(TimeOnly)), ("ts", typeof(TimeSpan)),
    };

    private static string GenerateCreateSql(Func<SchemaDiff, IReadOnlyList<string>> generate)
    {
        var t = new TableSchema { Name = "TypeMapParity" };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsIdentity = true });
        foreach (var (suffix, clr) in Families)
            t.Columns.Add(new ColumnSchema { Name = "C_" + suffix, ClrType = clr.FullName!, IsNullable = true });

        var statements = generate(new SchemaDiff { AddedTables = { t } });
        // Strip every identifier-escape character so the assertions are provider-agnostic.
        var sql = string.Join("\n", statements);
        return sql.Replace("[", "").Replace("]", "").Replace("`", "").Replace("\"", "");
    }

    private static void AssertMappings(string sql, Dictionary<string, string> expected)
    {
        foreach (var (suffix, type) in expected)
            Assert.Contains($"C_{suffix} {type}", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Sqlite_maps_every_family_to_its_documented_type()
    {
        var sql = GenerateCreateSql(d => new SqliteMigrationSqlGenerator().GenerateSql(d).Up);
        AssertMappings(sql, new()
        {
            ["int"] = "INTEGER", ["long"] = "INTEGER", ["short"] = "INTEGER", ["byte"] = "INTEGER",
            ["sbyte"] = "INTEGER", ["ushort"] = "INTEGER", ["uint"] = "INTEGER", ["ulong"] = "INTEGER",
            ["bool"] = "INTEGER", ["decimal"] = "TEXT", ["double"] = "REAL", ["float"] = "REAL",
            ["stringv"] = "TEXT", ["charv"] = "TEXT", ["guid"] = "TEXT", ["blob"] = "BLOB",
            ["dt"] = "TEXT", ["dto"] = "TEXT", ["don"] = "TEXT", ["ton"] = "TEXT", ["ts"] = "TEXT",
        });
    }

    [Fact]
    public void SqlServer_maps_every_family_to_its_documented_type()
    {
        var sql = GenerateCreateSql(d => new SqlServerMigrationSqlGenerator().GenerateSql(d).Up);
        AssertMappings(sql, new()
        {
            ["int"] = "INT", ["long"] = "BIGINT", ["short"] = "SMALLINT", ["byte"] = "TINYINT",
            ["sbyte"] = "SMALLINT", ["ushort"] = "INT", ["uint"] = "BIGINT", ["ulong"] = "DECIMAL(20,0)",
            ["bool"] = "BIT", ["decimal"] = "DECIMAL(18,2)", ["double"] = "FLOAT", ["float"] = "REAL",
            ["stringv"] = "NVARCHAR(MAX)", ["charv"] = "NCHAR(1)", ["guid"] = "UNIQUEIDENTIFIER",
            ["blob"] = "VARBINARY(MAX)", ["dt"] = "DATETIME2", ["dto"] = "DATETIMEOFFSET",
            ["don"] = "DATE", ["ton"] = "TIME", ["ts"] = "TIME",
        });
    }

    [Fact]
    public void Postgres_maps_every_family_to_its_documented_type()
    {
        var sql = GenerateCreateSql(d => new PostgresMigrationSqlGenerator().GenerateSql(d).Up);
        AssertMappings(sql, new()
        {
            ["int"] = "INTEGER", ["long"] = "BIGINT", ["short"] = "SMALLINT", ["byte"] = "SMALLINT",
            ["sbyte"] = "SMALLINT", ["ushort"] = "INTEGER", ["uint"] = "BIGINT", ["ulong"] = "NUMERIC(20,0)",
            ["bool"] = "BOOLEAN", ["decimal"] = "DECIMAL(18,2)", ["double"] = "DOUBLE PRECISION",
            ["float"] = "REAL", ["stringv"] = "TEXT", ["charv"] = "CHAR(1)", ["guid"] = "UUID",
            ["blob"] = "BYTEA", ["dt"] = "TIMESTAMP", ["dto"] = "TIMESTAMPTZ",
            ["don"] = "DATE", ["ton"] = "TIME", ["ts"] = "INTERVAL",
        });
    }

    [Fact]
    public void MySql_maps_every_family_to_its_documented_type()
    {
        var sql = GenerateCreateSql(d => new MySqlMigrationSqlGenerator().GenerateSql(d).Up);
        AssertMappings(sql, new()
        {
            ["int"] = "INT", ["long"] = "BIGINT", ["short"] = "SMALLINT", ["byte"] = "TINYINT UNSIGNED",
            ["sbyte"] = "TINYINT", ["ushort"] = "SMALLINT UNSIGNED", ["uint"] = "INT UNSIGNED",
            ["ulong"] = "BIGINT UNSIGNED", ["bool"] = "TINYINT(1)", ["decimal"] = "DECIMAL(18,2)",
            ["double"] = "DOUBLE", ["float"] = "FLOAT", ["stringv"] = "LONGTEXT", ["charv"] = "CHAR(1)",
            ["guid"] = "CHAR(36)", ["blob"] = "BLOB", ["dt"] = "DATETIME(6)", ["dto"] = "DATETIME(6)",
            ["don"] = "DATE", ["ton"] = "TIME(6)", ["ts"] = "TIME(6)",
        });
    }
}
