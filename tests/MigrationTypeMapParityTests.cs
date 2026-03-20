using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// X2: Verifies that all 4 migration SQL generators map the expanded set of CLR types
/// (byte[], DateOnly, TimeOnly, DateTimeOffset, TimeSpan, char, sbyte, ushort, uint, ulong)
/// and that enum types are resolved via their underlying integral type.
/// </summary>
public class MigrationTypeMapParityTests
{
    // X2: enums must be internal (not private) so migration generators can resolve them
    // via Assembly.GetType() in the ResolveType helper.
    internal enum TestStatusEnum { Active, Inactive }
    internal enum TestLongEnum : long { Big = 100 }

    private static SchemaDiff MakeSingleColumnDiff(string clrTypeFullName)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(new TableSchema
        {
            Name = "TestTable",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema
                {
                    Name = "Id",
                    ClrType = typeof(int).FullName!,
                    IsPrimaryKey = true
                },
                new ColumnSchema
                {
                    Name = "Col",
                    ClrType = clrTypeFullName,
                    IsNullable = true
                }
            }
        });
        return diff;
    }

    private static string ExtractColType(MigrationSqlStatements result, string colName)
    {
        // The CREATE TABLE statement contains column definitions; extract the type for the named column.
        var createSql = result.Up.First(s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
        // Parse: <escaped_col_name> <TYPE> NULL/NOT NULL
        // The column name is escaped differently per provider, so find "Col" in the SQL
        var idx = createSql.IndexOf(colName, StringComparison.OrdinalIgnoreCase);
        if (idx < 0) throw new Exception($"Column {colName} not found in: {createSql}");
        // Skip past the escaped column name to the type
        // Find the closing bracket/quote after colName
        var afterName = idx + colName.Length;
        // Skip closing escape char (], ", `)
        while (afterName < createSql.Length && (createSql[afterName] == ']' || createSql[afterName] == '"' || createSql[afterName] == '`'))
            afterName++;
        // Skip whitespace
        while (afterName < createSql.Length && createSql[afterName] == ' ')
            afterName++;
        // Read until NULL or NOT NULL
        var typeEnd = createSql.IndexOf(" NULL", afterName, StringComparison.OrdinalIgnoreCase);
        if (typeEnd < 0) throw new Exception($"Could not find NULL marker after type in: {createSql}");
        return createSql.Substring(afterName, typeEnd - afterName).Trim();
    }

    // ── byte[] ──────────────────────────────────────────────────────────────

    [Fact]
    public void X2_ByteArray_Sqlite_Maps_To_BLOB()
    {
        var gen = new SqliteMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(byte[]).FullName!));
        var colType = ExtractColType(result, "Col");
        Assert.Equal("BLOB", colType);
    }

    [Fact]
    public void X2_ByteArray_SqlServer_Maps_To_VARBINARY_MAX()
    {
        var gen = new SqlServerMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(byte[]).FullName!));
        var colType = ExtractColType(result, "Col");
        Assert.Equal("VARBINARY(MAX)", colType);
    }

    [Fact]
    public void X2_ByteArray_MySql_Maps_To_BLOB()
    {
        var gen = new MySqlMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(byte[]).FullName!));
        var colType = ExtractColType(result, "Col");
        Assert.Equal("BLOB", colType);
    }

    [Fact]
    public void X2_ByteArray_Postgres_Maps_To_BYTEA()
    {
        var gen = new PostgresMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(byte[]).FullName!));
        var colType = ExtractColType(result, "Col");
        Assert.Equal("BYTEA", colType);
    }

    // ── DateOnly ────────────────────────────────────────────────────────────

    [Fact]
    public void X2_DateOnly_Sqlite_Maps_To_TEXT()
    {
        var gen = new SqliteMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(DateOnly).FullName!));
        Assert.Equal("TEXT", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_DateOnly_SqlServer_Maps_To_DATE()
    {
        var gen = new SqlServerMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(DateOnly).FullName!));
        Assert.Equal("DATE", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_DateOnly_MySql_Maps_To_DATE()
    {
        var gen = new MySqlMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(DateOnly).FullName!));
        Assert.Equal("DATE", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_DateOnly_Postgres_Maps_To_DATE()
    {
        var gen = new PostgresMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(DateOnly).FullName!));
        Assert.Equal("DATE", ExtractColType(result, "Col"));
    }

    // ── TimeOnly ────────────────────────────────────────────────────────────

    [Fact]
    public void X2_TimeOnly_Sqlite_Maps_To_TEXT()
    {
        var gen = new SqliteMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TimeOnly).FullName!));
        Assert.Equal("TEXT", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_TimeOnly_SqlServer_Maps_To_TIME()
    {
        var gen = new SqlServerMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TimeOnly).FullName!));
        Assert.Equal("TIME", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_TimeOnly_MySql_Maps_To_TIME()
    {
        var gen = new MySqlMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TimeOnly).FullName!));
        Assert.Equal("TIME", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_TimeOnly_Postgres_Maps_To_TIME()
    {
        var gen = new PostgresMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TimeOnly).FullName!));
        Assert.Equal("TIME", ExtractColType(result, "Col"));
    }

    // ── Enum maps to underlying type ────────────────────────────────────────

    [Fact]
    public void X2_Enum_Sqlite_Maps_To_Underlying_Int()
    {
        var gen = new SqliteMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TestStatusEnum).FullName!));
        // int -> INTEGER in SQLite
        Assert.Equal("INTEGER", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_Enum_SqlServer_Maps_To_Underlying_Int()
    {
        var gen = new SqlServerMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TestStatusEnum).FullName!));
        // int -> INT in SQL Server
        Assert.Equal("INT", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_Enum_MySql_Maps_To_Underlying_Int()
    {
        var gen = new MySqlMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TestStatusEnum).FullName!));
        // int -> INT in MySQL
        Assert.Equal("INT", ExtractColType(result, "Col"));
    }

    [Fact]
    public void X2_Enum_Postgres_Maps_To_Underlying_Int()
    {
        var gen = new PostgresMigrationSqlGenerator();
        var result = gen.GenerateSql(MakeSingleColumnDiff(typeof(TestStatusEnum).FullName!));
        // int -> INTEGER in Postgres
        Assert.Equal("INTEGER", ExtractColType(result, "Col"));
    }

    // ── All 4 generators handle the same expanded type set ──────────────────

    [Theory]
    [InlineData("System.Byte[]")]
    [InlineData("System.DateOnly")]
    [InlineData("System.TimeOnly")]
    [InlineData("System.DateTimeOffset")]
    [InlineData("System.TimeSpan")]
    [InlineData("System.Char")]
    [InlineData("System.SByte")]
    [InlineData("System.UInt16")]
    [InlineData("System.UInt32")]
    [InlineData("System.UInt64")]
    public void X2_All_Generators_Map_Expanded_Type(string clrTypeName)
    {
        var generators = new IMigrationSqlGenerator[]
        {
            new SqliteMigrationSqlGenerator(),
            new SqlServerMigrationSqlGenerator(),
            new MySqlMigrationSqlGenerator(),
            new PostgresMigrationSqlGenerator()
        };

        // Use the FullName for byte[] which is "System.Byte[]"
        var diff = MakeSingleColumnDiff(clrTypeName);

        foreach (var gen in generators)
        {
            var result = gen.GenerateSql(diff);
            var colType = ExtractColType(result, "Col");

            // The type should NOT be the fallback type (TEXT/NVARCHAR(MAX)/LONGTEXT/TEXT)
            // This verifies the expanded type map is being used
            var isFallback = gen switch
            {
                SqliteMigrationSqlGenerator => colType == "TEXT" && clrTypeName != "System.DateOnly"
                    && clrTypeName != "System.TimeOnly" && clrTypeName != "System.DateTimeOffset"
                    && clrTypeName != "System.TimeSpan" && clrTypeName != "System.Char",
                SqlServerMigrationSqlGenerator => colType == "NVARCHAR(MAX)",
                MySqlMigrationSqlGenerator => colType == "LONGTEXT",
                PostgresMigrationSqlGenerator => colType == "TEXT" && clrTypeName != "System.DateOnly"
                    && clrTypeName != "System.TimeOnly" && clrTypeName != "System.DateTimeOffset"
                    && clrTypeName != "System.TimeSpan" && clrTypeName != "System.Char",
                _ => false
            };
            Assert.False(isFallback, $"{gen.GetType().Name} fell back to default type '{colType}' for {clrTypeName}");
        }
    }
}
