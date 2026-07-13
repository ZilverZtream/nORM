using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// MySQL requires a temporal function default's fractional-seconds precision to
/// match the column's exactly, so the DATETIME(6) columns the generator now emits
/// need CURRENT_TIMESTAMP-family defaults rewritten to CURRENT_TIMESTAMP(6) —
/// the bare form fails CREATE TABLE with "Invalid default value".
/// </summary>
[Trait("Category", "Fast")]
public class MySqlTemporalDefaultPrecisionTests
{
    private static string GenerateCreateTable(string clrType, string defaultValue)
    {
        var diff = new SchemaDiff();
        diff.AddedTables.Add(new TableSchema
        {
            Name = "DefaultPrecision_Test",
            Columns = new List<ColumnSchema>
            {
                new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true, IsNullable = false },
                new ColumnSchema { Name = "At", ClrType = clrType, IsNullable = false, DefaultValue = defaultValue },
            }
        });
        var result = new MySqlMigrationSqlGenerator().GenerateSql(diff);
        return result.Up.First(s => s.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(6)")]
    [InlineData("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP(6)")]
    [InlineData("NOW()", "NOW(6)")]
    [InlineData("LOCALTIMESTAMP", "LOCALTIMESTAMP(6)")]
    public void Temporal_function_default_carries_column_precision(string given, string expected)
    {
        var sql = GenerateCreateTable(typeof(DateTime).FullName!, given);
        Assert.Contains($"DEFAULT {expected}", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Literal_default_is_not_rewritten()
    {
        var sql = GenerateCreateTable(typeof(DateTime).FullName!, "'2026-01-01 00:00:00'");
        Assert.Contains("DEFAULT '2026-01-01 00:00:00'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Non_temporal_column_default_is_not_rewritten()
    {
        var sql = GenerateCreateTable(typeof(int).FullName!, "0");
        Assert.Contains("DEFAULT 0", sql, StringComparison.Ordinal);
    }
}
