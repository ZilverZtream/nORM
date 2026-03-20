using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entity types for compile-time query parameter parity tests ──────────────

public enum CTQ_Status
{
    Draft = 0,
    Active = 1,
    Archived = 2,
    Deleted = 3
}

[Flags]
public enum CTQ_Flags : long
{
    None = 0L,
    Read = 1L,
    Write = 2L,
    Execute = 4L,
    All = Read | Write | Execute
}

[Table("CTQ_Events")]
public class CTQ_Event
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
    public DateTime EventDate { get; set; }
}

/// <summary>
/// SG1: [CompileTimeQuery] generated methods must use the same parameter binding as
/// runtime queries. These tests verify that <see cref="ParameterOptimizer.AddOptimizedParam"/>
/// is public and handles all special types (DateOnly, TimeOnly, enum, char) identically
/// to what a source-generated query method would emit.
/// </summary>
public class CompileTimeQueryParameterParityTests
{
    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  1. AddOptimizedParam is public and callable from test assembly
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_AddOptimizedParam_Is_Public_And_Callable()
    {
        // The SG1 fix made ParameterOptimizer a public static class so that
        // source-generated compile-time query methods can call AddOptimizedParam.
        // Verify it is accessible from outside the nORM assembly.
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        // This line would fail to compile if AddOptimizedParam were internal
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 42);

        Assert.Single(cmd.Parameters);
        Assert.Equal(42, cmd.Parameters[0].Value);
    }

    [Fact]
    public void SG1_ParameterOptimizer_Type_Is_Public()
    {
        var type = typeof(ParameterOptimizer);
        Assert.True(type.IsPublic, "ParameterOptimizer must be public for source-generated code to reference it.");
        Assert.True(type.IsAbstract && type.IsSealed, "ParameterOptimizer must be a static class (abstract + sealed).");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  2. DateOnly through AddOptimizedParam
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_DateOnly_AddOptimizedParam_SetsDbTypeDate_And_ConvertsToDateTime()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var dateOnly = new DateOnly(2025, 12, 25);
        ParameterOptimizer.AddOptimizedParam(cmd, "@birthday", dateOnly);

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@birthday", p.ParameterName);
        Assert.Equal(DbType.Date, p.DbType);
        // Value must be converted to DateTime (midnight) for provider compatibility
        Assert.IsType<DateTime>(p.Value);
        var dt = (DateTime)p.Value;
        Assert.Equal(new DateTime(2025, 12, 25, 0, 0, 0), dt);
    }

    [Fact]
    public void SG1_DateOnly_MinMax_Values()
    {
        using var cn = OpenMemory();

        // MinValue
        using var cmd1 = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd1, "@p0", DateOnly.MinValue);
        var dt1 = (DateTime)cmd1.Parameters[0].Value!;
        Assert.Equal(DateOnly.MinValue.Year, dt1.Year);

        // MaxValue
        using var cmd2 = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd2, "@p0", DateOnly.MaxValue);
        var dt2 = (DateTime)cmd2.Parameters[0].Value!;
        Assert.Equal(DateOnly.MaxValue.Year, dt2.Year);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  3. TimeOnly through AddOptimizedParam
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_TimeOnly_AddOptimizedParam_SetsDbTypeTime_And_ConvertsToTimeSpan()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var timeOnly = new TimeOnly(23, 59, 59);
        ParameterOptimizer.AddOptimizedParam(cmd, "@scheduledAt", timeOnly);

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@scheduledAt", p.ParameterName);
        Assert.Equal(DbType.Time, p.DbType);
        Assert.IsType<TimeSpan>(p.Value);
        var ts = (TimeSpan)p.Value;
        Assert.Equal(23, ts.Hours);
        Assert.Equal(59, ts.Minutes);
        Assert.Equal(59, ts.Seconds);
    }

    [Fact]
    public void SG1_TimeOnly_Midnight()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", TimeOnly.MinValue);

        var ts = (TimeSpan)cmd.Parameters[0].Value!;
        Assert.Equal(TimeSpan.Zero, ts);
        Assert.Equal(DbType.Time, cmd.Parameters[0].DbType);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  4. Enum through AddOptimizedParam
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_Enum_Int_Backed_ConvertsToUnderlyingInt()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@status", CTQ_Status.Active);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Int32, p.DbType);
        Assert.IsType<int>(p.Value);
        Assert.Equal(1, p.Value); // Active == 1
    }

    [Fact]
    public void SG1_Enum_Long_Backed_ConvertsToUnderlyingLong()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@flags", CTQ_Flags.All);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Int64, p.DbType);
        Assert.IsType<long>(p.Value);
        Assert.Equal(7L, p.Value); // Read|Write|Execute = 1|2|4 = 7
    }

    [Fact]
    public void SG1_Enum_AllValues_ConvertCorrectly()
    {
        using var cn = OpenMemory();

        foreach (CTQ_Status status in Enum.GetValues(typeof(CTQ_Status)))
        {
            using var cmd = cn.CreateCommand();
            ParameterOptimizer.AddOptimizedParam(cmd, "@p0", status);
            Assert.Equal((int)status, cmd.Parameters[0].Value);
            Assert.Equal(DbType.Int32, cmd.Parameters[0].DbType);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  5. Char through AddOptimizedParam
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_Char_ConvertsToString_WithDbTypeStringFixedLength()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@grade", 'A');

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.StringFixedLength, p.DbType);
        Assert.IsType<string>(p.Value);
        Assert.Equal("A", p.Value);
    }

    [Fact]
    public void SG1_Char_UnicodeAndSpecial()
    {
        using var cn = OpenMemory();

        var testChars = new[] { '\u00E9', '\u4E16', ' ', '\0' }; // accented e, CJK char, space, null char
        foreach (char c in testChars)
        {
            using var cmd = cn.CreateCommand();
            ParameterOptimizer.AddOptimizedParam(cmd, "@p0", c);
            Assert.Equal(c.ToString(), cmd.Parameters[0].Value);
            Assert.Equal(DbType.StringFixedLength, cmd.Parameters[0].DbType);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  6. Runtime query round-trip with DateOnly-equivalent values
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SG1_Runtime_Query_DateOnly_Equivalent_RoundTrip()
    {
        // End-to-end: insert rows with DateTime values (DateOnly equivalent),
        // query via runtime LINQ, verify correct results.
        // This proves the parameter binder works at the ORM level.
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE CTQ_Events (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, EventDate TEXT)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var christmas = new DateTime(2025, 12, 25);
        var newYear = new DateTime(2026, 1, 1);
        var birthday = new DateTime(1990, 6, 15);

        ctx.Add(new CTQ_Event { Name = "Christmas", EventDate = christmas });
        ctx.Add(new CTQ_Event { Name = "NewYear", EventDate = newYear });
        ctx.Add(new CTQ_Event { Name = "Birthday", EventDate = birthday });
        await ctx.SaveChangesAsync();

        // Query filtering by exact date
        var results = await ctx.Query<CTQ_Event>()
            .Where(e => e.EventDate == christmas)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Christmas", results[0].Name);
    }

    [Fact]
    public async Task SG1_Runtime_Query_DateComparison_GreaterThan()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE CTQ_Events (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, EventDate TEXT)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new CTQ_Event { Name = "Past", EventDate = new DateTime(2020, 1, 1) });
        ctx.Add(new CTQ_Event { Name = "Recent", EventDate = new DateTime(2025, 6, 1) });
        ctx.Add(new CTQ_Event { Name = "Future", EventDate = new DateTime(2030, 12, 31) });
        await ctx.SaveChangesAsync();

        var cutoff = new DateTime(2025, 1, 1);
        var after = await ctx.Query<CTQ_Event>()
            .Where(e => e.EventDate > cutoff)
            .ToListAsync();

        Assert.Equal(2, after.Count);
        Assert.All(after, e => Assert.True(e.EventDate > cutoff));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  7. Null parameter handling parity
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SG1_Null_WithKnownType_SetsCorrectDbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", null, typeof(DateTime));

        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    [Fact]
    public void SG1_Null_WithoutKnownType_SetsDbTypeObject()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", null);

        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void SG1_DBNull_NormalizedToNullPath()
    {
        // DBNull.Value should be treated identically to null
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", DBNull.Value);

        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Object, p.DbType);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  8. Standard types parity (int, string, bool, decimal, Guid, long)
    // ─────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(42, DbType.Int32)]
    [InlineData(99L, DbType.Int64)]
    [InlineData(true, DbType.Boolean)]
    [InlineData(false, DbType.Boolean)]
    public void SG1_StandardTypes_MapToCorrectDbType(object value, DbType expected)
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", value);

        Assert.Equal(expected, cmd.Parameters[0].DbType);
    }

    [Fact]
    public void SG1_String_SetsDbTypeString_WithCorrectSize()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var str = "Hello, world!";
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", str);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(str.Length, p.Size);
        Assert.Equal(str, p.Value);
    }

    [Fact]
    public void SG1_Decimal_SetsDbTypeDecimal()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 123.456m);

        Assert.Equal(DbType.Decimal, cmd.Parameters[0].DbType);
        Assert.Equal(123.456m, cmd.Parameters[0].Value);
    }

    [Fact]
    public void SG1_Guid_SetsDbTypeGuid()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var guid = Guid.NewGuid();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", guid);

        Assert.Equal(DbType.Guid, cmd.Parameters[0].DbType);
        Assert.Equal(guid, cmd.Parameters[0].Value);
    }
}
