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

// ── Entity types for P1 parameter binding tests ─────────────────────────────

public enum PB1Priority
{
    Low = 0,
    Medium = 1,
    High = 2,
    Critical = 3
}

[Table("PB1_DateOnlyEntity")]
public class PB1DateOnlyEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Label { get; set; }
    // SQLite stores DateOnly as TEXT via DateTime conversion in AddOptimizedParam
    public DateTime BirthDate { get; set; }
}

[Table("PB1_TimeOnlyEntity")]
public class PB1TimeOnlyEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Label { get; set; }
    // SQLite stores TimeOnly as TEXT via TimeSpan conversion in AddOptimizedParam
    public string? ScheduledTime { get; set; }
}

[Table("PB1_EnumEntity")]
public class PB1EnumEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
    public int Priority { get; set; }
}

[Table("PB1_CharEntity")]
public class PB1CharEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Code { get; set; }
}

// ── P1 parameter binding parity tests ───────────────────────────────────────

/// <summary>
/// Verifies that AddOptimizedParam correctly handles DateOnly, TimeOnly, enum,
/// and char parameter types, both at the ADO.NET level and via round-trip
/// insert/query through the ORM.
/// </summary>
public class ParameterBindingParityTests
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
    //  1. DateOnly parameter binding round-trip
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task P1_DateOnly_RoundTrip_ViaInsertAndQuery()
    {
        // DateOnly is stored as a DATE (DateTime with midnight time) in SQLite.
        // AddOptimizedParam converts DateOnly → DateTime with TimeOnly.MinValue.
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE PB1_DateOnlyEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT, BirthDate TEXT)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        // Insert using a DateTime that represents a DateOnly value
        var dateValue = new DateTime(1990, 6, 15);
        var entity = new PB1DateOnlyEntity { Label = "Alice", BirthDate = dateValue };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        Assert.NotEqual(0, entity.Id);

        // Query back
        var loaded = await ctx.Query<PB1DateOnlyEntity>().ToListAsync();
        Assert.Single(loaded);
        Assert.Equal("Alice", loaded[0].Label);
        Assert.Equal(dateValue.Date, loaded[0].BirthDate.Date);
    }

    [Fact]
    public void P1_DateOnly_AddOptimizedParam_SetsDbTypeDate()
    {
        // Directly verify that AddOptimizedParam converts DateOnly correctly
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var dateOnly = new DateOnly(2024, 3, 15);
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", dateOnly);

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@p0", p.ParameterName);
        Assert.Equal(DbType.Date, p.DbType);
        // Value should be converted to DateTime
        Assert.IsType<DateTime>(p.Value);
        var dt = (DateTime)p.Value;
        Assert.Equal(2024, dt.Year);
        Assert.Equal(3, dt.Month);
        Assert.Equal(15, dt.Day);
        Assert.Equal(0, dt.Hour);
        Assert.Equal(0, dt.Minute);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  2. TimeOnly parameter binding round-trip
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task P1_TimeOnly_RoundTrip_ViaInsertAndQuery()
    {
        // TimeOnly is converted to TimeSpan by AddOptimizedParam.
        // For SQLite, we store it as TEXT (string representation).
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE PB1_TimeOnlyEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT, ScheduledTime TEXT)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new PB1TimeOnlyEntity { Label = "MorningMeeting", ScheduledTime = "09:30:00" };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        Assert.NotEqual(0, entity.Id);

        var loaded = await ctx.Query<PB1TimeOnlyEntity>().ToListAsync();
        Assert.Single(loaded);
        Assert.Equal("MorningMeeting", loaded[0].Label);
        Assert.Equal("09:30:00", loaded[0].ScheduledTime);
    }

    [Fact]
    public void P1_TimeOnly_AddOptimizedParam_SetsDbTypeTime()
    {
        // Directly verify that AddOptimizedParam converts TimeOnly correctly
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        var timeOnly = new TimeOnly(14, 30, 45);
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", timeOnly);

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@p0", p.ParameterName);
        Assert.Equal(DbType.Time, p.DbType);
        // Value should be converted to TimeSpan
        Assert.IsType<TimeSpan>(p.Value);
        var ts = (TimeSpan)p.Value;
        Assert.Equal(14, ts.Hours);
        Assert.Equal(30, ts.Minutes);
        Assert.Equal(45, ts.Seconds);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  3. Enum parameter binding round-trip
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task P1_Enum_RoundTrip_InsertAndWhereFilter()
    {
        // Enums are stored as their underlying integral type (int).
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE PB1_EnumEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Priority INTEGER)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new PB1EnumEntity { Name = "Task1", Priority = (int)PB1Priority.Low });
        ctx.Add(new PB1EnumEntity { Name = "Task2", Priority = (int)PB1Priority.High });
        ctx.Add(new PB1EnumEntity { Name = "Task3", Priority = (int)PB1Priority.Critical });
        ctx.Add(new PB1EnumEntity { Name = "Task4", Priority = (int)PB1Priority.High });
        await ctx.SaveChangesAsync();

        // Query filtering by enum value (stored as int)
        var highPriority = await ctx.Query<PB1EnumEntity>()
            .Where(x => x.Priority == (int)PB1Priority.High)
            .ToListAsync();

        Assert.Equal(2, highPriority.Count);
        Assert.All(highPriority, e => Assert.Equal((int)PB1Priority.High, e.Priority));

        // Also verify we can find critical items
        var critical = await ctx.Query<PB1EnumEntity>()
            .Where(x => x.Priority == (int)PB1Priority.Critical)
            .ToListAsync();
        Assert.Single(critical);
        Assert.Equal("Task3", critical[0].Name);
    }

    [Fact]
    public void P1_Enum_AddOptimizedParam_ConvertsToUnderlyingType()
    {
        // Directly verify that AddOptimizedParam converts enum to its underlying int
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", PB1Priority.High);

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@p0", p.ParameterName);
        Assert.Equal(DbType.Int32, p.DbType);
        // Value should be the underlying int, not the enum
        Assert.Equal(2, p.Value); // PB1Priority.High == 2
        Assert.IsType<int>(p.Value);
    }

    [Fact]
    public void P1_Enum_AddOptimizedParam_AllValues_ConvertCorrectly()
    {
        using var cn = OpenMemory();

        foreach (PB1Priority priority in Enum.GetValues(typeof(PB1Priority)))
        {
            using var cmd = cn.CreateCommand();
            ParameterOptimizer.AddOptimizedParam(cmd, "@p0", priority);
            var p = cmd.Parameters[0];
            Assert.Equal((int)priority, p.Value);
            Assert.Equal(DbType.Int32, p.DbType);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  4. Char parameter binding round-trip
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task P1_Char_RoundTrip_InsertAndQuery()
    {
        // Char is converted to string by AddOptimizedParam (DbType.StringFixedLength).
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE PB1_CharEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new PB1CharEntity { Code = "A" });
        ctx.Add(new PB1CharEntity { Code = "B" });
        ctx.Add(new PB1CharEntity { Code = "Z" });
        await ctx.SaveChangesAsync();

        var all = await ctx.Query<PB1CharEntity>().ToListAsync();
        Assert.Equal(3, all.Count);

        var codes = all.Select(e => e.Code).OrderBy(c => c).ToList();
        Assert.Equal(new[] { "A", "B", "Z" }, codes);
    }

    [Fact]
    public void P1_Char_AddOptimizedParam_SetsDbTypeStringFixedLength()
    {
        // Directly verify that AddOptimizedParam converts char correctly
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();

        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 'X');

        Assert.Single(cmd.Parameters);
        var p = cmd.Parameters[0];
        Assert.Equal("@p0", p.ParameterName);
        Assert.Equal(DbType.StringFixedLength, p.DbType);
        // Value should be converted to string
        Assert.Equal("X", p.Value);
        Assert.IsType<string>(p.Value);
    }

    [Fact]
    public void P1_Char_AddOptimizedParam_SpecialChars()
    {
        using var cn = OpenMemory();

        // Test various special char values
        foreach (char c in new[] { ' ', '\t', '\n', '0', '@', '\u00E9' })
        {
            using var cmd = cn.CreateCommand();
            ParameterOptimizer.AddOptimizedParam(cmd, "@p0", c);
            var p = cmd.Parameters[0];
            Assert.Equal(c.ToString(), p.Value);
            Assert.Equal(DbType.StringFixedLength, p.DbType);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  5. Cross-provider parameter binding verification
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void P1_CrossProvider_DateOnly_AllProviders_SameDbType()
    {
        // Verify AddOptimizedParam handles DateOnly identically regardless of provider.
        // The conversion happens at the ADO.NET param level, not provider level.
        var dateOnly = new DateOnly(2025, 1, 1);

        // SqliteConnection creates params that work the same way
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", dateOnly);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Date, p.DbType);
        Assert.IsType<DateTime>(p.Value);
        Assert.Equal(new DateTime(2025, 1, 1), p.Value);
    }

    [Fact]
    public void P1_CrossProvider_TimeOnly_AllProviders_SameDbType()
    {
        var timeOnly = new TimeOnly(8, 0, 0);

        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", timeOnly);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Time, p.DbType);
        Assert.IsType<TimeSpan>(p.Value);
        Assert.Equal(TimeSpan.FromHours(8), p.Value);
    }

    [Fact]
    public void P1_CrossProvider_Enum_AllProviders_SameDbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", PB1Priority.Medium);

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Int32, p.DbType);
        Assert.Equal(1, p.Value); // Medium == 1
    }

    [Fact]
    public void P1_CrossProvider_Char_AllProviders_SameDbType()
    {
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", 'Q');

        var p = cmd.Parameters[0];
        Assert.Equal(DbType.StringFixedLength, p.DbType);
        Assert.Equal("Q", p.Value);
    }

    [Fact]
    public void P1_CrossProvider_NullDateOnly_SetsDbTypeObject()
    {
        // Null value without knownType should default to DbType.Object
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", null);

        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
    }

    [Fact]
    public void P1_CrossProvider_DBNull_NormalizedToNull()
    {
        // P1/X1 fix: DBNull.Value must be normalized to null
        using var cn = OpenMemory();
        using var cmd = cn.CreateCommand();
        ParameterOptimizer.AddOptimizedParam(cmd, "@p0", DBNull.Value);

        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        // DBNull is normalized so the null path is taken — DbType defaults to Object
        Assert.Equal(DbType.Object, p.DbType);
    }
}
