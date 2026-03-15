using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Provider-matrix tests for raw SQL materialization (M1) and parameter binding (P2).
/// Verifies round-trip correctness for modern CLR types that Convert.ChangeType does not
/// support: Guid, DateOnly, TimeOnly, enum. Also verifies parameter binding for DateOnly,
/// TimeOnly, enum, char, and uint does not throw and returns correct results (P2).
/// </summary>
public class RawSqlTypeMatrixTests
{
    // ── Domain models ──────────────────────────────────────────────────────

    private enum OrderStatus { Pending = 1, Active = 2, Closed = 3 }

    [Table("TmxGuidDate")]
    private class TmxGuidDate
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public Guid GuidCol { get; set; }
        public DateOnly DateCol { get; set; }
        public OrderStatus StatusCol { get; set; }
    }

    [Table("TmxDateParam")]
    private class TmxDateParam
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public DateOnly DateCol { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("TmxEnumParam")]
    private class TmxEnumParam
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public OrderStatus Status { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("TmxTimeOnly")]
    private class TmxTimeOnly
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public TimeOnly TimeCol { get; set; }
    }

    [Table("TmxScalar")]
    private class TmxScalar
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int IntCol { get; set; }
    }

    // ── Setup helper ───────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Materialization — Guid from TEXT ───────────────────────────────

    [Fact]
    public async Task GuidColumn_MaterializesFromText()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxGuidDate (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "GuidCol TEXT NOT NULL, DateCol TEXT NOT NULL, StatusCol INTEGER NOT NULL)");
        await using var _ = ctx;

        var expected = Guid.NewGuid();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO TmxGuidDate (GuidCol, DateCol, StatusCol) " +
            $"VALUES ('{expected}', '2024-03-15', 1)";
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxGuidDate>(
            "SELECT Id, GuidCol, DateCol, StatusCol FROM TmxGuidDate");

        Assert.Single(results);
        Assert.Equal(expected, results[0].GuidCol);
    }

    // ── Materialization — DateOnly from TEXT ───────────────────────────

    [Fact]
    public async Task DateOnlyColumn_MaterializesFromText()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxGuidDate (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "GuidCol TEXT NOT NULL, DateCol TEXT NOT NULL, StatusCol INTEGER NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO TmxGuidDate (GuidCol, DateCol, StatusCol) " +
            $"VALUES ('{Guid.NewGuid()}', '2024-03-15', 1)";
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxGuidDate>(
            "SELECT Id, GuidCol, DateCol, StatusCol FROM TmxGuidDate");

        Assert.Single(results);
        Assert.Equal(new DateOnly(2024, 3, 15), results[0].DateCol);
    }

    // ── Materialization — enum from INTEGER ────────────────────────────

    [Fact]
    public async Task EnumColumn_MaterializesFromInteger()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxGuidDate (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "GuidCol TEXT NOT NULL, DateCol TEXT NOT NULL, StatusCol INTEGER NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO TmxGuidDate (GuidCol, DateCol, StatusCol) " +
            $"VALUES ('{Guid.NewGuid()}', '2024-01-01', 2)"; // 2 = Active
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxGuidDate>(
            "SELECT Id, GuidCol, DateCol, StatusCol FROM TmxGuidDate");

        Assert.Single(results);
        Assert.Equal(OrderStatus.Active, results[0].StatusCol);
    }

    // ── Materialization — TimeOnly from TEXT ───────────────────────────

    [Fact]
    public async Task TimeOnlyColumn_MaterializesFromText()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxTimeOnly (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "TimeCol TEXT NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO TmxTimeOnly (TimeCol) VALUES ('14:30:00')";
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxTimeOnly>(
            "SELECT Id, TimeCol FROM TmxTimeOnly");

        Assert.Single(results);
        Assert.Equal(new TimeOnly(14, 30, 0), results[0].TimeCol);
    }

    // ── Materialization — all three non-primitive types together ────────

    [Fact]
    public async Task AllModernTypes_MaterializeCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxGuidDate (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "GuidCol TEXT NOT NULL, DateCol TEXT NOT NULL, StatusCol INTEGER NOT NULL)");
        await using var _ = ctx;

        var g = Guid.NewGuid();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO TmxGuidDate (GuidCol, DateCol, StatusCol) " +
            $"VALUES ('{g}', '2025-06-01', 3)"; // 3 = Closed
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxGuidDate>(
            "SELECT Id, GuidCol, DateCol, StatusCol FROM TmxGuidDate");

        Assert.Single(results);
        Assert.Equal(g, results[0].GuidCol);
        Assert.Equal(new DateOnly(2025, 6, 1), results[0].DateCol);
        Assert.Equal(OrderStatus.Closed, results[0].StatusCol);
    }

    // ── Parameter binding — DateOnly in WHERE clause ───────────────────

    [Fact]
    public async Task DateOnlyParameter_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxDateParam (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "DateCol TEXT NOT NULL, Label TEXT NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "INSERT INTO TmxDateParam (DateCol, Label) VALUES ('2024-03-15', 'March');" +
            "INSERT INTO TmxDateParam (DateCol, Label) VALUES ('2024-06-01', 'June');";
        cmd.ExecuteNonQuery();

        var target = new DateOnly(2024, 3, 15);
        var results = await ctx.QueryUnchangedAsync<TmxDateParam>(
            "SELECT Id, DateCol, Label FROM TmxDateParam WHERE DateCol = @p0",
            default, target);

        Assert.Single(results);
        Assert.Equal("March", results[0].Label);
        Assert.Equal(target, results[0].DateCol);
    }

    // ── Parameter binding — enum in WHERE clause ───────────────────────

    [Fact]
    public async Task EnumParameter_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxEnumParam (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Status INTEGER NOT NULL, Name TEXT NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "INSERT INTO TmxEnumParam (Status, Name) VALUES (1, 'P1');" +
            "INSERT INTO TmxEnumParam (Status, Name) VALUES (2, 'A1');" +
            "INSERT INTO TmxEnumParam (Status, Name) VALUES (2, 'A2');";
        cmd.ExecuteNonQuery();

        var results = await ctx.QueryUnchangedAsync<TmxEnumParam>(
            "SELECT Id, Status, Name FROM TmxEnumParam WHERE Status = @p0",
            default, OrderStatus.Active);

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(OrderStatus.Active, r.Status));
    }

    // ── Parameter binding — TimeOnly does not throw ───────────────────

    [Fact]
    public async Task TimeOnlyParameter_DoesNotThrow()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxTimeOnly (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "TimeCol TEXT NOT NULL)");
        await using var _ = ctx;

        var ex = await Record.ExceptionAsync(() =>
            ctx.QueryUnchangedAsync<TmxTimeOnly>(
                "SELECT Id, TimeCol FROM TmxTimeOnly WHERE TimeCol = @p0",
                default, new TimeOnly(9, 0, 0)));
        Assert.Null(ex);
    }

    // ── Parameter binding — char does not throw ────────────────────────

    [Fact]
    public async Task CharParameter_DoesNotThrow()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxScalar (Id INTEGER PRIMARY KEY AUTOINCREMENT, IntCol INTEGER NOT NULL)");
        await using var _ = ctx;

        var ex = await Record.ExceptionAsync(() =>
            ctx.QueryUnchangedAsync<TmxScalar>(
                "SELECT Id, IntCol FROM TmxScalar WHERE IntCol = @p0",
                default, 'A'));
        Assert.Null(ex);
    }

    // ── Parameter binding — uint does not throw ────────────────────────

    [Fact]
    public async Task UIntParameter_DoesNotThrow()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxScalar (Id INTEGER PRIMARY KEY AUTOINCREMENT, IntCol INTEGER NOT NULL)");
        await using var _ = ctx;

        uint val = 42u;
        var ex = await Record.ExceptionAsync(() =>
            ctx.QueryUnchangedAsync<TmxScalar>(
                "SELECT Id, IntCol FROM TmxScalar WHERE IntCol = @p0",
                default, val));
        Assert.Null(ex);
    }

    // ── Cross-subsystem — bind DateOnly then materialize round-trip ────

    [Fact]
    public async Task DateOnly_RoundTrip_BindAndMaterialize()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxDateParam (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "DateCol TEXT NOT NULL, Label TEXT NOT NULL)");
        await using var _ = ctx;

        var d1 = new DateOnly(2025, 7, 4);
        var d2 = new DateOnly(2025, 12, 25);
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO TmxDateParam (DateCol, Label) VALUES " +
            $"('{d1:yyyy-MM-dd}', 'Independence'), ('{d2:yyyy-MM-dd}', 'Christmas')";
        cmd.ExecuteNonQuery();

        // DateOnly parameter → filter → DateOnly materialization
        var results = await ctx.QueryUnchangedAsync<TmxDateParam>(
            "SELECT Id, DateCol, Label FROM TmxDateParam WHERE DateCol = @p0",
            default, d1);

        Assert.Single(results);
        Assert.Equal(d1, results[0].DateCol);
        Assert.Equal("Independence", results[0].Label);
    }

    // ── Cross-subsystem — bind enum then materialize round-trip ────────

    [Fact]
    public async Task Enum_RoundTrip_BindAndMaterialize()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE TmxEnumParam (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Status INTEGER NOT NULL, Name TEXT NOT NULL)");
        await using var _ = ctx;

        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "INSERT INTO TmxEnumParam (Status, Name) VALUES (1, 'p'), (3, 'c')";
        cmd.ExecuteNonQuery();

        // Enum parameter → filter → enum materialization
        var results = await ctx.QueryUnchangedAsync<TmxEnumParam>(
            "SELECT Id, Status, Name FROM TmxEnumParam WHERE Status = @p0",
            default, OrderStatus.Closed);

        Assert.Single(results);
        Assert.Equal(OrderStatus.Closed, results[0].Status);
        Assert.Equal("c", results[0].Name);
    }
}
