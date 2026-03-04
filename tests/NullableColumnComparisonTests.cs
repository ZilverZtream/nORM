using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Q1: Verifies that nullable column-vs-column comparisons expand to three-valued SQL logic
/// (IS NULL guards), while non-nullable column comparisons use a plain operator.
/// </summary>
public class NullableColumnComparisonTests : TestBase
{
    private class NullableEntity
    {
        [Key] public int Id { get; set; }
        public int? NullableA { get; set; }
        public int? NullableB { get; set; }
        public int NonNullableA { get; set; }
        public int NonNullableB { get; set; }
        public string? StringA { get; set; }
        public string? StringB { get; set; }
    }

    private static DbConnection CreateConn()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    [Fact]
    public void NullableEqual_ExpandsToIsNullGuard()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.NullableA == e.NullableB, cn, provider);

        // Should contain IS NULL guard for both sides
        Assert.Contains("IS NULL", sql);
        Assert.Contains("=", sql);
    }

    [Fact]
    public void NullableNotEqual_ExpandsToIsNullGuards()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.NullableA != e.NullableB, cn, provider);

        // The expanded SQL must check IS NOT NULL for both operands and IS NULL crossings
        Assert.Contains("IS NOT NULL", sql);
        Assert.Contains("IS NULL", sql);
        Assert.Contains("<>", sql);
    }

    [Fact]
    public void NonNullableNotEqual_UsesPlainNotEqual_NoExpansion()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.NonNullableA != e.NonNullableB, cn, provider);

        // Non-nullable: should be a simple <> with no IS NULL expansion
        Assert.Contains("<>", sql);
        // The IS NULL guard should NOT appear for non-nullable comparisons
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void NonNullableEqual_UsesPlainEqual_NoExpansion()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.NonNullableA == e.NonNullableB, cn, provider);

        // Non-nullable equality should use plain =
        Assert.Contains("=", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void NullableEqual_Constant_StillUsesIsNull()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        int? val = null;
        var (sql, _) = Translate<NullableEntity>(e => e.NullableA == val, cn, provider);

        // null literal vs column should expand to IS NULL
        Assert.Contains("IS NULL", sql);
    }

    [Fact]
    public void StringEqual_ExpandsToIsNullGuard()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.StringA == e.StringB, cn, provider);

        // String (reference type) equality should expand to three-valued logic with IS NULL guard
        Assert.Contains("IS NULL", sql);
        Assert.Contains("=", sql);
    }

    [Fact]
    public void StringNotEqual_ExpandsToIsNullGuards()
    {
        using var cn = CreateConn();
        var provider = new SqliteProvider();
        var (sql, _) = Translate<NullableEntity>(e => e.StringA != e.StringB, cn, provider);

        // String inequality should contain both IS NULL and IS NOT NULL checks
        Assert.Contains("IS NOT NULL", sql);
        Assert.Contains("IS NULL", sql);
        Assert.Contains("<>", sql);
    }
}
