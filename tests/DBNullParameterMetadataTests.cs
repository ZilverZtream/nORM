using System;
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

// ══════════════════════════════════════════════════════════════════════════════
// P1/X1 — DBNull.Value metadata normalization in AddOptimizedParam (3.8→4.0)
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that <c>AddOptimizedParam</c> correctly normalises <c>DBNull.Value</c> to the
/// null path (DbType.Object), preventing type-metadata contamination when compiled-query
/// parameter rebinding passes <c>paramValues[i] ?? DBNull.Value</c>.
///
/// P1/X1 root cause: <c>AddOptimizedParam</c>'s null guard was <c>if (value == null)</c>.
/// <c>DBNull.Value</c> is a singleton of type <c>System.DBNull</c>, not reference-equal to
/// <c>null</c>, so it fell through to the non-null type-dispatch branch. <c>System.DBNull</c>
/// has no entry in <c>_typeMap</c>, leaving <c>param.DbType</c> at the ADO.NET provider
/// default (typically <c>DbType.String</c> for Microsoft.Data.Sqlite), causing wrong type
/// metadata for null compiled-parameter bindings.
///
/// Fix: added <c>if (value is DBNull) value = null;</c> at the top of <c>AddOptimizedParam</c>,
/// routing <c>DBNull.Value</c> to the same null path as <c>null</c>.
/// </summary>
public class DBNullParameterMetadataTests
{
    // ── Unit tests for AddOptimizedParam DBNull.Value normalisation ───────────

    private static DbCommand MakeCmd()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        return cn.CreateCommand();
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_SetsDbTypeObject()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value);
        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_Null_SetsDbTypeObject()
    {
        // Regression: existing null path must still yield DbType.Object.
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", null);
        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_WithKnownTypeInt_SetsDbTypeInt32()
    {
        // When a knownType is supplied alongside DBNull.Value, the mapped DbType must be used.
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value, typeof(int));
        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.Int32, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_WithKnownTypeString_SetsDbTypeString()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value, typeof(string));
        var p = cmd.Parameters[0];
        Assert.Equal(DBNull.Value, p.Value);
        Assert.Equal(DbType.String, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_WithKnownTypeGuid_SetsDbTypeGuid()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value, typeof(Guid));
        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Guid, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_WithKnownTypeDateTime_SetsDbTypeDateTime2()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value, typeof(DateTime));
        var p = cmd.Parameters[0];
        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    [Fact]
    public void AddOptimizedParam_DBNullValue_WithKnownTypeBool_SetsDbTypeBoolean()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", DBNull.Value, typeof(bool));
        var p = cmd.Parameters[0];
        Assert.Equal(DbType.Boolean, p.DbType);
    }

    // ── Confirm that non-null values are unaffected by the fix ───────────────

    [Fact]
    public void AddOptimizedParam_NonNullInt_SetsDbTypeInt32()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", 42);
        Assert.Equal(DbType.Int32, cmd.Parameters[0].DbType);
        Assert.Equal(42, cmd.Parameters[0].Value);
    }

    [Fact]
    public void AddOptimizedParam_NonNullString_SetsDbTypeStringAndSize()
    {
        using var cmd = MakeCmd();
        cmd.AddOptimizedParam("@p", "hello");
        var p = cmd.Parameters[0];
        Assert.Equal(DbType.String, p.DbType);
        Assert.Equal(5, p.Size);
    }

    // ── End-to-end: compiled query with nullable parameter = null ─────────────

    [Table("P1NullEntity")]
    private class P1NullEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? Score { get; set; }
    }

    private static (SqliteConnection cn, DbContext ctx) BuildContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE P1NullEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Score INTEGER);" +
            "INSERT INTO P1NullEntity (Name, Score) VALUES ('Alice', 10);" +
            "INSERT INTO P1NullEntity (Name, Score) VALUES (NULL, 20);" +
            "INSERT INTO P1NullEntity (Name, Score) VALUES ('Bob', NULL);";
        setup.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    [Fact]
    public async Task CompiledQuery_NullStringParam_ReturnsNullNameRows()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx; using var __ = cn;

        string? nameFilter = null;
        var results = await ctx.Query<P1NullEntity>()
            .Where(e => e.Name == nameFilter)
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].Name);
        Assert.Equal(20, results[0].Score);
    }

    [Fact]
    public async Task CompiledQuery_NullIntParam_ReturnsNullScoreRows()
    {
        var (cn, ctx) = BuildContext();
        await using var _ = ctx; using var __ = cn;

        int? scoreFilter = null;
        var results = await ctx.Query<P1NullEntity>()
            .Where(e => e.Score == scoreFilter)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
        Assert.Null(results[0].Score);
    }

    [Fact]
    public async Task CompiledQuery_NullParam_DoesNotThrowBindingError()
    {
        // The key regression: before the fix, DBNull.Value in AddOptimizedParam
        // left DbType as provider default (String for SQLite), causing a type mismatch
        // when the column is INTEGER. This test verifies no exception is thrown.
        var (cn, ctx) = BuildContext();
        await using var _ = ctx; using var __ = cn;

        int? scoreFilter = null;
        var ex = await Record.ExceptionAsync(() =>
            ctx.Query<P1NullEntity>().Where(e => e.Score == scoreFilter).ToListAsync());

        Assert.Null(ex);
    }

    // ── Null parameter type matrix ────────────────────────────────────────────

    [Table("P1TypeMatrix")]
    private class P1TypeMatrix
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string? StrCol { get; set; }
        public int? IntCol { get; set; }
        public Guid? GuidCol { get; set; }
        public DateTime? DtCol { get; set; }
        public bool? BoolCol { get; set; }
        public double? DblCol { get; set; }
    }

    private static (SqliteConnection cn, DbContext ctx) BuildMatrixContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE P1TypeMatrix (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "StrCol TEXT, IntCol INTEGER, GuidCol TEXT, DtCol TEXT, BoolCol INTEGER, DblCol REAL);" +
            "INSERT INTO P1TypeMatrix (StrCol, IntCol, GuidCol, DtCol, BoolCol, DblCol) " +
            "  VALUES ('val', 1, '00000000-0000-0000-0000-000000000001', '2024-01-01', 1, 1.5);" +
            "INSERT INTO P1TypeMatrix (StrCol, IntCol, GuidCol, DtCol, BoolCol, DblCol) " +
            "  VALUES (NULL, NULL, NULL, NULL, NULL, NULL);";
        setup.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    [Fact]
    public async Task NullMatrix_NullStringFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        string? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.StrCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].StrCol);
    }

    [Fact]
    public async Task NullMatrix_NullIntFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        int? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.IntCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].IntCol);
    }

    [Fact]
    public async Task NullMatrix_NullGuidFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        Guid? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.GuidCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].GuidCol);
    }

    [Fact]
    public async Task NullMatrix_NullDateTimeFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        DateTime? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.DtCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].DtCol);
    }

    [Fact]
    public async Task NullMatrix_NullBoolFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        bool? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.BoolCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].BoolCol);
    }

    [Fact]
    public async Task NullMatrix_NullDoubleFilter_ReturnsNullRow()
    {
        var (cn, ctx) = BuildMatrixContext();
        await using var _ = ctx; using var __ = cn;
        double? v = null;
        var r = await ctx.Query<P1TypeMatrix>().Where(e => e.DblCol == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].DblCol);
    }

    // ── Enum null parameter matrix ────────────────────────────────────────────

    private enum Status { Active = 1, Inactive = 2 }

    [Table("P1EnumMatrix")]
    private class P1EnumMatrix
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int? StatusCode { get; set; }  // stored as int for SQLite
        public string? Tag { get; set; }
    }

    [Fact]
    public async Task NullMatrix_NullEnumFilter_ReturnsNullRow()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE P1EnumMatrix (Id INTEGER PRIMARY KEY AUTOINCREMENT, StatusCode INTEGER, Tag TEXT);" +
            "INSERT INTO P1EnumMatrix (StatusCode, Tag) VALUES (1, 'active');" +
            "INSERT INTO P1EnumMatrix (StatusCode, Tag) VALUES (NULL, 'none');";
        setup.ExecuteNonQuery();
        await using var ctx = new DbContext(cn, new SqliteProvider());
        using var _cn = cn;

        int? v = null;
        var r = await ctx.Query<P1EnumMatrix>().Where(e => e.StatusCode == v).ToListAsync();
        Assert.Single(r);
        Assert.Null(r[0].StatusCode);
        Assert.Equal("none", r[0].Tag);
    }
}
