using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that DateTime parameter DbType is consistent (DateTime2) across both the
/// ParameterOptimizer (AddOptimizedParam) and ParameterAssign (AssignValue) pipelines
/// so that compiled/pooled and non-pooled execution paths produce identical parameter metadata.
/// </summary>
public class DateTimeDbTypePrecisionTests
{
    [Fact]
    public void ParameterAssign_DateTime_SetsDbTypeDateTime2()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();
        p.ParameterName = "@dt";

        ParameterAssign.AssignValue(p, new DateTime(2024, 1, 15, 12, 30, 45, 999));

        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    [Fact]
    public void ParameterOptimizer_DateTime_SetsDbTypeDateTime2()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();

        nORM.Internal.ParameterOptimizer.AddOptimizedParam(cmd, "@dt", new DateTime(2024, 1, 15, 12, 30, 45, 999));

        var p = cmd.Parameters["@dt"];
        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    [Fact]
    public void ParameterAssign_DateTime_SetsDbTypeDateTime2_MatchesOptimizer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();

        // Optimizer path.
        nORM.Internal.ParameterOptimizer.AddOptimizedParam(cmd, "@opt", new DateTime(2024, 6, 1, 8, 0, 0));
        var optimizerDbType = cmd.Parameters["@opt"].DbType;

        // AssignValue path (used by compiled/pooled queries to update parameter values).
        var reusedParam = cmd.CreateParameter();
        reusedParam.ParameterName = "@assign";
        ParameterAssign.AssignValue(reusedParam, new DateTime(2024, 6, 1, 8, 0, 0));
        var assignDbType = reusedParam.DbType;

        Assert.Equal(optimizerDbType, assignDbType);
    }

    [Fact]
    public void ParameterAssign_NullableDateTime_SetsDbTypeDateTime2()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();

        // Nullable DateTime unwrapped to DateTime at runtime.
        DateTime? dt = new DateTime(2025, 3, 1);
        var p = cmd.CreateParameter();
        p.ParameterName = "@dt";
        ParameterAssign.AssignValue(p, (object)dt.Value);

        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    /// <summary>
    /// Simulates the compiled/pooled path reusing a parameter that was previously set
    /// to a different DbType. After AssignValue assigns a DateTime, DbType must be
    /// DateTime2 regardless of the prior state of the parameter.
    /// </summary>
    [Fact]
    public void ParameterAssign_ReusedParam_PriorStateDoesNotAffectDateTime2()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();
        p.ParameterName = "@dt";

        // First assignment is an int (simulates prior query reuse).
        ParameterAssign.AssignValue(p, 42);
        Assert.Equal(DbType.Int32, p.DbType);

        // Second assignment is DateTime — must override to DateTime2.
        ParameterAssign.AssignValue(p, new DateTime(2024, 12, 31, 23, 59, 59, 999));
        Assert.Equal(DbType.DateTime2, p.DbType);
    }

    [Fact]
    public void ParameterAssign_DateTimeOffset_SetsDbTypeDateTimeOffset()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        var p = cmd.CreateParameter();
        p.ParameterName = "@dto";

        var dto = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.FromHours(2));
        ParameterAssign.AssignValue(p, dto);

        Assert.Equal(DbType.DateTimeOffset, p.DbType);
    }

    [Fact]
    public void ParameterOptimizer_DateTimeOffset_SetsDbTypeDateTimeOffset()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();

        var dto = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.FromHours(2));
        nORM.Internal.ParameterOptimizer.AddOptimizedParam(cmd, "@dto", dto);

        Assert.Equal(DbType.DateTimeOffset, cmd.Parameters["@dto"].DbType);
    }

    /// <summary>
    /// DateTimeOffset DbType is consistent across both pipelines (matches DateTime2 parity check).
    /// </summary>
    [Fact]
    public void ParameterAssign_DateTimeOffset_MatchesOptimizer()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();

        var dto = new DateTimeOffset(2024, 3, 15, 0, 0, 0, TimeSpan.Zero);
        nORM.Internal.ParameterOptimizer.AddOptimizedParam(cmd, "@opt", dto);
        var optimizerDbType = cmd.Parameters["@opt"].DbType;

        var p = cmd.CreateParameter();
        ParameterAssign.AssignValue(p, dto);
        var assignDbType = p.DbType;

        Assert.Equal(optimizerDbType, assignDbType);
    }
}
