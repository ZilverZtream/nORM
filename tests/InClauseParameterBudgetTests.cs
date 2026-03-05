using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQL-1: Verifies that the pessimistic 20% parameter reserve has been removed.
/// The old code rejected queries where items.Count > MaxParameters * 0.8 even when
/// the actual remaining budget was sufficient. The fix uses exact accounting.
/// </summary>
public class InClauseParameterBudgetTests : TestBase
{
    [Table("BudgetItem")]
    private class BudgetItem
    {
        [Key]
        public int Id { get; set; }
    }

    // ─── Near-ceiling tests ────────────────────────────────────────────────

    [Fact]
    public void LocalContains_NearParameterCeiling_IsNotRejected()
    {
        // SQLite MaxParameters = 999. With the old 20% reserve only 799 items were allowed.
        // With exact accounting, 800 items should succeed (no params used before this IN clause).
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var collection = Enumerable.Range(1, 800).ToArray();
        // Should NOT throw — 800 < 999 remaining params
        var (sql, parameters) = Translate<BudgetItem>(
            e => collection.Contains(e.Id),
            connection, provider);

        Assert.Contains("IN (", sql);
        Assert.Equal(800, parameters.Count);
    }

    [Fact]
    public void LocalContains_ExceedingHardLimit_IsRejected()
    {
        // SQLite MaxParameters = 999. A list of 1000 items exceeds the hard limit.
        // Note: Translate() uses reflection, so exceptions are wrapped in TargetInvocationException.
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var collection = Enumerable.Range(1, 1000).ToArray();
        var ex = Record.Exception(() =>
            Translate<BudgetItem>(e => collection.Contains(e.Id), connection, provider));

        // Unwrap TargetInvocationException from reflection
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException : ex;
        Assert.IsType<NormQueryException>(inner);
        Assert.Contains("exceeds remaining parameter budget", inner!.Message);
    }

    [Fact]
    public void LocalContains_LargeListWithMySql_IsNotRejected()
    {
        // MySQL MaxParameters = 65535. A 50000-item list should be allowed.
        var setup = CreateProvider(ProviderKind.MySql);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var collection = Enumerable.Range(1, 50_000).ToArray();
        // Should NOT throw
        var (sql, parameters) = Translate<BudgetItem>(
            e => collection.Contains(e.Id),
            connection, provider);

        Assert.Contains("IN (", sql);
        Assert.Equal(50_000, parameters.Count);
    }

    [Fact]
    public void LocalContains_WithNullsNearCeiling_NullsNotCountedAsParams()
    {
        // Nulls in the collection don't use parameters. With old code, items.Count (including
        // nulls) was used for the budget check, causing false rejections. Verify 800 non-nulls
        // + 10 nulls are allowed on SQLite (total 810 items, 800 params used).
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        int?[] collection = [.. Enumerable.Range(1, 800).Cast<int?>(), .. Enumerable.Repeat<int?>(null, 10)];
        var (sql, parameters) = Translate<BudgetItem>(
            e => collection.Contains(e.Id),
            connection, provider);

        Assert.Contains("IN (", sql);
        Assert.Contains("IS NULL", sql);
        Assert.Equal(800, parameters.Count); // Only non-null items become params
    }
}
