using System;
using System.Data.Common;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// MG-1: Unit tests for SqliteMigrationRunner.IsTableNotFoundError.
/// Validates that the method correctly distinguishes "no such table" errors (code 1 + message)
/// from other SQLITE_ERROR cases and falls back to message-only for non-SqliteException types.
/// </summary>
public class SqliteMigrationFaultDetectionTests
{
    // ── Helper to invoke private static IsTableNotFoundError ─────────────────

    private static readonly MethodInfo _isTableNotFound = typeof(SqliteMigrationRunner)
        .GetMethod("IsTableNotFoundError", BindingFlags.NonPublic | BindingFlags.Static)!;

    private static bool IsTableNotFoundError(DbException ex)
        => (bool)_isTableNotFound.Invoke(null, new object[] { ex })!;

    // ── Minimal concrete DbException for non-Sqlite fallback ─────────────────

    private sealed class GenericDbException : DbException
    {
        public GenericDbException(string message) : base(message) { }
    }

    // ── Factory helper for SqliteException with a specific error code ─────────

    private static SqliteException MakeSqliteException(int errorCode, string message)
    {
        // SqliteException constructor: (message, errorCode)
        return (SqliteException)Activator.CreateInstance(
            typeof(SqliteException),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            new object[] { message, errorCode },
            null)!;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// <summary>
    /// MG-1: SqliteException with code 1 and "no such table" message → true.
    /// This is the canonical "history table not yet created" signal.
    /// </summary>
    [Fact]
    public void IsTableNotFoundError_SqliteCode1_MatchingMessage_ReturnsTrue()
    {
        var ex = MakeSqliteException(1, "SQLite Error 1: 'no such table: __NormMigrationsHistory'.");
        Assert.True(IsTableNotFoundError(ex));
    }

    /// <summary>
    /// MG-1: SqliteException with code 1 but a different message (syntax error) → false.
    /// The AND condition must exclude other SQLITE_ERROR sub-cases.
    /// </summary>
    [Fact]
    public void IsTableNotFoundError_SqliteCode1_DifferentMessage_ReturnsFalse()
    {
        var ex = MakeSqliteException(1, "SQLite Error 1: 'syntax error near \"WHERE\"'.");
        Assert.False(IsTableNotFoundError(ex));
    }

    /// <summary>
    /// MG-1: SqliteException with "no such table" in the message but a non-1 error code → false.
    /// Error code 14 = SQLITE_CANTOPEN; the message matching alone is insufficient.
    /// </summary>
    [Fact]
    public void IsTableNotFoundError_SqliteCodeNon1_MatchingMessage_ReturnsFalse()
    {
        var ex = MakeSqliteException(14, "no such table: something");
        Assert.False(IsTableNotFoundError(ex));
    }

    /// <summary>
    /// MG-1: A generic (non-Sqlite) DbException with "no such table" in the message → true.
    /// The fallback path keeps the method useful for future providers.
    /// </summary>
    [Fact]
    public void IsTableNotFoundError_NonSqliteDbException_MatchingMessage_ReturnsTrue()
    {
        var ex = new GenericDbException("no such table: __NormMigrationsHistory");
        Assert.True(IsTableNotFoundError(ex));
    }

    /// <summary>
    /// MG-1: A generic (non-Sqlite) DbException without matching message → false.
    /// </summary>
    [Fact]
    public void IsTableNotFoundError_NonSqliteDbException_NonMatchingMessage_ReturnsFalse()
    {
        var ex = new GenericDbException("connection timeout");
        Assert.False(IsTableNotFoundError(ex));
    }
}
