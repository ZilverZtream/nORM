using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Provider-parity tests for SQL shape and dialect properties across all four providers.
/// These tests verify provider-specific SQL generation without requiring a live database
/// connection for non-SQLite providers.
/// </summary>
public class AsyncCancellationProviderParityTests
{
    // ── Test entity ───────────────────────────────────────────────────────────

    [Table("ParityEntity")]
    private class ParityEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static nORM.Mapping.TableMapping GetMapping<T>() where T : class
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        return ctx.GetMapping(typeof(T));
    }

    // ── SQL shape tests per provider ─────────────────────────────────────────

    /// <summary>
    /// MySQL identity retrieval SQL must reference LAST_INSERT_ID().
    /// </summary>
    [Fact]
    public void MySqlProvider_IdentityRetrieval_UsesLastInsertId()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var mapping = GetMapping<ParityEntity>();
        var sql = provider.GetIdentityRetrievalString(mapping);

        Assert.Contains("LAST_INSERT_ID", sql, System.StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// PostgreSQL identity retrieval SQL must reference RETURNING (PostgreSQL clause).
    /// </summary>
    [Fact]
    public void PostgresProvider_IdentityRetrieval_UsesReturning()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var mapping = GetMapping<ParityEntity>();
        var sql = provider.GetIdentityRetrievalString(mapping);

        Assert.Contains("RETURNING", sql, System.StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// SQL Server uses OUTPUT INSERTED (in the prefix clause, placed before VALUES) rather than
    /// SCOPE_IDENTITY (postfix). GetIdentityRetrievalPrefix contains the OUTPUT INSERTED clause;
    /// GetIdentityRetrievalString returns empty (no postfix needed).
    /// </summary>
    [Fact]
    public void SqlServerProvider_IdentityRetrieval_UsesScopeIdentityOrOutputInserted()
    {
        var provider = new SqlServerProvider();
        var mapping = GetMapping<ParityEntity>();

        // Suffix is now empty; OUTPUT INSERTED is in the prefix
        var suffix = provider.GetIdentityRetrievalString(mapping);
        var prefix = provider.GetIdentityRetrievalPrefix(mapping);

        Assert.Equal(string.Empty, suffix);
        Assert.Contains("OUTPUT INSERTED", prefix, System.StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// MySQL GetConcatSql must use the CONCAT() function (MySQL does not support || for strings by default).
    /// </summary>
    [Fact]
    public void MySqlProvider_GetConcatSql_UsesConcat_Function()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        var sql = provider.GetConcatSql("a", "b");

        Assert.Contains("CONCAT", sql, System.StringComparison.OrdinalIgnoreCase);
    }

    // ── Provider SQL generation parity ───────────────────────────────────────

    /// <summary>
    /// PostgreSQL requires "true"/"false" boolean literals (not "1"/"0").
    /// </summary>
    [Fact]
    public void PostgresProvider_BooleanLiterals_AreLowercaseTrueFalse()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());

        Assert.Equal("true", provider.BooleanTrueLiteral);
        Assert.Equal("false", provider.BooleanFalseLiteral);
    }

    /// <summary>
    /// MySQL inherits the base "1"/"0" boolean literals.
    /// </summary>
    [Fact]
    public void MySqlProvider_BooleanTrueLiteral_IsOne()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());

        Assert.Equal("1", provider.BooleanTrueLiteral);
    }

    /// <summary>
    /// SQL Server uses "1"/"0" boolean literals.
    /// </summary>
    [Fact]
    public void SqlServerProvider_BooleanTrueLiteral_IsOne()
    {
        var provider = new SqlServerProvider();

        Assert.Equal("1", provider.BooleanTrueLiteral);
    }

    /// <summary>
    /// PostgreSQL NullSafeEqual must use IS NOT DISTINCT FROM for index-friendly null-safe equality.
    /// </summary>
    [Fact]
    public void PostgresProvider_NullSafeEqual_UsesIsNotDistinctFrom()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var sql = provider.NullSafeEqual("a", "b");

        Assert.Contains("IS NOT DISTINCT FROM", sql, System.StringComparison.OrdinalIgnoreCase);
    }
}
