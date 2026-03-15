using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using System.Linq;

namespace nORM.Tests;

/// <summary>
/// Verifies that materializing a DB NULL into a non-nullable value type member
/// throws rather than silently defaulting to 0/false/etc.
/// Verifies that enum and Nullable&lt;TEnum&gt; properties are correctly materialized
/// from integer DB values via the reflection fallback path.
/// </summary>
public class MaterializerNullHandlingTests
{
    [Table("NullTestEntity")]
    private class NullTestEntity
    {
        [Key] public int Id { get; set; }
        public int NonNullableInt { get; set; }   // must not be NULL in DB
        public int? NullableInt { get; set; }     // may be NULL in DB
        public string? OptionalText { get; set; } // reference type, may be NULL
    }

    private static async Task<DbConnection> CreateSchemaAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        // Insert a row with NULL in the NonNullableInt column (force it via raw SQL)
        cmd.CommandText = "CREATE TABLE NullTestEntity (Id INTEGER PRIMARY KEY, NonNullableInt INTEGER, NullableInt INTEGER, OptionalText TEXT);" +
                          "INSERT INTO NullTestEntity VALUES (1, NULL, NULL, NULL);";
        await cmd.ExecuteNonQueryAsync();
        return cn;
    }

    [Fact]
    public async Task Query_NonNullableIntWithDbNull_ThrowsOnMaterialization()
    {
        await using var cn = await CreateSchemaAsync();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Materializing should throw because NonNullableInt is non-nullable value type but DB has NULL.
        // The runtime wraps the InvalidOperationException in a NormException, so we check the chain.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var result = await ctx.Query<NullTestEntity>().ToListAsync();
        });

        // The exception (or one of its inner exceptions) should indicate a null-related error.
        // With the optimized materializer, the typed accessor (e.g. GetInt32) throws directly
        // rather than our custom message, so accept either form.
        var allMessages = GetExceptionChain(ex).Select(e => e.Message);
        Assert.Contains(allMessages, m =>
            m.Contains("DB column returned NULL for non-nullable") ||
            m.Contains("data is NULL"));
    }

    private static System.Collections.Generic.IEnumerable<Exception> GetExceptionChain(Exception ex)
    {
        var current = ex;
        while (current != null)
        {
            yield return current;
            current = current.InnerException;
        }
    }

    [Fact]
    public async Task Query_NullableIntWithDbNull_ReturnsNull()
    {
        // NullableInt is int?, so DB NULL should successfully map to null
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE NullTestEntity (Id INTEGER PRIMARY KEY, NonNullableInt INTEGER, NullableInt INTEGER, OptionalText TEXT);" +
                                "INSERT INTO NullTestEntity VALUES (1, 42, NULL, NULL);";
            await setup.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<NullTestEntity>().ToListAsync();
        Assert.Single(results);
        Assert.Null(results[0].NullableInt);
        Assert.Equal(42, results[0].NonNullableInt);
    }

    // Enum conversion tests

    private enum TestStatus { Active = 1, Inactive = 2, Pending = 3 }

    [Table("EnumEntity")]
    private class EnumEntity
    {
        [Key] public int Id { get; set; }
        public TestStatus Status { get; set; }
        public TestStatus? NullableStatus { get; set; }
    }

    [Fact]
    public async Task Query_EnumProperty_ConvertsIntToEnum()
    {
        // An int DB value must be correctly converted to an enum type
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE EnumEntity (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL, NullableStatus INTEGER);" +
                                "INSERT INTO EnumEntity VALUES (1, 2, 3);";
            await setup.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<EnumEntity>().ToListAsync();

        Assert.Single(results);
        Assert.Equal(TestStatus.Inactive, results[0].Status);
        Assert.Equal(TestStatus.Pending, results[0].NullableStatus);
    }

    [Fact]
    public async Task Query_NullableEnumProperty_WithDbNull_ReturnsNull()
    {
        // A DB NULL for Nullable<TEnum> must return null, not throw
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE EnumEntity (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL, NullableStatus INTEGER);" +
                                "INSERT INTO EnumEntity VALUES (1, 1, NULL);";
            await setup.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        var results = await ctx.Query<EnumEntity>().ToListAsync();

        Assert.Single(results);
        Assert.Equal(TestStatus.Active, results[0].Status);
        Assert.Null(results[0].NullableStatus);
    }
}
