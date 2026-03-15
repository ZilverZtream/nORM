using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that QueryUnchangedAsync uses name-based column ordinal resolution
/// rather than positional (index-based) mapping.
///
/// Root bug: CreateSyncMaterializer mapped reader column i to entity property i.
/// Raw SQL returning columns in a different order than the entity's mapping silently
/// swapped field values — no exception, just wrong data.
///
/// Fix: QueryUnchangedAsync now builds a name→ordinal dictionary from reader.GetName()
/// and populates each property from its correctly named column regardless of SQL order.
/// </summary>
public class RawSqlNameBasedMaterializationTests
{
    [Table("NamedCol")]
    private class NamedColEntity
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    [Table("MultiType")]
    private class MultiTypeEntity
    {
        [Key] public int Id { get; set; }
        public int Count { get; set; }
        public string Label { get; set; } = string.Empty;
        public double Score { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── Regression: column order mismatch ──────────────────────────────────

    /// <summary>
    /// M-1 regression: two string columns (Code, Name) returned in swapped order by
    /// the SQL must still land in the correct properties.
    ///
    /// Before the fix: positional mapping assigned Code←reader[0] and Name←reader[1].
    /// When the SQL returns (Name, Code, Id), reader[0]=Name value → Code property,
    /// reader[1]=Code value → Name property — silent field swap, no exception.
    /// </summary>
    [Fact]
    public async Task QueryUnchangedAsync_ColumnsInSwappedOrder_PopulatesCorrectProperties()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE NamedCol (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO NamedCol VALUES (1, 'C001', 'Alpha');");
        await using (cn) await using (ctx)
        {
            // SQL deliberately returns columns in a different order than the entity mapping.
            // The entity mapping order is: Id, Code, Name.
            // The SQL returns: Name, Code, Id.
            var results = await ctx.QueryUnchangedAsync<NamedColEntity>(
                "SELECT \"Name\", \"Code\", \"Id\" FROM \"NamedCol\"");

            Assert.Single(results);
            // M-1 fix: values must land in the property matching the column name, not the ordinal.
            Assert.Equal(1, results[0].Id);
            Assert.Equal("C001", results[0].Code);   // Before fix: "Alpha" (swapped)
            Assert.Equal("Alpha", results[0].Name);  // Before fix: "C001" (swapped)
        }
    }

    /// <summary>
    /// Reversed column order (Id last instead of first) must still populate correctly.
    /// </summary>
    [Fact]
    public async Task QueryUnchangedAsync_IdColumnLast_PopulatesCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE NamedCol (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO NamedCol VALUES (42, 'Z999', 'Zeta');");
        await using (cn) await using (ctx)
        {
            var results = await ctx.QueryUnchangedAsync<NamedColEntity>(
                "SELECT \"Code\", \"Name\", \"Id\" FROM \"NamedCol\"");

            Assert.Single(results);
            Assert.Equal(42, results[0].Id);
            Assert.Equal("Z999", results[0].Code);
            Assert.Equal("Zeta", results[0].Name);
        }
    }

    /// <summary>
    /// Mixed-type entity with columns in full reverse order must populate correctly.
    /// Before the fix, int←string and double←int would cause type errors or silently wrong values.
    /// </summary>
    [Fact]
    public async Task QueryUnchangedAsync_MixedTypes_ReversedOrder_PopulatesCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE MultiType (Id INTEGER PRIMARY KEY, Count INTEGER NOT NULL, " +
            "Label TEXT NOT NULL, Score REAL NOT NULL);" +
            "INSERT INTO MultiType VALUES (7, 100, 'Test', 3.14);");
        await using (cn) await using (ctx)
        {
            // SQL returns columns in reverse order relative to entity mapping.
            var results = await ctx.QueryUnchangedAsync<MultiTypeEntity>(
                "SELECT \"Score\", \"Label\", \"Count\", \"Id\" FROM \"MultiType\"");

            Assert.Single(results);
            Assert.Equal(7, results[0].Id);
            Assert.Equal(100, results[0].Count);
            Assert.Equal("Test", results[0].Label);
            Assert.Equal(3.14, results[0].Score, precision: 10);
        }
    }

    // ── Aligned order: verify normal case still works ────────────────────────

    /// <summary>
    /// When SQL returns columns in the same order as the entity mapping,
    /// behavior must be identical to before the fix (no regression on the happy path).
    /// </summary>
    [Fact]
    public async Task QueryUnchangedAsync_ColumnsInMappingOrder_StillWorksCorrectly()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE NamedCol (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO NamedCol VALUES (1, 'A', 'First');" +
            "INSERT INTO NamedCol VALUES (2, 'B', 'Second');");
        await using (cn) await using (ctx)
        {
            // Columns in the same order as entity: Id, Code, Name.
            var results = await ctx.QueryUnchangedAsync<NamedColEntity>(
                "SELECT \"Id\", \"Code\", \"Name\" FROM \"NamedCol\" ORDER BY \"Id\"");

            Assert.Equal(2, results.Count);
            Assert.Equal(1, results[0].Id); Assert.Equal("A", results[0].Code); Assert.Equal("First", results[0].Name);
            Assert.Equal(2, results[1].Id); Assert.Equal("B", results[1].Code); Assert.Equal("Second", results[1].Name);
        }
    }

    /// <summary>
    /// A missing column (present in mapping but absent from the SQL SELECT list) must
    /// throw an InvalidOperationException with a clear diagnostic message — not silently
    /// populate from the wrong column (which was the M-1 failure mode).
    /// </summary>
    [Fact]
    public async Task QueryUnchangedAsync_MissingColumn_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext(
            "CREATE TABLE NamedCol (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO NamedCol VALUES (1, 'X', 'Y');");
        await using (cn) await using (ctx)
        {
            // Omit the "Code" column — must fail with a clear message instead of silent default.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                ctx.QueryUnchangedAsync<NamedColEntity>(
                    "SELECT \"Id\", \"Name\" FROM \"NamedCol\""));

            Assert.Contains("Code", ex.Message);
        }
    }
}
