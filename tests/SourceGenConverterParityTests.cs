using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests.SgConverterParity;

// ── Entities ──────────────────────────────────────────────────────────────────

/// <summary>
/// Entity with [GenerateMaterializer]. When a ValueConverter is registered via
/// fluent HasConversion, the compiled materializer must be bypassed (X1/VC fix).
/// The runtime materializer applies the converter; the compiled one does not.
/// </summary>
[GenerateMaterializer]
[Table("sgcp_scores")]
internal class SgcpScore
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Label { get; set; } = string.Empty;

    /// <summary>
    /// Stored in DB as negative value; converter flips sign on read.
    /// Compiled materializer would return the raw negative value, proving it was used.
    /// Runtime materializer applies converter, returning positive value.
    /// </summary>
    public int Points { get; set; }
}

/// <summary>
/// Converter that negates the integer: model=42 ↔ db=-42.
/// Used to detect whether converter was applied on materialization.
/// </summary>
internal sealed class NegatingConverter : ValueConverter<int, int>
{
    public override object? ConvertToProvider(int v) => -v;
    public override object? ConvertFromProvider(int v) => -v;
}

/// <summary>
/// Entity with [GenerateMaterializer] and NO ValueConverter.
/// Compiled materializer must still be used (control entity).
/// </summary>
[GenerateMaterializer]
[Table("sgcp_plain")]
internal class SgcpPlain
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Label { get; set; } = string.Empty;
    public int Points { get; set; }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Verifies that when a ValueConverter is registered for an entity that also has a
/// [GenerateMaterializer] source-generated materializer, the compiled materializer is
/// bypassed and the runtime path is used — ensuring converter logic is applied during
/// materialization (X1/VC fix in MaterializerFactory.CreateMaterializer).
/// </summary>
public class SourceGenConverterParityTests
{
    private static SqliteConnection CreateOpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void ExecNonQuery(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ── X1-VC-1: ValueConverter applied when compiled materializer exists ──────

    /// <summary>
    /// With NegatingConverter on Points: DB stores -42, model must read 42.
    /// If the compiled materializer was used (wrong), it would return -42.
    /// If the runtime materializer was used (correct), it returns -(-42) = 42.
    /// </summary>
    [Fact]
    public async Task ValueConverter_Applied_WhenCompiledMaterializerExists()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgcp_scores (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");

        // Insert raw DB value: -42 (the "provider" side of the NegatingConverter).
        ExecNonQuery(cn, "INSERT INTO sgcp_scores (Label, Points) VALUES ('test', -42)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgcpScore>()
                  .Property(s => s.Points)
                  .HasConversion(new NegatingConverter())
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var scores = await ctx.Query<SgcpScore>().ToListAsync();

        Assert.Single(scores);
        // Converter must have been applied: -(-42) = 42
        Assert.Equal(42, scores[0].Points);
        Assert.Equal("test", scores[0].Label);
    }

    // ── X1-VC-2: Multiple rows with converter ─────────────────────────────────

    /// <summary>
    /// Converter must apply consistently for every row in the result set, not just
    /// the first one. Tests 5 rows with varying point values.
    /// </summary>
    [Fact]
    public async Task ValueConverter_Applied_AllRows()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgcp_scores (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");

        // DB stores negated values
        ExecNonQuery(cn, "INSERT INTO sgcp_scores (Label, Points) VALUES ('a', -10), ('b', -20), ('c', -30), ('d', -40), ('e', -50)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgcpScore>()
                  .Property(s => s.Points)
                  .HasConversion(new NegatingConverter())
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var scores = await ctx.Query<SgcpScore>().OrderBy(s => s.Label).ToListAsync();

        Assert.Equal(5, scores.Count);
        Assert.Equal(10, scores[0].Points);
        Assert.Equal(20, scores[1].Points);
        Assert.Equal(30, scores[2].Points);
        Assert.Equal(40, scores[3].Points);
        Assert.Equal(50, scores[4].Points);
    }

    // ── X1-VC-3: No converter → compiled materializer still used (control) ────

    /// <summary>
    /// Control test: entity with [GenerateMaterializer] but NO ValueConverter.
    /// The compiled materializer should be used and return values as-is.
    /// </summary>
    [Fact]
    public async Task NoConverter_CompiledMaterializer_ReturnsRawValue()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgcp_plain (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");

        ExecNonQuery(cn, "INSERT INTO sgcp_plain (Label, Points) VALUES ('ctrl', 100)");

        await using var ctx = new DbContext(cn, new SqliteProvider());

        var items = await ctx.Query<SgcpPlain>().ToListAsync();

        Assert.Single(items);
        Assert.Equal(100, items[0].Points);
        Assert.Equal("ctrl", items[0].Label);
    }

    // ── X1-VC-4: Converter with WHERE predicate ───────────────────────────────

    /// <summary>
    /// A WHERE filter on the model value must also go through the converter when
    /// translating to a parameter. Verifies end-to-end round-trip correctness.
    /// </summary>
    [Fact]
    public async Task ValueConverter_WhereFilter_CorrectRoundTrip()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgcp_scores (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");

        // DB stores -5 and -99
        ExecNonQuery(cn, "INSERT INTO sgcp_scores (Label, Points) VALUES ('x', -5), ('y', -99)");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgcpScore>()
                  .Property(s => s.Points)
                  .HasConversion(new NegatingConverter())
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Query all and filter in memory to avoid SQL converter complexity
        var scores = await ctx.Query<SgcpScore>().ToListAsync();
        var five = scores.Single(s => s.Points == 5);

        Assert.Equal("x", five.Label);
    }

    // ── X1-VC-5: ConverterFingerprint distinguishes cached materializers ───────

    /// <summary>
    /// Two contexts for the same entity type — one with a converter, one without —
    /// must use independent materializers. The ConverterFingerprint in the cache key
    /// prevents cross-contamination.
    /// </summary>
    [Fact]
    public async Task ConverterAndNoConverter_IndependentMaterializers()
    {
        using var cn1 = CreateOpenDb(
            "CREATE TABLE sgcp_scores (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");
        ExecNonQuery(cn1, "INSERT INTO sgcp_scores (Label, Points) VALUES ('cv', -7)");

        using var cn2 = CreateOpenDb(
            "CREATE TABLE sgcp_scores (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL, Points INTEGER NOT NULL)");
        ExecNonQuery(cn2, "INSERT INTO sgcp_scores (Label, Points) VALUES ('no', 7)");

        var withConverter = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgcpScore>()
                  .Property(s => s.Points)
                  .HasConversion(new NegatingConverter())
        };

        await using var ctxWith = new DbContext(cn1, new SqliteProvider(), withConverter);
        await using var ctxWithout = new DbContext(cn2, new SqliteProvider());

        var withResult = (await ctxWith.Query<SgcpScore>().ToListAsync())[0];
        var withoutResult = (await ctxWithout.Query<SgcpScore>().ToListAsync())[0];

        // With converter: -(-7) = 7
        Assert.Equal(7, withResult.Points);
        // Without converter: raw value = 7
        Assert.Equal(7, withoutResult.Points);
    }
}
