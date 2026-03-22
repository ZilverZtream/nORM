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

// ── Entities ─────────────────────────────────────────────────────────────────

file enum PistHue { Red = 0, Green = 1, Blue = 2 }

// For cross-provider parity tests: manually-assigned key (no [DatabaseGenerated])
// so the INSERT SQL does not include provider-specific identity-retrieval clauses.
[Table("PistParityItem")]
file class PistParityItem
{
    [Key]
    public int Id { get; set; }
    public DateOnly BirthDate { get; set; }
    public Guid Token { get; set; }
    public PistHue Color { get; set; }
}

[Table("PistItem")]
file class PistItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public DateOnly BirthDate { get; set; }
    public TimeOnly StartTime { get; set; }
    public PistHue Color { get; set; }
    public Guid Token { get; set; }
}

[Table("PistNullableItem")]
file class PistNullableItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public DateOnly? BirthDate { get; set; }
    public TimeOnly? StartTime { get; set; }
    public PistHue? Color { get; set; }
    public Guid? Token { get; set; }
}

/// <summary>
/// Verifies that PreparedInsertCommand correctly binds special .NET types
/// (DateOnly, TimeOnly, enum, Guid, typed nulls) via ParameterAssign.AssignValue.
///
/// Root cause of the X1 bug: PreparedInsertCommand.ExecuteAsync used
/// <c>param.Value = value ?? DBNull.Value</c>, bypassing ParameterAssign.AssignValue
/// which sets DbType, Size=-1, Precision, and Scale. For text-bound types
/// (DateOnly, TimeOnly, Guid) in Microsoft.Data.Sqlite, Size=0 (the default for a
/// new DbParameter) truncates the text representation to an empty string, producing
/// corrupt stored values.
/// </summary>
public class PreparedInsertSpecialTypeTests
{
    private static (SqliteConnection Cn, DbContext Ctx) CreatePistDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE PistItem (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    BirthDate TEXT NOT NULL,
    StartTime TEXT NOT NULL,
    Color INTEGER NOT NULL,
    Token TEXT NOT NULL);
CREATE TABLE PistNullableItem (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    BirthDate TEXT,
    StartTime TEXT,
    Color INTEGER,
    Token TEXT);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── DateOnly ─────────────────────────────────────────────────────────────

    /// <summary>
    /// InsertAsync with a DateOnly column must store the date in ISO-8601 format,
    /// not as an empty string caused by Size=0 truncation.
    /// </summary>
    [Fact]
    public async Task InsertAsync_DateOnly_RoundTripsViaQuery()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var expected = new DateOnly(2024, 7, 15);
        var entity = new PistItem { BirthDate = expected, StartTime = new TimeOnly(9, 0), Color = PistHue.Green, Token = Guid.NewGuid() };
        await ctx.InsertAsync(entity);

        var rows = await ctx.Query<PistItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected, rows[0].BirthDate);
    }

    // ── TimeOnly ─────────────────────────────────────────────────────────────

    /// <summary>
    /// InsertAsync with a TimeOnly column must store the time in text form,
    /// not truncated to empty string.
    /// </summary>
    [Fact]
    public async Task InsertAsync_TimeOnly_RoundTripsViaQuery()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var expected = new TimeOnly(14, 30, 45);
        var entity = new PistItem { BirthDate = new DateOnly(2024, 1, 1), StartTime = expected, Color = PistHue.Red, Token = Guid.NewGuid() };
        await ctx.InsertAsync(entity);

        var rows = await ctx.Query<PistItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected.Hour,   rows[0].StartTime.Hour);
        Assert.Equal(expected.Minute, rows[0].StartTime.Minute);
        Assert.Equal(expected.Second, rows[0].StartTime.Second);
    }

    // ── Enum ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// InsertAsync with an enum column must store and retrieve the integer value.
    /// </summary>
    [Fact]
    public async Task InsertAsync_Enum_RoundTripsViaQuery()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var entity = new PistItem { BirthDate = new DateOnly(2024, 1, 1), StartTime = new TimeOnly(0, 0), Color = PistHue.Blue, Token = Guid.NewGuid() };
        await ctx.InsertAsync(entity);

        var rows = await ctx.Query<PistItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(PistHue.Blue, rows[0].Color);
    }

    // ── Guid ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// InsertAsync with a Guid column must store the full UUID text,
    /// not an empty string caused by Size=0 truncation.
    /// </summary>
    [Fact]
    public async Task InsertAsync_Guid_RoundTripsViaQuery()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var expected = Guid.NewGuid();
        var entity = new PistItem { BirthDate = new DateOnly(2024, 1, 1), StartTime = new TimeOnly(0, 0), Color = PistHue.Red, Token = expected };
        await ctx.InsertAsync(entity);

        var rows = await ctx.Query<PistItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected, rows[0].Token);
    }

    // ── Nullable types ────────────────────────────────────────────────────────

    /// <summary>
    /// InsertAsync with a null DateOnly? must bind DBNull, not a truncated empty string.
    /// </summary>
    [Fact]
    public async Task InsertAsync_NullableDateOnly_NullValue_BindsDbNull()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var entity = new PistNullableItem { BirthDate = null };
        await ctx.InsertAsync(entity);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT BirthDate FROM PistNullableItem WHERE Id = " + entity.Id;
        var stored = check.ExecuteScalar();
        Assert.True(stored is null or DBNull, $"Expected DBNull, got: {stored}");
    }

    /// <summary>
    /// InsertAsync with a null Guid? must bind DBNull.
    /// </summary>
    [Fact]
    public async Task InsertAsync_NullableGuid_NullValue_BindsDbNull()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var entity = new PistNullableItem { Token = null };
        await ctx.InsertAsync(entity);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Token FROM PistNullableItem WHERE Id = " + entity.Id;
        var stored = check.ExecuteScalar();
        Assert.True(stored is null or DBNull, $"Expected DBNull, got: {stored}");
    }

    /// <summary>
    /// InsertAsync with a null enum? must bind DBNull.
    /// </summary>
    [Fact]
    public async Task InsertAsync_NullableEnum_NullValue_BindsDbNull()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var entity = new PistNullableItem { Color = null };
        await ctx.InsertAsync(entity);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Color FROM PistNullableItem WHERE Id = " + entity.Id;
        var stored = check.ExecuteScalar();
        Assert.True(stored is null or DBNull, $"Expected DBNull, got: {stored}");
    }

    // ── Cache reuse across consecutive inserts ────────────────────────────────

    /// <summary>
    /// PreparedInsertCommand is reused across consecutive InsertAsync calls.
    /// Each reuse must correctly bind the current entity's values via
    /// ParameterAssign.AssignValue — not carry over stale DbType/Size metadata
    /// from the previous call.
    /// </summary>
    [Fact]
    public async Task InsertAsync_ConsecutiveRows_EachRoundTripsCorrectly()
    {
        var (cn, ctx) = CreatePistDb();
        await using var _ = ctx;
        using var __ = cn;

        var dates  = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 6, 15), new DateOnly(2024, 12, 31) };
        var tokens = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var colors = new[] { PistHue.Red, PistHue.Green, PistHue.Blue };

        for (int i = 0; i < 3; i++)
        {
            await ctx.InsertAsync(new PistItem
            {
                BirthDate = dates[i],
                StartTime = new TimeOnly(i, 0),
                Color     = colors[i],
                Token     = tokens[i]
            });
        }

        var rows = await ctx.Query<PistItem>().ToListAsync();
        Assert.Equal(3, rows.Count);

        // Sort by Id to match insertion order
        rows.Sort((a, b) => a.Id.CompareTo(b.Id));
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(dates[i],  rows[i].BirthDate);
            Assert.Equal(colors[i], rows[i].Color);
            Assert.Equal(tokens[i], rows[i].Token);
        }
    }

    // ── Cross-provider parity (TA1 fix) ───────────────────────────────────────

    private static nORM.Providers.DatabaseProvider MakeParityProvider(string kind) => kind switch
    {
        "sqlite"    => new SqliteProvider(),
        "mysql"     => new MySqlProvider(new SqliteParameterFactory()),
        "postgres"  => new PostgresProvider(new SqliteParameterFactory()),
        "sqlserver" => new SqlServerProvider(),
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    /// <summary>
    /// The PreparedInsert parameter binding fix (ParameterAssign.AssignValue) must
    /// work correctly with all four provider dialects using the SQLite engine.
    /// DateOnly round-trips correctly regardless of the SQL INSERT dialect used.
    /// Uses manually-assigned keys to avoid provider-specific identity retrieval SQL.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task InsertAsync_DateOnly_AllProviders_RoundTripsCorrectly(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE PistParityItem (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, Token TEXT NOT NULL, Color INTEGER NOT NULL)";
        setup.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, MakeParityProvider(kind));

        var expected = new DateOnly(2024, 7, 15);
        await ctx.InsertAsync(new PistParityItem { Id = 1, BirthDate = expected, Token = Guid.NewGuid(), Color = PistHue.Green });

        var rows = await ctx.Query<PistParityItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected, rows[0].BirthDate);
    }

    /// <summary>
    /// Guid InsertAsync must round-trip correctly across all provider dialects.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task InsertAsync_Guid_AllProviders_RoundTripsCorrectly(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE PistParityItem (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, Token TEXT NOT NULL, Color INTEGER NOT NULL)";
        setup.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, MakeParityProvider(kind));

        var expected = Guid.NewGuid();
        await ctx.InsertAsync(new PistParityItem { Id = 1, BirthDate = new DateOnly(2024, 1, 1), Token = expected, Color = PistHue.Red });

        var rows = await ctx.Query<PistParityItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(expected, rows[0].Token);
    }

    /// <summary>
    /// Enum InsertAsync must round-trip correctly across all provider dialects.
    /// </summary>
    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task InsertAsync_Enum_AllProviders_RoundTripsCorrectly(string kind)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE PistParityItem (Id INTEGER PRIMARY KEY, BirthDate TEXT NOT NULL, Token TEXT NOT NULL, Color INTEGER NOT NULL)";
        setup.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, MakeParityProvider(kind));

        await ctx.InsertAsync(new PistParityItem { Id = 1, BirthDate = new DateOnly(2024, 1, 1), Token = Guid.NewGuid(), Color = PistHue.Blue });

        var rows = await ctx.Query<PistParityItem>().ToListAsync();
        Assert.Single(rows);
        Assert.Equal(PistHue.Blue, rows[0].Color);
    }
}
