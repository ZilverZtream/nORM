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
/// Blocker 9 — Aggregate/null/boolean/enum/type parity.
/// Covers: bool columns in WHERE, enum ordering/filtering, DateOnly/TimeOnly
/// round-trips, decimal precision, Guid round-trips, nullable vs non-null
/// columns, and IS NULL / IS NOT NULL predicate forms.
/// All tests use SQLite in-memory DB.
/// </summary>
public class TypeConversionParityTests
{
    // ══════════════════════════════════════════════════════════════════════
    // Shared entity covering all column types under test
    // ══════════════════════════════════════════════════════════════════════

    [Table("TypeParityRow")]
    private class TypeParityRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        // bool
        public bool IsEnabled { get; set; }
        public bool? NullableBool { get; set; }

        // enum (stored as int)
        public int Status { get; set; }

        // dates / times
        public DateOnly BirthDate { get; set; }
        public TimeOnly WakeTime { get; set; }
        public DateOnly? NullableDate { get; set; }
        public TimeOnly? NullableTime { get; set; }

        // decimal precision
        public decimal Price { get; set; }
        public decimal? NullablePrice { get; set; }

        // Guid
        public Guid UniqueId { get; set; }
        public Guid? NullableGuid { get; set; }

        // nullable int
        public int? NullableScore { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE TypeParityRow (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    IsEnabled     INTEGER NOT NULL DEFAULT 0,
                    NullableBool  INTEGER,
                    Status        INTEGER NOT NULL DEFAULT 0,
                    BirthDate     TEXT    NOT NULL DEFAULT '2000-01-01',
                    WakeTime      TEXT    NOT NULL DEFAULT '00:00:00',
                    NullableDate  TEXT,
                    NullableTime  TEXT,
                    Price         REAL    NOT NULL DEFAULT 0,
                    NullablePrice REAL,
                    UniqueId      TEXT    NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
                    NullableGuid  TEXT,
                    NullableScore INTEGER
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void InsertRaw(
        SqliteConnection cn,
        bool isEnabled,
        bool? nullableBool,
        int status,
        DateOnly birthDate,
        TimeOnly wakeTime,
        DateOnly? nullableDate,
        TimeOnly? nullableTime,
        decimal price,
        decimal? nullablePrice,
        Guid uniqueId,
        Guid? nullableGuid,
        int? nullableScore)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO TypeParityRow
                (IsEnabled, NullableBool, Status, BirthDate, WakeTime,
                 NullableDate, NullableTime, Price, NullablePrice,
                 UniqueId, NullableGuid, NullableScore)
            VALUES
                (@ie, @nb, @st, @bd, @wt,
                 @nd, @nt, @pr, @np,
                 @ui, @ng, @ns)";
        cmd.Parameters.AddWithValue("@ie", isEnabled ? 1 : 0);
        cmd.Parameters.AddWithValue("@nb", nullableBool.HasValue ? (object)(nullableBool.Value ? 1 : 0) : DBNull.Value);
        cmd.Parameters.AddWithValue("@st", status);
        cmd.Parameters.AddWithValue("@bd", birthDate.ToString("yyyy-MM-dd"));
        cmd.Parameters.AddWithValue("@wt", wakeTime.ToString("HH:mm:ss.fffffff"));
        cmd.Parameters.AddWithValue("@nd", nullableDate.HasValue ? (object)nullableDate.Value.ToString("yyyy-MM-dd") : DBNull.Value);
        cmd.Parameters.AddWithValue("@nt", nullableTime.HasValue ? (object)nullableTime.Value.ToString("HH:mm:ss.fffffff") : DBNull.Value);
        cmd.Parameters.AddWithValue("@pr", (double)price);
        cmd.Parameters.AddWithValue("@np", nullablePrice.HasValue ? (object)(double)nullablePrice.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@ui", uniqueId.ToString());
        cmd.Parameters.AddWithValue("@ng", nullableGuid.HasValue ? (object)nullableGuid.Value.ToString() : DBNull.Value);
        cmd.Parameters.AddWithValue("@ns", (object?)nullableScore ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static (SqliteConnection Cn, DbContext Ctx) BuildWithTwoRows()
    {
        var (cn, ctx) = CreateContext();
        InsertRaw(cn,
            isEnabled: true, nullableBool: true, status: 1,
            birthDate: new DateOnly(1990, 6, 15),
            wakeTime: new TimeOnly(7, 30, 0),
            nullableDate: new DateOnly(2020, 1, 1),
            nullableTime: new TimeOnly(8, 0, 0),
            price: 19.99m, nullablePrice: 9.99m,
            uniqueId: new Guid("11111111-0000-0000-0000-000000000000"),
            nullableGuid: new Guid("aaaaaaaa-0000-0000-0000-000000000000"),
            nullableScore: 100);

        InsertRaw(cn,
            isEnabled: false, nullableBool: null, status: 2,
            birthDate: new DateOnly(1985, 12, 31),
            wakeTime: new TimeOnly(6, 0, 0),
            nullableDate: null, nullableTime: null,
            price: 5.50m, nullablePrice: null,
            uniqueId: new Guid("22222222-0000-0000-0000-000000000000"),
            nullableGuid: null,
            nullableScore: null);

        return (cn, ctx);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Bool columns in WHERE
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Bool_WhereTrueFilter_ReturnsOnlyEnabledRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(results);
        Assert.True(results[0].IsEnabled);
    }

    [Fact]
    public async Task Bool_WhereFalseFilter_ReturnsOnlyDisabledRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == false)
            .ToListAsync();

        Assert.Single(results);
        Assert.False(results[0].IsEnabled);
    }

    [Fact]
    public async Task Bool_BareMemberPredicate_ReturnsEnabledRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled)
            .ToListAsync();

        Assert.Single(results);
        Assert.True(results[0].IsEnabled);
    }

    [Fact]
    public async Task Bool_NegatedMemberPredicate_ReturnsDisabledRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => !r.IsEnabled)
            .ToListAsync();

        Assert.Single(results);
        Assert.False(results[0].IsEnabled);
    }

    [Fact]
    public async Task NullableBool_IsNullFilter_ReturnsNullBoolRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableBool == null)
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].NullableBool);
    }

    [Fact]
    public async Task NullableBool_IsTrueFilter_ReturnsNonNullTrueRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableBool == true)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(true, results[0].NullableBool);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Enum columns: filtering and ordering
    // ══════════════════════════════════════════════════════════════════════

    private enum RowStatus { Draft = 0, Active = 1, Archived = 2 }

    [Fact]
    public async Task Enum_WhereFilterByIntValue_ReturnsMatchingRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var activeVal = (int)RowStatus.Active;
        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.Status == activeVal)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal((int)RowStatus.Active, results[0].Status);
    }

    [Fact]
    public async Task Enum_OrderByEnumColumn_SortsNumerically()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: (int)RowStatus.Archived,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 1m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null, nullableScore: null);

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: (int)RowStatus.Draft,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 2m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null, nullableScore: null);

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: (int)RowStatus.Active,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 3m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null, nullableScore: null);

        var rows = await ctx.Query<TypeParityRow>()
            .OrderBy(r => r.Status)
            .ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal((int)RowStatus.Draft,    rows[0].Status);
        Assert.Equal((int)RowStatus.Active,   rows[1].Status);
        Assert.Equal((int)RowStatus.Archived, rows[2].Status);
    }

    [Fact]
    public async Task Enum_GreaterThanFilter_ReturnsCorrectSubset()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var activeVal = (int)RowStatus.Active;
        // Status > Active means only Archived (value 2)
        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.Status > activeVal)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal((int)RowStatus.Archived, results[0].Status);
    }

    // ══════════════════════════════════════════════════════════════════════
    // DateOnly / TimeOnly round-trips via InsertAsync
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DateOnly_RoundTrip_InsertAndQuery_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Equal(new DateOnly(1990, 6, 15), rows[0].BirthDate);
    }

    [Fact]
    public async Task TimeOnly_RoundTrip_InsertAndQuery_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Equal(new TimeOnly(7, 30, 0), rows[0].WakeTime);
    }

    [Fact]
    public async Task NullableDateOnly_WhenNull_ReturnsNullInCLR()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == false)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Null(rows[0].NullableDate);
    }

    [Fact]
    public async Task NullableDateOnly_WhenNonNull_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.NotNull(rows[0].NullableDate);
        Assert.Equal(new DateOnly(2020, 1, 1), rows[0].NullableDate!.Value);
    }

    [Fact]
    public async Task NullableTimeOnly_WhenNull_ReturnsNullInCLR()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == false)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Null(rows[0].NullableTime);
    }

    [Fact]
    public async Task NullableTimeOnly_WhenNonNull_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.NotNull(rows[0].NullableTime);
        Assert.Equal(new TimeOnly(8, 0, 0), rows[0].NullableTime!.Value);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Decimal precision round-trips
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Decimal_RoundTrip_PreservesTwoDecimalPlaces()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .OrderBy(r => r.Price)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        // 5.50 comes first
        Assert.Equal(5.50m,  rows[0].Price);
        Assert.Equal(19.99m, rows[1].Price);
    }

    [Fact]
    public async Task Decimal_FilterByExactValue_ReturnsMatchingRow()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var price = 19.99m;
        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.Price == price)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(19.99m, results[0].Price);
    }

    [Fact]
    public async Task NullableDecimal_WhenNull_ReturnsNullInCLR()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == false)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Null(rows[0].NullablePrice);
    }

    [Fact]
    public async Task NullableDecimal_WhenNonNull_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.NotNull(rows[0].NullablePrice);
        Assert.Equal(9.99m, rows[0].NullablePrice!.Value);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Guid round-trips
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Guid_RoundTrip_PreservesIdentity()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(new Guid("11111111-0000-0000-0000-000000000000"), rows[0].UniqueId);
        Assert.Equal(new Guid("22222222-0000-0000-0000-000000000000"), rows[1].UniqueId);
    }

    [Fact]
    public async Task Guid_FilterByValue_ReturnsMatchingRow()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var target = new Guid("22222222-0000-0000-0000-000000000000");
        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.UniqueId == target)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(target, results[0].UniqueId);
    }

    [Fact]
    public async Task NullableGuid_WhenNull_ReturnsNullInCLR()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == false)
            .ToListAsync();

        Assert.Single(rows);
        Assert.Null(rows[0].NullableGuid);
    }

    [Fact]
    public async Task NullableGuid_WhenNonNull_PreservesValue()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var rows = await ctx.Query<TypeParityRow>()
            .Where(r => r.IsEnabled == true)
            .ToListAsync();

        Assert.Single(rows);
        Assert.NotNull(rows[0].NullableGuid);
        Assert.Equal(new Guid("aaaaaaaa-0000-0000-0000-000000000000"), rows[0].NullableGuid!.Value);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Nullable columns: IS NULL / IS NOT NULL predicates
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IsNull_NullableInt_FiltersToNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore == null)
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].NullableScore);
    }

    [Fact]
    public async Task IsNotNull_NullableInt_FiltersToNonNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore != null)
            .ToListAsync();

        Assert.Single(results);
        Assert.NotNull(results[0].NullableScore);
        Assert.Equal(100, results[0].NullableScore!.Value);
    }

    [Fact]
    public async Task IsNull_NullableGuid_FiltersToNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableGuid == null)
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].NullableGuid);
    }

    [Fact]
    public async Task IsNotNull_NullableGuid_FiltersToNonNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableGuid != null)
            .ToListAsync();

        Assert.Single(results);
        Assert.NotNull(results[0].NullableGuid);
    }

    [Fact]
    public async Task IsNull_NullableDecimal_FiltersToNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullablePrice == null)
            .ToListAsync();

        Assert.Single(results);
        Assert.Null(results[0].NullablePrice);
    }

    [Fact]
    public async Task IsNotNull_NullablePrice_FiltersToNonNullRows()
    {
        var (cn, ctx) = BuildWithTwoRows();
        using var _cn = cn; using var _ctx = ctx;

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullablePrice != null)
            .ToListAsync();

        Assert.Single(results);
        Assert.NotNull(results[0].NullablePrice);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Multiple-row edge: all nulls vs no nulls in a given nullable column
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IsNull_AllRowsNull_ReturnsAllRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Insert three rows, all with NullableScore = null
        for (int i = 0; i < 3; i++)
        {
            InsertRaw(cn,
                isEnabled: true, nullableBool: null, status: 0,
                birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
                nullableDate: null, nullableTime: null,
                price: 1m, nullablePrice: null,
                uniqueId: Guid.NewGuid(), nullableGuid: null,
                nullableScore: null);
        }

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore == null)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.Null(r.NullableScore));
    }

    [Fact]
    public async Task IsNotNull_NoNullRows_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Insert rows where NullableScore is always set
        for (int i = 1; i <= 3; i++)
        {
            InsertRaw(cn,
                isEnabled: true, nullableBool: null, status: 0,
                birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
                nullableDate: null, nullableTime: null,
                price: 1m, nullablePrice: null,
                uniqueId: Guid.NewGuid(), nullableGuid: null,
                nullableScore: i * 10);
        }

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore == null)
            .ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task IsNull_NoNullRows_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        for (int i = 1; i <= 3; i++)
        {
            InsertRaw(cn,
                isEnabled: true, nullableBool: null, status: 0,
                birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
                nullableDate: null, nullableTime: null,
                price: 1m, nullablePrice: null,
                uniqueId: Guid.NewGuid(), nullableGuid: null,
                nullableScore: i);
        }

        var nullResults = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore == null)
            .ToListAsync();

        Assert.Empty(nullResults);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Nullable int: C# 3VL parity (null != value includes null rows)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task NullableInt_NotEqualValue_IncludesNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: 0,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 1m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null,
            nullableScore: null);    // null != 42 → true in C# → INCLUDE

        InsertRaw(cn,
            isEnabled: false, nullableBool: null, status: 0,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 2m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null,
            nullableScore: 99);     // 99 != 42 → true → INCLUDE

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: 0,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 3m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null,
            nullableScore: 42);     // 42 != 42 → false → EXCLUDE

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore != 42)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NullableScore is null);
        Assert.Contains(results, r => r.NullableScore == 99);
    }

    [Fact]
    public async Task NullableInt_EqualValue_ExcludesNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        InsertRaw(cn,
            isEnabled: true, nullableBool: null, status: 0,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 1m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null,
            nullableScore: null);   // null == 42 → false → EXCLUDE

        InsertRaw(cn,
            isEnabled: false, nullableBool: null, status: 0,
            birthDate: new DateOnly(2000, 1, 1), wakeTime: new TimeOnly(8, 0),
            nullableDate: null, nullableTime: null,
            price: 2m, nullablePrice: null,
            uniqueId: Guid.NewGuid(), nullableGuid: null,
            nullableScore: 42);    // 42 == 42 → true → INCLUDE

        var results = await ctx.Query<TypeParityRow>()
            .Where(r => r.NullableScore == 42)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(42, results[0].NullableScore);
    }
}
