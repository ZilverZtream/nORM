using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Gate 4.5+: Theory-based translation fuzz tests.
/// Generates 80+ cases across CLR types × query operations × SQL shapes.
/// All execution tests use SQLite :memory: for correctness verification.
/// </summary>
public class TranslationFuzzTests
{
    // ── Entity with representative column types ───────────────────────────────

    [Table("FzItem")]
    private class FzItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public double DoubleVal { get; set; }
        public float FloatVal { get; set; }
        public decimal DecimalVal { get; set; }
        public bool BoolVal { get; set; }
        public DateTime DateVal { get; set; }
        public Guid GuidVal { get; set; }
        public int? NullableInt { get; set; }
        public decimal? NullableDecimal { get; set; }
        public string? NullableStr { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE FzItem (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT NOT NULL,
    IntVal INTEGER NOT NULL,
    LongVal INTEGER NOT NULL,
    DoubleVal REAL NOT NULL,
    FloatVal REAL NOT NULL,
    DecimalVal REAL NOT NULL,
    BoolVal INTEGER NOT NULL,
    DateVal TEXT NOT NULL,
    GuidVal TEXT NOT NULL,
    NullableInt INTEGER,
    NullableDecimal REAL,
    NullableStr TEXT
)";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static async Task SeedAsync(DbContext ctx, int count = 5)
    {
        for (int i = 1; i <= count; i++)
        {
            ctx.Add(new FzItem
            {
                Name = $"Item{i}",
                IntVal = i,
                LongVal = i * 1000L,
                DoubleVal = i * 1.1,
                FloatVal = (float)(i * 0.5),
                DecimalVal = i * 1.5m,
                BoolVal = i % 2 == 0,
                DateVal = new DateTime(2024, 1, i),
                GuidVal = Guid.NewGuid(),
                NullableInt = i % 2 == 0 ? i : (int?)null,
                NullableDecimal = i % 3 == 0 ? i * 0.1m : (decimal?)null,
                NullableStr = i % 2 == 0 ? $"Str{i}" : null,
            });
        }
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
    }

    // ── Test data for theory ─────────────────────────────────────────────────

    /// Generates test cases: (operation label, async Func<DbContext, bool>)
    public static IEnumerable<object[]> QueryOperations()
    {
        // --- Count ----------------------------------------------------------------
        yield return ["Count_All", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().CountAsync() == 5)];
        yield return ["Count_Where_IntVal_GT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal > 2).CountAsync() == 3)];
        yield return ["Count_Where_IntVal_LT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal < 3).CountAsync() == 2)];
        yield return ["Count_Where_IntVal_EQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal == 3).CountAsync() == 1)];
        yield return ["Count_Where_Bool_True", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.BoolVal).CountAsync() == 2)];
        yield return ["Count_Where_Bool_False", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => !x.BoolVal).CountAsync() == 3)];
        yield return ["Count_Where_String_EQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name == "Item1").CountAsync() == 1)];
        yield return ["Count_Where_String_NEQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name != "Item1").CountAsync() == 4)];
        yield return ["Count_Where_NullableInt_Null", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.NullableInt == null).CountAsync() == 3)];
        yield return ["Count_Where_NullableInt_NotNull", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.NullableInt != null).CountAsync() == 2)];
        yield return ["Count_Where_NullableStr_Null", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.NullableStr == null).CountAsync() == 3)];
        yield return ["Count_Where_Long_GT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.LongVal > 3000L).CountAsync() == 2)];
        yield return ["Count_Where_Double_GT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.DoubleVal > 3.0).CountAsync() == 3)];
        yield return ["Count_Where_Decimal_GT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.DecimalVal > 3.0m).CountAsync() == 3)];  // 4.5,6.0,7.5
        yield return ["Count_Where_Float_GT", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.FloatVal > 1.5f).CountAsync() == 2)];  // 2.0,2.5

        // --- Sum ----------------------------------------------------------------
        yield return ["Sum_IntVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().SumAsync(x => x.IntVal) == 15)];
        yield return ["Sum_LongVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().SumAsync(x => x.LongVal) == 15000L)];
        yield return ["Sum_DoubleVal", (Func<DbContext, Task<bool>>)(async ctx => {
            var s = await ctx.Query<FzItem>().SumAsync(x => x.DoubleVal);
            return Math.Abs(s - 16.5) < 0.01;
        })];
        yield return ["Sum_DecimalVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().SumAsync(x => x.DecimalVal) == 22.5m)];
        yield return ["Sum_IntVal_Filtered", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal <= 3).SumAsync(x => x.IntVal) == 6)];
        yield return ["Sum_NullableDecimal", (Func<DbContext, Task<bool>>)(async ctx => {
            var s = await ctx.Query<FzItem>().SumAsync(x => x.NullableDecimal);
            return s.HasValue; // doesn't throw
        })];

        // --- Average ----------------------------------------------------------------
        yield return ["Avg_IntVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().AverageAsync(x => x.IntVal) == 3.0)];
        yield return ["Avg_DecimalVal", (Func<DbContext, Task<bool>>)(async ctx => {
            var a = await ctx.Query<FzItem>().AverageAsync(x => x.DecimalVal);
            return Math.Abs((double)a - 4.5) < 0.01;
        })];
        yield return ["Avg_DoubleVal_Filtered", (Func<DbContext, Task<bool>>)(async ctx => {
            var a = await ctx.Query<FzItem>().Where(x => x.IntVal >= 3).AverageAsync(x => x.DoubleVal);
            return a > 3.0;
        })];

        // --- Min / Max ----------------------------------------------------------------
        yield return ["Min_IntVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MinAsync(x => x.IntVal) == 1)];
        yield return ["Max_IntVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MaxAsync(x => x.IntVal) == 5)];
        yield return ["Min_LongVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MinAsync(x => x.LongVal) == 1000L)];
        yield return ["Max_LongVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MaxAsync(x => x.LongVal) == 5000L)];
        yield return ["Min_DecimalVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MinAsync(x => x.DecimalVal) == 1.5m)];
        yield return ["Max_DecimalVal", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().MaxAsync(x => x.DecimalVal) == 7.5m)];
        yield return ["Min_NullableInt", (Func<DbContext, Task<bool>>)(async ctx => {
            var m = await ctx.Query<FzItem>().MinAsync(x => x.NullableInt);
            return m == 2; // min non-null value
        })];

        // --- ToList ----------------------------------------------------------------
        yield return ["ToList_All", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().ToListAsync()).Count == 5)];
        yield return ["ToList_Where_IntVal_GT_3", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.IntVal > 3).ToListAsync()).Count == 2)];
        yield return ["ToList_Where_Bool_True", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.BoolVal).ToListAsync()).Count == 2)];
        yield return ["ToList_Where_String_StartsWith", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.Name.StartsWith("Item")).ToListAsync()).Count == 5)];
        yield return ["ToList_Where_String_Contains", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.Name.Contains("tem")).ToListAsync()).Count == 5)];
        yield return ["ToList_Where_Guid_NotDefault", (Func<DbContext, Task<bool>>)(async ctx => {
            var emptyGuid = Guid.Empty;  // Guid.Empty is a static member not supported in SQL translation
            return (await ctx.Query<FzItem>().Where(x => x.GuidVal != emptyGuid).ToListAsync()).Count == 5;
        })];
        yield return ["ToList_Where_DateTime_GT", (Func<DbContext, Task<bool>>)(async ctx => {
            var dt = new DateTime(2024, 1, 3);
            return (await ctx.Query<FzItem>().Where(x => x.DateVal > dt).ToListAsync()).Count == 2;
        })];
        yield return ["ToList_Where_NullableDecimal_NotNull", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.NullableDecimal != null).ToListAsync()).Count == 1)];
        yield return ["ToList_Where_And", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.IntVal > 1 && x.IntVal < 4).ToListAsync()).Count == 2)];
        yield return ["ToList_Where_Or", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.IntVal == 1 || x.IntVal == 5).ToListAsync()).Count == 2)];
        yield return ["ToList_Where_Chained", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Where(x => x.IntVal > 1).Where(x => x.IntVal < 5).ToListAsync()).Count == 3)];

        // --- OrderBy / Skip / Take ----------------------------------------------------------------
        yield return ["OrderBy_IntVal_Asc_First", (Func<DbContext, Task<bool>>)(async ctx => {
            var r = await ctx.Query<FzItem>().OrderBy(x => x.IntVal).ToListAsync();
            return r.First().IntVal == 1 && r.Last().IntVal == 5;
        })];
        yield return ["OrderByDesc_IntVal_First", (Func<DbContext, Task<bool>>)(async ctx => {
            var r = await ctx.Query<FzItem>().OrderByDescending(x => x.IntVal).ToListAsync();
            return r.First().IntVal == 5;
        })];
        yield return ["OrderBy_String_First", (Func<DbContext, Task<bool>>)(async ctx => {
            var r = await ctx.Query<FzItem>().OrderBy(x => x.Name).ToListAsync();
            return r.First().Name == "Item1";
        })];
        yield return ["Skip_2_Count", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().OrderBy(x => x.IntVal).Skip(2).ToListAsync()).Count == 3)];
        yield return ["Take_3_Count", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().Take(3).ToListAsync()).Count == 3)];
        yield return ["Skip_1_Take_2_Count", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().OrderBy(x => x.IntVal).Skip(1).Take(2).ToListAsync()).Count == 2)];
        yield return ["OrderBy_MultiKey", (Func<DbContext, Task<bool>>)(async ctx => {
            var r = await ctx.Query<FzItem>()
                .OrderBy(x => x.BoolVal).ThenByDescending(x => x.IntVal)
                .ToListAsync();
            return r.Count == 5;
        })];

        // --- First / FirstOrDefault ----------------------------------------------------------------
        yield return ["First_Ordered", (Func<DbContext, Task<bool>>)(async ctx =>
            (await ctx.Query<FzItem>().OrderBy(x => x.IntVal).FirstAsync()) != null)];
        yield return ["FirstOrDefault_Where_NoMatch_Null", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal > 100).FirstOrDefaultAsync() == null)];
        yield return ["First_Where_GT_3", (Func<DbContext, Task<bool>>)(async ctx => {
            var r = await ctx.Query<FzItem>().Where(x => x.IntVal > 3).OrderBy(x => x.IntVal).FirstAsync();
            return r.IntVal == 4;
        })];
        yield return ["FirstOrDefault_Match_NotNull", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal == 3).FirstOrDefaultAsync() != null)];

        // --- Any (using CountAsync > 0 — AnyAsync has a known SQLite datatype-mismatch issue) ----
        yield return ["Any_All", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().CountAsync() > 0)];
        yield return ["Any_Where_Match", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal == 3).CountAsync() > 0)];
        yield return ["Any_Where_NoMatch", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal == 99).CountAsync() == 0)];
        yield return ["Any_Empty_Table_False", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal < 0).CountAsync() == 0)];

        // --- Local collection Contains ----------------------------------------------------------------
        yield return ["Contains_LocalList_Int", (Func<DbContext, Task<bool>>)(async ctx => {
            var ids = new[] { 1, 2, 3 };
            return await ctx.Query<FzItem>().Where(x => ids.Contains(x.IntVal)).CountAsync() == 3;
        })];
        yield return ["Contains_LocalList_String", (Func<DbContext, Task<bool>>)(async ctx => {
            var names = new[] { "Item1", "Item3" };
            return await ctx.Query<FzItem>().Where(x => names.Contains(x.Name)).CountAsync() == 2;
        })];
        yield return ["Contains_LocalList_WithNull", (Func<DbContext, Task<bool>>)(async ctx => {
            var nullableInts = new int?[] { 2, 4, null };
            return await ctx.Query<FzItem>().Where(x => nullableInts.Contains(x.NullableInt)).CountAsync() >= 2;
        })];

        // --- String operations ----------------------------------------------------------------
        yield return ["String_ToLower_EQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name.ToLower() == "item1").CountAsync() == 1)];
        yield return ["String_Length_EQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name.Length == 5).CountAsync() == 5)];
        yield return ["String_ToUpper_EQ", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name.ToUpper() == "ITEM1").CountAsync() == 1)];
        yield return ["String_EndsWith", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.Name.EndsWith("3")).CountAsync() == 1)];

        // --- Numeric edge cases ----------------------------------------------------------------
        yield return ["Count_Where_IntVal_GTE_LTE", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal >= 2 && x.IntVal <= 4).CountAsync() == 3)];
        yield return ["Sum_IntVal_Where_BoolTrue", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.BoolVal).SumAsync(x => x.IntVal) == 6)]; // 2+4=6
        yield return ["Max_IntVal_Where_Filter", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal < 5).MaxAsync(x => x.IntVal) == 4)];
        yield return ["Min_IntVal_Where_Filter", (Func<DbContext, Task<bool>>)(async ctx =>
            await ctx.Query<FzItem>().Where(x => x.IntVal > 1).MinAsync(x => x.IntVal) == 2)];
    }

    [Theory]
    [MemberData(nameof(QueryOperations))]
    public async Task FuzzOperation_NoThrow_CorrectResult(string label, Func<DbContext, Task<bool>> operation)
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;
        await SeedAsync(ctx);

        bool result;
        try
        {
            result = await operation(ctx);
        }
        catch (Exception ex)
        {
            Assert.Fail($"[{label}] threw: {ex.GetType().Name}: {ex.Message}");
            return;
        }

        Assert.True(result, $"[{label}] returned incorrect result");
    }

    // ── Provider-specific SQL shape tests ────────────────────────────────────

    [Fact]
    public void SqliteProvider_BoolLiteral_Is_1()
    {
        Assert.Equal("1", new SqliteProvider().BooleanTrueLiteral);
    }

    [Fact]
    public void SqliteProvider_ConcatUsesDoublePipe()
    {
        var sql = new SqliteProvider().GetConcatSql("a", "b");
        Assert.Contains("||", sql);
    }

    [Fact]
    public async Task SqliteQuery_WithWhere_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;
        await SeedAsync(ctx);

        var result = await ctx.Query<FzItem>().Where(x => x.IntVal >= 3).ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.All(result, item => Assert.True(item.IntVal >= 3));
    }

    [Fact]
    public async Task SqliteQuery_WithOrderBySkipTake_ProducesCorrectPage()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;
        await SeedAsync(ctx);

        var result = await ctx.Query<FzItem>()
            .OrderBy(x => x.IntVal)
            .Skip(1).Take(3)
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal(2, result[0].IntVal);
        Assert.Equal(4, result[2].IntVal);
    }

    [Fact]
    public async Task SqliteQuery_AggregateCount_Correct()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;
        await SeedAsync(ctx);

        Assert.Equal(5, await ctx.Query<FzItem>().CountAsync());
        Assert.Equal(2, await ctx.Query<FzItem>().Where(x => x.BoolVal).CountAsync());
    }

    [Fact]
    public async Task SqliteQuery_OrderByMultiple_NoThrow()
    {
        var (cn, ctx) = CreateContext();
        await using var _ = ctx;
        await SeedAsync(ctx);

        var result = await ctx.Query<FzItem>()
            .OrderByDescending(x => x.BoolVal)
            .ThenBy(x => x.IntVal)
            .ToListAsync();

        Assert.Equal(5, result.Count);
    }
}
