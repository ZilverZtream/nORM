using System;
using System.Collections.Generic;
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

namespace nORM.Tests;

/// <summary>
/// Section 8 gate 4.0→4.5: Source-generated (compiled) vs runtime-query equivalence tests.
/// Verifies that Norm.CompileQuery (precompiled path) and runtime LINQ produce identical
/// results for the same input data, using SQLite in-memory databases.
/// </summary>
public class SourceGenRuntimeParityTests
{
    // ── Entity ────────────────────────────────────────────────────────────────

    public class ParityItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public string? Category { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateDb(int rowCount = 10)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ParityItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Category TEXT);";
        cmd.ExecuteNonQuery();

        for (int i = 1; i <= rowCount; i++)
        {
            using var ins = cn.CreateCommand();
            var cat = i % 3 == 0 ? "NULL" : $"'cat{i % 3}'";
            ins.CommandText = $"INSERT INTO ParityItem VALUES ({i}, 'item{i}', {i * 10}, {cat})";
            ins.ExecuteNonQuery();
        }

        return cn;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 1. Basic select all
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_SelectAll_SameRowCountAndValues()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled: select all by passing a dummy param
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>());

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>().ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
            Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 2. Select with Where
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_WhereFilter_SameFilteredResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value > minVal));

        var threshold = 50;
        var compiledResult = await compiled(ctx, threshold);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Value > threshold)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Filter should match at least one row");
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
        }
    }

    [Fact]
    public async Task Parity_WhereEquality_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        for (int id = 1; id <= 10; id++)
        {
            var compiledResult = await compiled(ctx, id);
            var runtimeResult = await ctx.Query<ParityItem>()
                .Where(x => x.Id == id)
                .ToListAsync();

            Assert.Equal(runtimeResult.Count, compiledResult.Count);
            Assert.Single(compiledResult);
            Assert.Equal(runtimeResult[0].Id, compiledResult[0].Id);
            Assert.Equal(runtimeResult[0].Name, compiledResult[0].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 3. OrderBy + Skip + Take (paging parity)
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_OrderBySkipTake_SamePagingResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled: page 2 (skip 3, take 3), ordered by Value descending
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderByDescending(x => x.Value).Skip(3).Take(3));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderByDescending(x => x.Value)
            .Skip(3)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, compiledResult.Count);
        Assert.Equal(3, runtimeResult.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
        }
    }

    [Fact]
    public async Task Parity_OrderByAsc_TakeN_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id).Take(5));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderBy(x => x.Id)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, compiledResult.Count);
        Assert.Equal(5, runtimeResult.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 4. Count aggregate
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_Count_CompiledListCountMatchesRuntimeCountAsync()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled query returns List<T>; count its elements
        var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value > minVal));

        var threshold = 30;
        var compiledResult = await compiled(ctx, threshold);
        var runtimeCount = await ctx.Query<ParityItem>()
            .Where(x => x.Value > threshold)
            .CountAsync();

        Assert.Equal(runtimeCount, compiledResult.Count);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 5. FirstOrDefault
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_FirstOrDefault_ExistingRow_SameEntity()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledResult = await compiled(ctx, 1);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 1)
            .FirstOrDefaultAsync();

        Assert.Single(compiledResult);
        Assert.NotNull(runtimeResult);
        Assert.Equal(runtimeResult!.Id, compiledResult[0].Id);
        Assert.Equal(runtimeResult.Name, compiledResult[0].Name);
        Assert.Equal(runtimeResult.Value, compiledResult[0].Value);
    }

    [Fact]
    public async Task Parity_FirstOrDefault_NonExistentRow_BothEmpty()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledResult = await compiled(ctx, 9999);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 9999)
            .FirstOrDefaultAsync();

        Assert.Empty(compiledResult);
        Assert.Null(runtimeResult);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 6. Select projection
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_SelectProjection_SameProjectedValues()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled query projects subset of columns via anonymous type
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id).Select(x => new { x.Id, x.Name }));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderBy(x => x.Id)
            .Select(x => new { x.Id, x.Name })
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 7. Where with null comparison
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_WhereNullCategory_SameIsNullBehavior()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Rows where Category IS NULL (every 3rd row: id 3, 6, 9)
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().Where(x => x.Category == null));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Category == null)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Should find null-category rows");
        foreach (var item in compiledResult)
            Assert.Null(item.Category);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    [Fact]
    public async Task Parity_WhereNotNullCategory_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().Where(x => x.Category != null));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Category != null)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Should find non-null-category rows");
        foreach (var item in compiledResult)
            Assert.NotNull(item.Category);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 8. Multiple queries same context
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_MultipleQueriesSameContext_BothCorrect()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        // Run compiled first
        var compiledResult1 = await compiled(ctx, 1);
        var compiledResult5 = await compiled(ctx, 5);

        // Then runtime on the same context
        var runtimeResult1 = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 1)
            .ToListAsync();
        var runtimeResult5 = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 5)
            .ToListAsync();

        // Compiled and runtime must agree for both parameter values
        Assert.Single(compiledResult1);
        Assert.Single(runtimeResult1);
        Assert.Equal(runtimeResult1[0].Id, compiledResult1[0].Id);
        Assert.Equal(runtimeResult1[0].Name, compiledResult1[0].Name);

        Assert.Single(compiledResult5);
        Assert.Single(runtimeResult5);
        Assert.Equal(runtimeResult5[0].Id, compiledResult5[0].Id);
        Assert.Equal(runtimeResult5[0].Name, compiledResult5[0].Name);
    }

    [Fact]
    public async Task Parity_InterleavedCompiledAndRuntime_AllCorrect()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiledById = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledByVal = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value >= minVal));

        // Interleave: compiled → runtime → compiled → runtime
        var cr1 = await compiledById(ctx, 3);
        var rr1 = await ctx.Query<ParityItem>().Where(x => x.Id == 3).ToListAsync();
        Assert.Single(cr1);
        Assert.Single(rr1);
        Assert.Equal(rr1[0].Id, cr1[0].Id);

        var cr2 = await compiledByVal(ctx, 90);
        var rr2 = await ctx.Query<ParityItem>().Where(x => x.Value >= 90).ToListAsync();
        Assert.Equal(rr2.Count, cr2.Count);
        for (int i = 0; i < rr2.Count; i++)
            Assert.Equal(rr2[i].Id, cr2[i].Id);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Additional parity tests
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_EmptyTable_BothReturnEmpty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE ParityItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Category TEXT)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>());

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>().ToListAsync();

        Assert.Empty(compiledResult);
        Assert.Empty(runtimeResult);
    }

    [Fact]
    public async Task Parity_RepeatedCompiledCalls_StableResults()
    {
        using var cn = CreateDb(5);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id));

        var runtimeResult = await ctx.Query<ParityItem>().OrderBy(x => x.Id).ToListAsync();

        // Run compiled 20 times — each invocation must yield the same stable result
        for (int iter = 0; iter < 20; iter++)
        {
            var compiledResult = await compiled(ctx, iter);
            Assert.Equal(runtimeResult.Count, compiledResult.Count);
            for (int i = 0; i < runtimeResult.Count; i++)
            {
                Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
                Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
                Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
            }
        }
    }
}

// ── Entities for sourcegen/runtime equivalence tests (explicit PK, no identity) ─

[GenerateMaterializer]
[Table("G50Eq_Basic")]
internal class SgEqBasic
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Score { get; set; }
    public string? Note { get; set; }
}

[GenerateMaterializer]
[Table("G50Eq_Enum")]
internal class SgEqEnum
{
    [Key]
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public SgEqStatus Status { get; set; }
}

internal enum SgEqStatus { Draft = 0, Published = 1, Deleted = 2 }

[GenerateMaterializer]
[Table("G50Eq_Date")]
internal class SgEqDate
{
    [Key]
    public int Id { get; set; }
    public DateOnly Created { get; set; }
    public TimeOnly Start { get; set; }
    public string Tag { get; set; } = string.Empty;
}

[GenerateMaterializer]
[Table("G50Eq_Conv")]
internal class SgEqConv
{
    [Key]
    public int Id { get; set; }
    public int Points { get; set; }  // negated in DB via converter
    public string Tag { get; set; } = string.Empty;
}

internal sealed class SgNegatingConv : ValueConverter<int, int>
{
    public override object? ConvertToProvider(int v) => -v;
    public override object? ConvertFromProvider(int v) => -v;
}

[GenerateMaterializer]
[Table("G50Eq_Renamed")]
internal class SgEqRenamed
{
    [Key]
    public int Id { get; set; }

    [Column("FullName")]
    public string Name { get; set; } = string.Empty;

    [Column("PointsVal")]
    public int Points { get; set; }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

internal static class SgEqHelpers
{
    internal static DatabaseProvider MakeProvider(string kind) => kind switch
    {
        "sqlite"   => new SqliteProvider(),
        "mysql"    => new MySqlProvider(new SqliteParameterFactory()),
        "postgres" => new PostgresProvider(new SqliteParameterFactory()),
        _          => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    internal static (SqliteConnection Cn, DbContext Ctx) Open(string kind, string ddl,
        DbContextOptions? opts = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, MakeProvider(kind), opts ?? new DbContextOptions()));
    }

    internal static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}

/// <summary>
/// Verifies compiled vs runtime result equivalence for a basic entity across
/// all lock-step providers (SQLite, MySQL lock-step, PostgreSQL lock-step).
/// </summary>
public class SourceGenBasicEquivalenceTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Basic (Id INTEGER PRIMARY KEY, " +
        "Name TEXT NOT NULL, Score INTEGER NOT NULL, Note TEXT)";

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Basic_CompiledQuery_MatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 5; i++)
            ctx.Add(new SgEqBasic { Id = i, Name = $"item{i}", Score = i * 10,
                                    Note = i % 2 == 0 ? null : $"note{i}" });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<SgEqBasic>().Where(x => x.Score >= minScore).OrderBy(x => x.Id));

        var compResult = await compiled(ctx, 20);
        var rtResult = await ctx.Query<SgEqBasic>()
            .Where(x => x.Score >= 20)
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        for (int i = 0; i < rtResult.Count; i++)
        {
            Assert.Equal(rtResult[i].Id,    compResult[i].Id);
            Assert.Equal(rtResult[i].Name,  compResult[i].Name);
            Assert.Equal(rtResult[i].Score, compResult[i].Score);
            Assert.Equal(rtResult[i].Note,  compResult[i].Note);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Basic_NullableNote_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new SgEqBasic { Id = 1, Name = "with-note", Score = 1, Note = "present" });
        ctx.Add(new SgEqBasic { Id = 2, Name = "no-note",   Score = 2, Note = null });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqBasic>().Where(x => x.Note == null));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqBasic>()
            .Where(x => x.Note == null)
            .ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        Assert.Single(compResult);
        Assert.All(compResult, r => Assert.Null(r.Note));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Basic_Paging_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        for (int i = 1; i <= 10; i++)
            ctx.Add(new SgEqBasic { Id = i, Name = $"item{i:D2}", Score = i });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqBasic>().OrderBy(x => x.Id).Skip(3).Take(4));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqBasic>()
            .OrderBy(x => x.Id)
            .Skip(3).Take(4)
            .ToListAsync();

        Assert.Equal(4, compResult.Count);
        Assert.Equal(4, rtResult.Count);
        for (int i = 0; i < 4; i++)
            Assert.Equal(rtResult[i].Id, compResult[i].Id);
    }
}

/// <summary>
/// Verifies that compiled and runtime paths produce identical results for enum-typed
/// columns across all lock-step providers.
/// </summary>
public class SourceGenEnumEquivalenceTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Enum (Id INTEGER PRIMARY KEY, " +
        "Label TEXT NOT NULL, Status INTEGER NOT NULL)";

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Enum_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new SgEqEnum { Id = 1, Label = "d1", Status = SgEqStatus.Draft     });
        ctx.Add(new SgEqEnum { Id = 2, Label = "p1", Status = SgEqStatus.Published });
        ctx.Add(new SgEqEnum { Id = 3, Label = "p2", Status = SgEqStatus.Published });
        ctx.Add(new SgEqEnum { Id = 4, Label = "dl", Status = SgEqStatus.Deleted   });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, SgEqStatus s) =>
            c.Query<SgEqEnum>().Where(x => x.Status == s).OrderBy(x => x.Id));

        var compResult = await compiled(ctx, SgEqStatus.Published);
        var rtResult = await ctx.Query<SgEqEnum>()
            .Where(x => x.Status == SgEqStatus.Published)
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        Assert.Equal(2, compResult.Count);
        Assert.All(compResult, r => Assert.Equal(SgEqStatus.Published, r.Status));
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Enum_Update_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        var item = new SgEqEnum { Id = 1, Label = "evolving", Status = SgEqStatus.Draft };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Status = SgEqStatus.Published;
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<SgEqEnum>().Where(x => x.Id == id));

        var result = (await compiled(ctx, 1)).Single();
        var rtResult = ctx.Query<SgEqEnum>().Single(x => x.Id == 1);

        Assert.Equal(SgEqStatus.Published, result.Status);
        Assert.Equal(SgEqStatus.Published, rtResult.Status);
    }
}

/// <summary>
/// Verifies that compiled and runtime paths produce identical results for
/// DateOnly/TimeOnly columns across all lock-step providers.
/// </summary>
public class SourceGenDateTimeEquivalenceTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Date (Id INTEGER PRIMARY KEY, " +
        "Created TEXT NOT NULL, Start TEXT NOT NULL, Tag TEXT NOT NULL)";

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task DateOnly_TimeOnly_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new SgEqDate { Id = 1, Created = new DateOnly(2024, 1, 15), Start = new TimeOnly(8, 0),   Tag = "a" });
        ctx.Add(new SgEqDate { Id = 2, Created = new DateOnly(2024, 6, 30), Start = new TimeOnly(14, 30), Tag = "b" });
        ctx.Add(new SgEqDate { Id = 3, Created = new DateOnly(2025, 1, 1),  Start = new TimeOnly(0, 0),   Tag = "c" });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqDate>().OrderBy(x => x.Id));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqDate>().OrderBy(x => x.Id).ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        for (int i = 0; i < rtResult.Count; i++)
        {
            Assert.Equal(rtResult[i].Created, compResult[i].Created);
            Assert.Equal(rtResult[i].Start,   compResult[i].Start);
            Assert.Equal(rtResult[i].Tag,     compResult[i].Tag);
        }
    }
}

/// <summary>
/// Verifies that ValueConverter entities bypass the compiled materializer and use
/// the runtime path, producing identical results to runtime queries.
/// </summary>
public class SourceGenConverterEquivalenceTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Conv (Id INTEGER PRIMARY KEY, " +
        "Points INTEGER NOT NULL, Tag TEXT NOT NULL)";

    private static DbContextOptions ConverterOpts() => new()
    {
        OnModelCreating = mb =>
            mb.Entity<SgEqConv>()
              .Property(r => r.Points)
              .HasConversion(new SgNegatingConv())
    };

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Converter_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl, ConverterOpts());
        await using var _ = ctx; using var __ = cn;

        SgEqHelpers.Exec(cn, "INSERT INTO G50Eq_Conv (Id, Points, Tag) VALUES (1, -42, 'test')");

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqConv>().OrderBy(x => x.Id));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqConv>().OrderBy(x => x.Id).ToListAsync();

        Assert.Single(compResult);
        Assert.Single(rtResult);
        Assert.Equal(42, compResult[0].Points);  // converter applied
        Assert.Equal(42, rtResult[0].Points);
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Converter_MultipleRows_AllConvertedEqually_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl, ConverterOpts());
        await using var _ = ctx; using var __ = cn;

        SgEqHelpers.Exec(cn,
            "INSERT INTO G50Eq_Conv (Id, Points, Tag) VALUES (1, -10, 'a'), (2, -20, 'b'), (3, -30, 'c')");

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqConv>().OrderBy(x => x.Tag));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqConv>().OrderBy(x => x.Tag).ToListAsync();

        Assert.Equal(3, compResult.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(rtResult[i].Points, compResult[i].Points);
            Assert.True(compResult[i].Points > 0, "Converter should have negated the DB value");
        }
    }
}

/// <summary>
/// Verifies that [Column] attribute-renamed columns are materialized identically
/// by the compiled and runtime paths across all lock-step providers.
/// </summary>
public class SourceGenRenamedColumnEquivalenceTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Renamed (Id INTEGER PRIMARY KEY, " +
        "FullName TEXT NOT NULL, PointsVal INTEGER NOT NULL)";

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task RenamedColumn_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new SgEqRenamed { Id = 1, Name = "Alice", Points = 100 });
        ctx.Add(new SgEqRenamed { Id = 2, Name = "Bob",   Points = 200 });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int dummy) =>
            c.Query<SgEqRenamed>().OrderBy(x => x.Id));

        var compResult = await compiled(ctx, 0);
        var rtResult = await ctx.Query<SgEqRenamed>().OrderBy(x => x.Id).ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        for (int i = 0; i < rtResult.Count; i++)
        {
            Assert.Equal(rtResult[i].Name,   compResult[i].Name);
            Assert.Equal(rtResult[i].Points, compResult[i].Points);
        }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task RenamedColumn_WhereFilter_CompiledMatchesRuntime_AllProviders(string kind)
    {
        var (cn, ctx) = SgEqHelpers.Open(kind, Ddl);
        await using var _ = ctx; using var __ = cn;

        ctx.Add(new SgEqRenamed { Id = 1, Name = "Alice", Points = 100 });
        ctx.Add(new SgEqRenamed { Id = 2, Name = "Bob",   Points = 200 });
        await ctx.SaveChangesAsync();

        var compiled = Norm.CompileQuery((DbContext c, int minPts) =>
            c.Query<SgEqRenamed>().Where(x => x.Points >= minPts).OrderBy(x => x.Id));

        var compResult = await compiled(ctx, 150);
        var rtResult = await ctx.Query<SgEqRenamed>()
            .Where(x => x.Points >= 150)
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(rtResult.Count, compResult.Count);
        Assert.Single(compResult);
        Assert.Equal("Bob", compResult[0].Name);
    }
}

/// <summary>
/// Verifies that two contexts with different providers accessing the same entity
/// type do not share cached query plans (cross-context plan isolation).
/// </summary>
public class SourceGenCrossContextPlanIsolationTests
{
    private const string Ddl =
        "CREATE TABLE G50Eq_Basic (Id INTEGER PRIMARY KEY, " +
        "Name TEXT NOT NULL, Score INTEGER NOT NULL, Note TEXT)";

    [Fact]
    public async Task TwoContexts_DifferentProviders_NoPlanCollision()
    {
        using var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using (var c = cn1.CreateCommand()) { c.CommandText = Ddl; c.ExecuteNonQuery(); }

        using var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using (var c = cn2.CreateCommand()) { c.CommandText = Ddl; c.ExecuteNonQuery(); }

        using (var c = cn1.CreateCommand())
        {
            c.CommandText = "INSERT INTO G50Eq_Basic (Id, Name, Score, Note) VALUES (1, 'Alice', 10, 'note1')";
            c.ExecuteNonQuery();
        }
        using (var c = cn2.CreateCommand())
        {
            c.CommandText = "INSERT INTO G50Eq_Basic (Id, Name, Score, Note) VALUES (1, 'Bob', 20, NULL)";
            c.ExecuteNonQuery();
        }

        await using var ctxSqlite = new DbContext(cn1, new SqliteProvider());
        await using var ctxMysql  = new DbContext(cn2, new MySqlProvider(new SqliteParameterFactory()));

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<SgEqBasic>().Where(x => x.Score >= minScore));

        var sqliteResult = await compiled(ctxSqlite, 0);
        var mysqlResult  = await compiled(ctxMysql,  0);

        Assert.Single(sqliteResult);
        Assert.Equal("Alice", sqliteResult[0].Name);
        Assert.Equal(10, sqliteResult[0].Score);

        Assert.Single(mysqlResult);
        Assert.Equal("Bob", mysqlResult[0].Name);
        Assert.Equal(20, mysqlResult[0].Score);
    }
}
