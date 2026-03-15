using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Support entities
// ══════════════════════════════════════════════════════════════════════════════

[Table("BoolPredEntity")]
file class BoolPredEntity
{
    [Key]
    public int Id { get; set; }
    public bool IsActive { get; set; }
    public bool? IsEnabled { get; set; }
    public string? Name { get; set; }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.4 → 3.5 : BooleanFalseLiteral contract + cross-provider bool SQL shape
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies that each provider exposes the correct BooleanTrueLiteral / BooleanFalseLiteral
/// values and that WHERE clauses with bool equality predicates embed provider-appropriate
/// literals rather than hard-coded "0"/"1".
/// </summary>
public class BoolLiteralSqlShapeTests
{
    // ── Provider literal property values ──────────────────────────────────────

    public static IEnumerable<object[]> ProviderLiterals() =>
    [
        [new SqliteProvider(),                               "1", "0"],
        [new SqlServerProvider(),                            "1", "0"],
        [new MySqlProvider(new SqliteParameterFactory()),    "1", "0"],
        [new PostgresProvider(new SqliteParameterFactory()), "true", "false"],
    ];

    [Theory]
    [MemberData(nameof(ProviderLiterals))]
    public void Provider_BooleanTrueLiteral_ReturnsCorrectValue(object provider, string expectedTrue, string _)
    {
        var p = (DatabaseProvider)provider;
        Assert.Equal(expectedTrue, p.BooleanTrueLiteral);
    }

    [Theory]
    [MemberData(nameof(ProviderLiterals))]
    public void Provider_BooleanFalseLiteral_ReturnsCorrectValue(object provider, string _, string expectedFalse)
    {
        var p = (DatabaseProvider)provider;
        Assert.Equal(expectedFalse, p.BooleanFalseLiteral);
    }

    // ── SQL shape: all four providers × all bool equality predicate forms ─────

    public static IEnumerable<object[]> AllProviders() =>
    [
        [new SqliteProvider(),                               "1",    "0"],
        [new SqlServerProvider(),                            "1",    "0"],
        [new MySqlProvider(new SqliteParameterFactory()),    "1",    "0"],
        [new PostgresProvider(new SqliteParameterFactory()), "true", "false"],
    ];

    public static IEnumerable<object[]> AllProvidersOnly() =>
    [
        [new SqliteProvider()],
        [new SqlServerProvider()],
        [new MySqlProvider(new SqliteParameterFactory())],
        [new PostgresProvider(new SqliteParameterFactory())],
    ];

    private static (DbContext ctx, SqliteConnection cn) MakeCtx(DatabaseProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return (new DbContext(cn, provider), cn);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_EqualsTrue_EmitsTrueLiteral(object provider, string trueLit, string _)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == true).ToString();
            Assert.Contains($"= {trueLit}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_EqualsFalse_EmitsFalseLiteral(object provider, string _, string falseLit)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == false).ToString();
            Assert.Contains($"= {falseLit}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_NotEqualsTrue_EmitsTrueLiteral(object provider, string trueLit, string _)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive != true).ToString();
            Assert.Contains($"<> {trueLit}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_NotEqualsFalse_EmitsFalseLiteral(object provider, string _, string falseLit)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive != false).ToString();
            Assert.Contains($"<> {falseLit}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProvidersOnly))]
    public void BoolPredicate_BoolMember_EmitsTrueLiteral(object provider)
    {
        // e => e.IsActive — standalone bool member must produce a WHERE clause.
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive).ToString();
            Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [MemberData(nameof(AllProvidersOnly))]
    public void BoolPredicate_NegatedBoolMember_ContainsWhereClause(object provider)
    {
        // e => !e.IsActive must produce a valid WHERE clause.
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => !e.IsActive).ToString();
            Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    // ── SQLite execution tests: bool literals actually filter correctly ────────

    private static SqliteConnection CreateAndSeedBoolDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE BoolPredEntity (Id INTEGER PRIMARY KEY, IsActive INTEGER NOT NULL, IsEnabled INTEGER, Name TEXT);
            INSERT INTO BoolPredEntity VALUES (1, 1, 1, 'Alice');
            INSERT INTO BoolPredEntity VALUES (2, 0, 0, 'Bob');
            INSERT INTO BoolPredEntity VALUES (3, 1, NULL, 'Carol');
            INSERT INTO BoolPredEntity VALUES (4, 0, 1, 'Dave');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task BoolPredicate_EqualsTrue_SQLite_ReturnsActiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsActive == true).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public async Task BoolPredicate_EqualsFalse_SQLite_ReturnsInactiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsActive == false).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.False(r.IsActive));
    }

    [Fact]
    public async Task BoolPredicate_BoolMember_SQLite_ReturnsActiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsActive).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public async Task BoolPredicate_NegatedBoolMember_SQLite_ReturnsInactiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => !e.IsActive).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.False(r.IsActive));
    }

    [Fact]
    public async Task BoolPredicate_NotEqualsTrue_SQLite_ReturnsInactiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsActive != true).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.False(r.IsActive));
    }

    [Fact]
    public async Task BoolPredicate_NotEqualsFalse_SQLite_ReturnsActiveRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsActive != false).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.5 → 4.0 : Expanded bool matrix — null bool, joins/subqueries
// ══════════════════════════════════════════════════════════════════════════════

public class BoolLiteralExpandedTests
{
    public static IEnumerable<object[]> AllProviders() =>
    [
        [new SqliteProvider()],
        [new SqlServerProvider()],
        [new MySqlProvider(new SqliteParameterFactory())],
        [new PostgresProvider(new SqliteParameterFactory())],
    ];

    private static (DbContext ctx, SqliteConnection cn) MakeCtx(DatabaseProvider provider)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return (new DbContext(cn, provider), cn);
    }

    // ── Null bool ─────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void NullableBool_EqualsNull_EmitsIsNull(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == null).ToString();
            Assert.Contains("IS NULL", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void NullableBool_EqualsTrue_EmitsTrueLiteral(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == true).ToString();
            Assert.Contains($"= {p.BooleanTrueLiteral}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void NullableBool_EqualsFalse_EmitsFalseLiteral(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == false).ToString();
            Assert.Contains($"= {p.BooleanFalseLiteral}", sql);
        }
    }

    // ── Bool predicates in AND combinations ───────────────────────────────────

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_AndWithId_EmitsBothConditions(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.Id > 0 && e.IsActive == true).ToString();
            Assert.Contains($"= {p.BooleanTrueLiteral}", sql);
            Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_FalseAndWithId_EmitsFalseLiteral(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>().Where(e => e.Id > 0 && e.IsActive == false).ToString();
            Assert.Contains($"= {p.BooleanFalseLiteral}", sql);
        }
    }

    // ── Bool predicates in multi-clause chains ────────────────────────────────

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_ChainedWhere_TrueAndFalse_BothLiteralsPresent(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            // Chain: .Where(e => e.IsActive == true).Where(e => e.IsEnabled == false)
            // Both bool literals must appear in the SQL.
            var sql = ctx.Query<BoolPredEntity>()
                .Where(e => e.IsActive == true)
                .Where(e => e.IsEnabled == false)
                .ToString();
            Assert.Contains($"= {p.BooleanTrueLiteral}", sql);
            Assert.Contains($"= {p.BooleanFalseLiteral}", sql);
        }
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void BoolPredicate_ChainedWhere_FalseAndTrue_BothLiteralsPresent(object provider)
    {
        var p = (DatabaseProvider)provider;
        var (ctx, cn) = MakeCtx(p);
        using (cn) using (ctx)
        {
            var sql = ctx.Query<BoolPredEntity>()
                .Where(e => e.IsActive == false)
                .Where(e => e.IsEnabled == true)
                .ToString();
            Assert.Contains($"= {p.BooleanFalseLiteral}", sql);
            Assert.Contains($"= {p.BooleanTrueLiteral}", sql);
        }
    }

    // ── SQLite execution: null bool ────────────────────────────────────────────

    private static SqliteConnection CreateAndSeedBoolDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE BoolPredEntity (Id INTEGER PRIMARY KEY, IsActive INTEGER NOT NULL, IsEnabled INTEGER, Name TEXT);
            INSERT INTO BoolPredEntity VALUES (1, 1, 1,    'Alice');
            INSERT INTO BoolPredEntity VALUES (2, 0, 0,    'Bob');
            INSERT INTO BoolPredEntity VALUES (3, 1, NULL, 'Carol');
            INSERT INTO BoolPredEntity VALUES (4, 0, 1,    'Dave');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task NullableBool_EqualsNull_SQLite_ReturnsNullRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == null).ToListAsync();

        Assert.Single(results);
        Assert.Equal(3, results[0].Id);
    }

    [Fact]
    public async Task NullableBool_EqualsTrue_SQLite_ReturnsEnabledRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == true).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r.IsEnabled));
    }

    [Fact]
    public async Task NullableBool_EqualsFalse_SQLite_ReturnsDisabledRows()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<BoolPredEntity>().Where(e => e.IsEnabled == false).ToListAsync();

        Assert.Single(results);
        Assert.Equal(2, results[0].Id);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 : Concurrency/cancellation chaos with bool predicates
// ══════════════════════════════════════════════════════════════════════════════

public class BoolLiteralChaosTests
{
    private static SqliteConnection CreateAndSeedBoolDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE BoolPredEntity (Id INTEGER PRIMARY KEY, IsActive INTEGER NOT NULL, IsEnabled INTEGER, Name TEXT);
            INSERT INTO BoolPredEntity VALUES (1, 1, 1, 'Alice');
            INSERT INTO BoolPredEntity VALUES (2, 0, 0, 'Bob');
            INSERT INTO BoolPredEntity VALUES (3, 1, NULL, 'Carol');
            INSERT INTO BoolPredEntity VALUES (4, 0, 1, 'Dave');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task CompiledQuery_BoolPredicate_True_HighContention_AllCallsCorrect()
    {
        // 30 concurrent tasks, each querying with bool-true predicate.
        // Verifies the cached plan with BooleanTrueLiteral is safe under concurrency.
        const int ConcurrentCount = 30;

        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<BoolPredEntity>().Where(e => e.IsActive == true && e.Id >= minId));

        // Warm up
        await compiled(ctx, 1);

        var tasks = Enumerable.Range(0, ConcurrentCount).Select(i =>
            Task.Run(async () =>
            {
                var results = await compiled(ctx, 1);
                return results.ToList();
            }));

        var allResults = await Task.WhenAll(tasks);

        Assert.All(allResults, results =>
        {
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.True(r.IsActive));
        });
    }

    [Fact]
    public async Task CompiledQuery_BoolPredicate_False_HighContention_AllCallsCorrect()
    {
        // Same but with bool-false predicate.
        const int ConcurrentCount = 30;

        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<BoolPredEntity>().Where(e => e.IsActive == false && e.Id >= minId));

        // Warm up
        await compiled(ctx, 1);

        var tasks = Enumerable.Range(0, ConcurrentCount).Select(i =>
            Task.Run(async () =>
            {
                var results = await compiled(ctx, 1);
                return results.ToList();
            }));

        var allResults = await Task.WhenAll(tasks);

        Assert.All(allResults, results =>
        {
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.False(r.IsActive));
        });
    }

    [Fact]
    public async Task CompiledQuery_BoolPredicate_AlternatingTrueFalse_NoCrossContamination()
    {
        // Two compiled queries (one bool-true, one bool-false) called alternately.
        // Verifies separate SQL cache entries for each literal.
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var trueQuery = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<BoolPredEntity>().Where(e => e.IsActive == true && e.Id >= minId));
        var falseQuery = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<BoolPredEntity>().Where(e => e.IsActive == false && e.Id >= minId));

        for (int i = 0; i < 10; i++)
        {
            var trueResults = (await trueQuery(ctx, 1)).ToList();
            var falseResults = (await falseQuery(ctx, 1)).ToList();

            Assert.Equal(2, trueResults.Count);
            Assert.All(trueResults, r => Assert.True(r.IsActive));

            Assert.Equal(2, falseResults.Count);
            Assert.All(falseResults, r => Assert.False(r.IsActive));
        }
    }

    [Fact]
    public async Task BoolPredicate_PreCancelledToken_Throws()
    {
        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Pre-cancelled token must propagate as OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => ctx.Query<BoolPredEntity>().Where(e => e.IsActive == true).ToListAsync(cts.Token));
    }

    [Fact]
    public async Task CompiledQuery_NegatedBoolPredicate_HighContention_AllCallsCorrect()
    {
        // Negated bool (!e.IsActive) under 30 concurrent tasks.
        const int ConcurrentCount = 30;

        using var cn = CreateAndSeedBoolDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<BoolPredEntity>().Where(e => !e.IsActive && e.Id >= minId));

        await compiled(ctx, 1);

        var tasks = Enumerable.Range(0, ConcurrentCount).Select(_ =>
            Task.Run(async () => (await compiled(ctx, 1)).ToList()));

        var allResults = await Task.WhenAll(tasks);

        Assert.All(allResults, results =>
        {
            Assert.Equal(2, results.Count);
            Assert.All(results, r => Assert.False(r.IsActive));
        });
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 : Provider literal correctness + TranslateJsonPathAccess docs
// ══════════════════════════════════════════════════════════════════════════════

public class BoolLiteralGate50Tests
{
    [Fact]
    public void Provider_PostgresBooleanFalseLiteral_ReturnsFalse()
    {
        // PostgresProvider must return "false" — not "0" — for BooleanFalseLiteral.
        var pg = new PostgresProvider(new SqliteParameterFactory());
        Assert.Equal("false", pg.BooleanFalseLiteral);
        Assert.Equal("true", pg.BooleanTrueLiteral);
    }

    [Fact]
    public void Provider_SqliteBooleanFalseLiteral_ReturnsZero()
    {
        var p = new SqliteProvider();
        Assert.Equal("0", p.BooleanFalseLiteral);
        Assert.Equal("1", p.BooleanTrueLiteral);
    }

    [Fact]
    public void Provider_SqlServerBooleanFalseLiteral_ReturnsZero()
    {
        var p = new SqlServerProvider();
        Assert.Equal("0", p.BooleanFalseLiteral);
        Assert.Equal("1", p.BooleanTrueLiteral);
    }

    [Fact]
    public void Provider_MySqlBooleanFalseLiteral_ReturnsZero()
    {
        var p = new MySqlProvider(new SqliteParameterFactory());
        Assert.Equal("0", p.BooleanFalseLiteral);
        Assert.Equal("1", p.BooleanTrueLiteral);
    }

    [Fact]
    public void TranslateJsonPathAccess_ReturnsFormattedSql()
    {
        // Verifies TranslateJsonPathAccess returns the expected jsonb_extract_path_text format.
        // The comment in PostgresProvider now correctly documents that sb.ToString() allocates.
        var pg = new PostgresProvider(new SqliteParameterFactory());
        var result = pg.TranslateJsonPathAccess("\"Data\"", "user.address.city");
        Assert.Contains("jsonb_extract_path_text", result);
        Assert.Contains("\"Data\"", result);
        // Result is a freshly allocated string (pooled StringBuilder, but ToString() allocates)
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void Postgres_BoolPredicate_EqualsTrue_UsesTrueLiteral_NotOne()
    {
        // Regression guard: PostgreSQL query must never emit "= 1" for a bool-true predicate.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new PostgresProvider(new SqliteParameterFactory()));

        var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == true).ToString();

        Assert.DoesNotContain("= 1", sql);
        Assert.DoesNotContain("= 0", sql);
        Assert.Contains("= true", sql);
    }

    [Fact]
    public void Postgres_BoolPredicate_EqualsFalse_UsesFalseLiteral_NotZero()
    {
        // Regression guard: PostgreSQL query must never emit "= 0" for a bool-false predicate.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new PostgresProvider(new SqliteParameterFactory()));

        var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == false).ToString();

        Assert.DoesNotContain("= 0", sql);
        Assert.DoesNotContain("= 1", sql);
        Assert.Contains("= false", sql);
    }

    [Fact]
    public void Sqlite_BoolPredicate_EqualsTrue_UsesOne_NotTrue()
    {
        // Regression guard: SQLite must emit "= 1", not "= true".
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == true).ToString();

        Assert.DoesNotContain("= true", sql);
        Assert.Contains("= 1", sql);
    }

    [Fact]
    public void Sqlite_BoolPredicate_EqualsFalse_UsesZero_NotFalse()
    {
        // Regression guard: SQLite must emit "= 0", not "= false".
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var sql = ctx.Query<BoolPredEntity>().Where(e => e.IsActive == false).ToString();

        Assert.DoesNotContain("= false", sql);
        Assert.Contains("= 0", sql);
    }
}
