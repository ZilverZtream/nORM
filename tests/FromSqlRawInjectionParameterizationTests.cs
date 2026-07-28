using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

// Adversarial audit hunt: FromSqlRaw / FromSqlInterpolated COMPOSABLE path (deferred, derived-table wrap).
// Captures the emitted CommandText + DbParameters via an interceptor so we can PROVE a value is bound as a
// parameter (never concatenated into SQL text), and diffs composed-operator results against an in-memory oracle
// to catch parameter-numbering collisions between the raw positional params (@p0..) and composed operators.
[Trait("Category", TestCategory.Fast)]
public sealed class FromSqlRawInjectionParameterizationTests
{
    private readonly ITestOutputHelper _out;
    public FromSqlRawInjectionParameterizationTests(ITestOutputHelper o) => _out = o;

    // Records every CommandText + parameter set that flows through execution, on BOTH sync and async paths.
    private sealed class CaptureInterceptor : BaseDbCommandInterceptor
    {
        public readonly List<(string Sql, List<(string Name, object? Value)> Ps)> Captured = new();
        public CaptureInterceptor() : base(NullLogger.Instance) { }

        private void Record(DbCommand cmd)
        {
            var ps = new List<(string, object?)>();
            foreach (DbParameter p in cmd.Parameters)
                ps.Add((p.ParameterName, p.Value));
            Captured.Add((cmd.CommandText, ps));
        }
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, DbContext context)
        { Record(command); return base.ReaderExecuting(command, context); }
        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { Record(command); return base.ReaderExecutingAsync(command, context, ct); }
        public override InterceptionResult<object?> ScalarExecuting(DbCommand command, DbContext context)
        { Record(command); return base.ScalarExecuting(command, context); }
        public override Task<InterceptionResult<object?>> ScalarExecutingAsync(DbCommand command, DbContext context, CancellationToken ct)
        { Record(command); return base.ScalarExecutingAsync(command, context, ct); }
    }

    [Table("HuntWidget")]
    public sealed class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn, out CaptureInterceptor cap, params (int id, string name, int score)[] rows)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HuntWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using (var ins = cn.CreateCommand())
        {
            ins.CommandText = "INSERT INTO HuntWidget (Id,Name,Score) VALUES (@i,@n,@s)";
            var pi = ins.CreateParameter(); pi.ParameterName = "@i"; ins.Parameters.Add(pi);
            var pn = ins.CreateParameter(); pn.ParameterName = "@n"; ins.Parameters.Add(pn);
            var ps = ins.CreateParameter(); ps.ParameterName = "@s"; ins.Parameters.Add(ps);
            foreach (var r in rows) { pi.Value = r.id; pn.Value = r.name; ps.Value = r.score; ins.ExecuteNonQuery(); }
        }
        cap = new CaptureInterceptor();
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        opts.CommandInterceptors.Add(cap);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // ── HUNT 1: composable FromSqlInterpolated binds a malicious value as a parameter, not into text ──
    [Fact]
    public void composable_interpolated_binds_dangerous_value_as_parameter()
    {
        using var ctx = Ctx(out var cn, out var cap, (1, "alice", 10), (2, "bob", 20));
        using var _cn = cn;
        const string evil = "x'; DROP TABLE HuntWidget; --";

        // Compose Where over the interpolated root so the DEFERRED derived-table path is exercised.
        var rows = ctx.FromSqlInterpolated<Widget>($"SELECT * FROM HuntWidget WHERE Name = {evil}")
            .Where(w => w.Score >= 0)
            .OrderBy(w => w.Id)
            .ToList();

        Assert.Empty(rows); // no row named that; and the table must still exist afterwards

        Assert.NotEmpty(cap.Captured);
        var last = cap.Captured[^1];
        _out.WriteLine("SQL: " + last.Sql);
        foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");

        Assert.DoesNotContain("DROP TABLE", last.Sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(evil, last.Sql);
        Assert.Contains(last.Ps, p => Equals(p.Value, evil));

        // Table survived — the injection did not execute.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM HuntWidget";
        Assert.Equal(2L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── HUNT 2: composable FromSqlRaw positional param binds a malicious value as a parameter ──
    [Fact]
    public void composable_raw_positional_binds_dangerous_value_as_parameter()
    {
        using var ctx = Ctx(out var cn, out var cap, (1, "alice", 10), (2, "bob", 20));
        using var _cn = cn;
        const string evil = "' OR '1'='1";

        var rows = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Name = @p0", evil)
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();

        Assert.Empty(rows); // ' OR '1'='1 as a literal name matches nothing; if concatenated it would match ALL

        Assert.NotEmpty(cap.Captured);
        var last = cap.Captured[^1];
        _out.WriteLine("SQL: " + last.Sql);
        foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        Assert.DoesNotContain("OR '1'='1", last.Sql);
        Assert.Contains(last.Ps, p => Equals(p.Value, evil));
    }

    // ── HUNT 3: raw @p0 positional param + a composed Contains (IN list) — numbering must not collide ──
    [Fact]
    public void raw_param_plus_composed_contains_no_param_collision()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35), (5, "e", 45));
        using var _cn = cn;

        var ids = new List<int> { 2, 3, 5 };
        // Raw filter Score >= 15 (raw @p0=15). Composed Contains over ids → IN (...). If the IN clause reuses
        // @p0 (resetting the param index instead of continuing past the reserved raw slot) the raw 15 and the
        // list would cross-bind and the result would diverge from the oracle.
        var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15)
            .Where(w => ids.Contains(w.Id))
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();

        // Oracle: Score>=15 → ids 2,3,4,5 ; ∩ {2,3,5} → 2,3,5
        var expected = new[] { 2, 3, 5 };
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        Assert.Equal(expected, got);
    }

    // ── HUNT 4: raw @p0 + composed Where with TWO closure captures — all three must bind distinctly ──
    [Fact]
    public void raw_param_plus_two_composed_closures_bind_distinctly()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "keep", 5), (2, "keep", 15), (3, "drop", 25), (4, "keep", 35));
        using var _cn = cn;

        int lo = 10, hi = 40;
        string name = "keep";
        var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 1) // raw @p0 = 1 (all rows)
            .Where(w => w.Score > lo && w.Score < hi && w.Name == name)
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();

        // Oracle: Score>=1 (all) ; Score>10 && Score<40 && Name=="keep" → ids 2 (15,keep), 4 (35,keep)
        Assert.Equal(new[] { 2, 4 }, got);
    }

    // ── HUNT 5: cached parameterless raw + composed closure re-binds fresh values across calls ──
    [Fact]
    public void parameterless_raw_composed_closure_rebinds_fresh_values()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35));
        using var _cn = cn;

        List<int> Run(int threshold) => ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget")
            .Where(w => w.Score >= threshold)
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();

        Assert.Equal(new[] { 3, 4 }, Run(25)); // warms cache: threshold 25
        Assert.Equal(new[] { 2, 3, 4 }, Run(15)); // must re-bind 15, not replay cached 25
        Assert.Equal(new[] { 1, 2, 3, 4 }, Run(0));
    }

    // ── HUNT 6: interpolated DateTime + Guid values bind correctly through the composable path ──
    [Table("HuntTyped")]
    public sealed class Typed
    {
        [Key] public int Id { get; set; }
        public DateTime When { get; set; }
        public Guid Ref { get; set; }
    }

    [Fact]
    public async Task composable_interpolated_datetime_and_guid_bind_and_match()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE HuntTyped (Id INTEGER PRIMARY KEY, \"When\" TEXT NOT NULL, \"Ref\" TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var g1 = Guid.NewGuid(); var g2 = Guid.NewGuid();
        var t1 = new DateTime(2024, 1, 15, 8, 30, 0); var t2 = new DateTime(2024, 6, 1, 12, 0, 0);
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Typed>().HasKey(x => x.Id) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Seed via nORM so the stored representation matches nORM's own binding.
        await ctx.InsertAsync(new Typed { Id = 1, When = t1, Ref = g1 });
        await ctx.InsertAsync(new Typed { Id = 2, When = t2, Ref = g2 });

        var byDate = ctx.FromSqlInterpolated<Typed>($"SELECT * FROM HuntTyped WHERE \"When\" = {t2}")
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 2 }, byDate);

        var byGuid = ctx.FromSqlInterpolated<Typed>($"SELECT * FROM HuntTyped WHERE \"Ref\" = {g1}")
            .Select(x => x.Id).ToList();
        Assert.Equal(new[] { 1 }, byGuid);
    }

    // ── HUNT 7: raw @p0 + composed Where with a LITERAL constant (fixed param, not a closure) ──
    // Literal constants take a different parameterization path than captured closures. If a composed
    // literal reuses @p0 (the reserved raw slot) the raw value and the literal cross-bind.
    [Fact]
    public void raw_param_plus_composed_literal_constant_no_collision()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35), (5, "e", 45));
        using var _cn = cn;

        // raw @p0 = 15 (Score>=15 → 2,3,4,5) ; composed literal Score <= 35 → 2,3,4
        var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15)
            .Where(w => w.Score <= 35)
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        Assert.Equal(new[] { 2, 3, 4 }, got);
    }

    // ── HUNT 8: two raw positional params + composed literal/closure — full numbering sweep ──
    [Fact]
    public void two_raw_params_plus_composed_predicate_no_collision()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35), (5, "e", 45));
        using var _cn = cn;

        // raw: Score BETWEEN @p0 and @p1  (15..45 → 2,3,4,5)
        int cap1 = 40;
        var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0 AND Score <= @p1", 15, 45)
            .Where(w => w.Score < cap1)   // closure 40 → 2,3,4
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        Assert.Equal(new[] { 2, 3, 4 }, got);
    }

    // ── HUNT 9: raw param + scalar aggregate (Sum/Min/Max) composed on top ──
    [Fact]
    public void raw_param_plus_scalar_aggregate()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35));
        using var _cn = cn;

        // Score >= 15 → 15,25,35. Sum=75, Min=15, Max=35, Count=3.
        Assert.Equal(75, ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15).Sum(w => w.Score));
        Assert.Equal(15, ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15).Min(w => w.Score));
        Assert.Equal(35, ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15).Max(w => w.Score));
        Assert.Equal(3, ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15).Count());
        // Count with a composed predicate literal
        Assert.Equal(2, ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15).Count(w => w.Score > 20));
    }

    // ── HUNT 10: characterize whether the COMPOSABLE path validates raw SQL text (stacked statement) ──
    // The immediate FromSqlRawAsync validates via NormValidator. Does the deferred/composable path also gate?
    // A stacked "; DROP TABLE" would be a syntax error inside a derived table anyway — this documents which.
    [Fact]
    public void composable_stacked_statement_does_not_drop_table()
    {
        using var ctx = Ctx(out var cn, out var cap, (1, "alice", 10), (2, "bob", 20));
        using var _cn = cn;

        var ex = Record.Exception(() =>
            ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget; DROP TABLE HuntWidget").ToList());
        _out.WriteLine("Exception: " + (ex?.GetType().Name ?? "<none>") + " / " + ex?.Message);

        // Regardless of loud/quiet, the table must survive (no injection executed).
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM HuntWidget";
        Assert.Equal(2L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── HUNT 11: raw SQL references @p1 but only ONE param supplied; a composed operator then wants @p1 ──
    // The raw SQL's @p1 slot is NOT reserved (only @p0 is), so a composed literal/closure could bind @p1 and
    // silently satisfy the raw SQL's dangling @p1 reference with a composed value. Characterize loud vs silent.
    [Fact]
    public void undersupplied_raw_param_referenced_by_raw_sql_then_composed()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35));
        using var _cn = cn;

        // Raw SQL references @p0 AND @p1, but only @p0 (=15) is supplied. Then compose a literal predicate.
        var ex = Record.Exception(() =>
        {
            var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0 AND Score <= @p1", 15)
                .Where(w => w.Name == "keep")
                .OrderBy(w => w.Id)
                .Select(w => w.Id)
                .ToList();
            _out.WriteLine("Rows returned: " + string.Join(",", got));
        });
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        _out.WriteLine("Exception: " + (ex?.GetType().Name ?? "<none>") + " / " + ex?.Message);
        // We only DOCUMENT here (mismatched param count is user error). A loud error is the desired EF-like
        // behavior; a silent cross-bind of @p1 to the composed value would be the concerning outcome.
        Assert.True(true);
    }

    // ── HUNT 12: correlated EXISTS-style subquery (another entity) composed inside the raw root's Where ──
    [Table("HuntTag")]
    public sealed class Tag
    {
        [Key] public int Id { get; set; }
        public int WidgetId { get; set; }
    }

    [Fact]
    public void raw_param_plus_correlated_subquery_no_param_collision()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HuntWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
                CREATE TABLE HuntTag (Id INTEGER PRIMARY KEY, WidgetId INTEGER NOT NULL);
                INSERT INTO HuntWidget (Id,Name,Score) VALUES (1,'a',5),(2,'b',15),(3,'c',25),(4,'d',35);
                INSERT INTO HuntTag (Id,WidgetId) VALUES (1,2),(2,4);
                """;
            cmd.ExecuteNonQuery();
        }
        var cap = new CaptureInterceptor();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Widget>().HasKey(w => w.Id); mb.Entity<Tag>().HasKey(t => t.Id); }
        };
        opts.CommandInterceptors.Add(cap);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // raw @p0 = 10 (Score>=10 → 2,3,4). Composed correlated Any over Tag → widgets that have a tag → 2,4.
        var ex = Record.Exception(() =>
        {
            var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 10)
                .Where(w => ctx.Query<Tag>().Any(t => t.WidgetId == w.Id))
                .OrderBy(w => w.Id)
                .Select(w => w.Id)
                .ToList();
            _out.WriteLine("Rows: " + string.Join(",", got));
            Assert.Equal(new[] { 2, 4 }, got);
        });
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        _out.WriteLine("Exception: " + (ex?.GetType().Name ?? "<none>") + " / " + ex?.Message);
        // If this shape is unsupported it should be loud (NormUnsupportedFeatureException), not wrong rows.
        Assert.True(ex == null || ex is NormUnsupportedFeatureException || ex is NormException,
            "Unexpected exception type: " + ex);
    }

    // ── HUNT 13: over-supplied raw params (more values than @pN placeholders) + composed literal ──
    [Fact]
    public void oversupplied_raw_params_plus_composed_literal()
    {
        using var ctx = Ctx(out var cn, out var cap,
            (1, "a", 5), (2, "b", 15), (3, "c", 25), (4, "d", 35));
        using var _cn = cn;

        // Only @p0 referenced, but TWO params supplied (@p1 reserved but unused). A composed literal then
        // takes @p2. Verify the composed literal is not shifted onto the reserved-but-unused @p1 slot.
        var got = ctx.FromSqlRaw<Widget>("SELECT * FROM HuntWidget WHERE Score >= @p0", 15, 999)
            .Where(w => w.Score <= 25)
            .OrderBy(w => w.Id)
            .Select(w => w.Id)
            .ToList();
        if (cap.Captured.Count > 0)
        {
            var last = cap.Captured[^1];
            _out.WriteLine("SQL: " + last.Sql);
            foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        }
        // Score in [15,25] → 2,3. The unused @p1=999 must not affect anything.
        Assert.Equal(new[] { 2, 3 }, got);
    }

    // ── HUNT 14: NULL interpolated value binds DBNull as a parameter (not the literal text "NULL") ──
    [Fact]
    public void composable_interpolated_null_binds_dbnull_parameter()
    {
        using var ctx = Ctx(out var cn, out var cap, (1, "alice", 10), (2, "bob", 20));
        using var _cn = cn;
        string? nothing = null;
        var rows = ctx.FromSqlInterpolated<Widget>($"SELECT * FROM HuntWidget WHERE Name = {nothing}")
            .ToList();
        Assert.Empty(rows); // Name = NULL is UNKNOWN → no rows (matches what the user literally wrote)
        var last = cap.Captured[^1];
        _out.WriteLine("SQL: " + last.Sql);
        foreach (var p in last.Ps) _out.WriteLine($"  {p.Name} = [{p.Value}]");
        Assert.Contains("@p0", last.Sql);
        Assert.Contains(last.Ps, p => p.Name == "@p0" && (p.Value is DBNull || p.Value is null));
    }

    // ── HUNT 15: enum interpolated value binds its underlying integral and matches (both paths) ──
    public enum Kind { Alpha = 1, Beta = 2, Gamma = 3 }

    [Table("HuntEnum")]
    public sealed class EnumRow
    {
        [Key] public int Id { get; set; }
        public int KindValue { get; set; }
    }

    [Fact]
    public async Task interpolated_enum_binds_underlying_integral_and_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE HuntEnum (Id INTEGER PRIMARY KEY, KindValue INTEGER NOT NULL);
                INSERT INTO HuntEnum (Id,KindValue) VALUES (1,1),(2,2),(3,3);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<EnumRow>().HasKey(x => x.Id) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var k = Kind.Beta; // underlying 2

        // Immediate interpolated path
        var immediate = await ctx.FromSqlInterpolatedAsync<EnumRow>(
            $"SELECT Id, KindValue FROM HuntEnum WHERE KindValue = {k}");
        Assert.Equal(new[] { 2 }, immediate.Select(r => r.Id).ToArray());

        // Composable interpolated path
        var composable = ctx.FromSqlInterpolated<EnumRow>($"SELECT * FROM HuntEnum WHERE KindValue = {k}")
            .Select(r => r.Id).ToList();
        Assert.Equal(new[] { 2 }, composable);
    }
}
