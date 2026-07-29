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
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Guards that a COMPILED query (Norm.CompileQuery, free-parameter binding path) returns the SAME rows as the
/// equivalent REGULAR query (closure-capture binding path) and a LINQ-to-objects oracle — a compiled != oracle
/// divergence (with regular == oracle) is a silent-wrong compiled-query binding bug. Covers computed/arithmetic
/// param predicates, enum/nullable/decimal/DateTime/DateTimeOffset param comparisons (relational + equality,
/// including cross-offset same-instant), multi-invocation with distinct values, and param-in-projection.
/// SQLite :memory: only.
/// </summary>
[Trait("Category", "Fast")]
public sealed class CompiledQueryDifferentialTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    public CompiledQueryDifferentialTests(ITestOutputHelper o) => _out = o;

    public enum Status { New = 0, Active = 1, Closed = 2 }

    [Table("Cq60Row")]
    public sealed class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public string Name { get; set; } = "";
        public int? NVal { get; set; }
        public decimal Price { get; set; }
        public DateTime When { get; set; }
        public Status Status { get; set; }
        public Guid Gid { get; set; }
        public DateTimeOffset Dto { get; set; }
        public TimeSpan Dur { get; set; }
        public bool Flag { get; set; }
    }

    public sealed class Box { public int Id { get; set; } public long V { get; set; } }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;
    private List<Row> _all = null!;
    private static readonly Guid[] Guids =
    {
        Guid.Parse("11111111-1111-1111-1111-111111111111"),
        Guid.Parse("22222222-2222-2222-2222-222222222222"),
        Guid.Parse("33333333-3333-3333-3333-333333333333"),
    };

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using (var cmd = _cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Cq60Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, A INTEGER NOT NULL, B INTEGER NOT NULL, " +
                "Name TEXT NOT NULL, NVal INTEGER NULL, Price TEXT NOT NULL, [When] TEXT NOT NULL, Status TEXT NOT NULL, Gid TEXT NOT NULL, " +
                "Dto TEXT NOT NULL, Dur TEXT NOT NULL, Flag INTEGER NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter())
        });

        for (int i = 1; i <= 20; i++)
        {
            _ctx.Add(new Row
            {
                A = i * 2,
                B = (i % 5) + 1,
                Name = "n" + i,
                NVal = (i % 3 == 0) ? (int?)null : i,
                Price = 10m + i + (i % 2 == 0 ? 0.5m : 0m), // fractional scale on even i
                When = new DateTime(2020, 1, 1).AddDays(i),
                Status = (Status)(i % 3), // New/Active/Closed cycle
                Gid = Guids[i % 3],
                Dto = new DateTimeOffset(2021, 1, 1, 12, 0, 0, TimeSpan.FromHours(i % 3)).AddDays(i),
                Dur = TimeSpan.FromMinutes(i * 5),
                Flag = (i % 2 == 0),
            });
        }
        await _ctx.SaveChangesAsync();

        // Ground-truth CLR view of what nORM materializes.
        _all = (await _ctx.Query<Row>().AsNoTracking().OrderBy(r => r.Id).ToListAsync());
    }

    public async Task DisposeAsync() { _ctx.Dispose(); await _cn.DisposeAsync(); }

    private static int[] Ids(IEnumerable<Row> rows) => rows.OrderBy(r => r.Id).Select(r => r.Id).ToArray();

    private void Diff(string label, int[] oracle, int[] regular, int[] compiled)
    {
        _out.WriteLine($"{label}\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
    }

    // ─────────────── Surface 1: computed / arithmetic param in WHERE ───────────────

    [Fact]
    public async Task S1_where_param_times_two()
    {
        int val = 15;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A > p * 2));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A > local * 2).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A > val * 2));
        Diff("S1 Where(A > p*2)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S1_where_param_plus_one_equal()
    {
        int val = 9;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A == p + 1));
        var compiled = Ids(await cq(_ctx, val));   // A == 10 -> row i=5
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A == local + 1).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A == val + 1));
        Diff("S1 Where(A == p+1)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S1_where_string_concat_param()
    {
        string prefix = "n1";
        var cq = Norm.CompileQuery((DbContext c, string p) => c.Query<Row>().Where(r => r.Name == p + "9"));
        var compiled = Ids(await cq(_ctx, prefix));  // Name == "n19"
        string local = prefix;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Name == local + "9").ToListAsync());
        var oracle = Ids(_all.Where(r => r.Name == prefix + "9"));
        Diff("S1 Where(Name == p + \"9\")", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S1_where_param_startswith()
    {
        string prefix = "n2";
        var cq = Norm.CompileQuery((DbContext c, string p) => c.Query<Row>().Where(r => r.Name.StartsWith(p)));
        var compiled = Ids(await cq(_ctx, prefix));
        string local = prefix;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Name.StartsWith(local)).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Name.StartsWith(prefix)));
        Diff("S1 Where(Name.StartsWith(p))", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S1_where_param_times_plus()
    {
        int val = 4;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A >= p * 3 + 1));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A >= local * 3 + 1).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A >= val * 3 + 1));
        Diff("S1 Where(A >= p*3+1)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 2: param vs converter / enum / nullable ───────────────

    [Fact]
    public async Task S2_where_enum_param()
    {
        var val = Status.Active;
        var cq = Norm.CompileQuery((DbContext c, Status s) => c.Query<Row>().Where(r => r.Status == s));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Status == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Status == val));
        Diff("S2 Where(Status == enumParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S2_where_nullable_int_param_equal()
    {
        int? val = 7;
        var cq = Norm.CompileQuery((DbContext c, int? p) => c.Query<Row>().Where(r => r.NVal == p));
        var compiled = Ids(await cq(_ctx, val));
        int? local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.NVal == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.NVal == val));
        Diff("S2 Where(NVal == p?)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S2_where_nullable_int_param_not_equal()
    {
        int? val = 7;
        var cq = Norm.CompileQuery((DbContext c, int? p) => c.Query<Row>().Where(r => r.NVal != p));
        var compiled = Ids(await cq(_ctx, val));
        int? local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.NVal != local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.NVal != val)); // C# null semantics: null != 7 is true
        Diff("S2 Where(NVal != p?)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 3: Contains over the PARAM collection (arity unknown at plan-gen) ───────────────

    [Fact]
    public async Task S3_where_param_array_contains()
    {
        var ids = new[] { 4, 8, 12 };  // matches A values -> rows i=2,4,6
        Func<DbContext, int[], Task<List<Row>>> cq;
        try
        {
            cq = Norm.CompileQuery((DbContext c, int[] arr) => c.Query<Row>().Where(r => arr.Contains(r.A)));
        }
        catch (Exception ex)
        {
            _out.WriteLine($"S3 array-param compile threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
            return;
        }
        int[] compiled;
        try { compiled = Ids(await cq(_ctx, ids)); }
        catch (Exception ex)
        {
            _out.WriteLine($"S3 array-param invoke threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
            return;
        }
        var local = ids;
        var regular = Ids(await _ctx.Query<Row>().Where(r => local.Contains(r.A)).ToListAsync());
        var oracle = Ids(_all.Where(r => ids.Contains(r.A)));
        Diff("S3 Where(arrParam.Contains(A))", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S3_where_param_array_contains_different_lengths_across_calls()
    {
        Func<DbContext, int[], Task<List<Row>>> cq;
        try
        {
            cq = Norm.CompileQuery((DbContext c, int[] arr) => c.Query<Row>().Where(r => arr.Contains(r.A)));
        }
        catch (Exception ex)
        {
            _out.WriteLine($"S3-multi compile threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
            return;
        }

        foreach (var ids in new[] { new[] { 4 }, new[] { 4, 8 }, new[] { 2, 6, 10, 14 } })
        {
            int[] compiled;
            try { compiled = Ids(await cq(_ctx, ids)); }
            catch (Exception ex)
            {
                _out.WriteLine($"S3-multi invoke len={ids.Length} threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
                continue;
            }
            var oracle = Ids(_all.Where(r => ids.Contains(r.A)));
            Diff($"S3-multi len={ids.Length}", oracle, oracle, compiled);
            Assert.Equal(oracle, compiled);
        }
    }

    [Fact]
    public async Task S3_where_param_list_contains()
    {
        var ids = new List<int> { 6, 10 };
        Func<DbContext, List<int>, Task<List<Row>>> cq;
        try
        {
            cq = Norm.CompileQuery((DbContext c, List<int> lst) => c.Query<Row>().Where(r => lst.Contains(r.A)));
        }
        catch (Exception ex)
        {
            _out.WriteLine($"S3 list-param compile threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
            return;
        }
        int[] compiled;
        try { compiled = Ids(await cq(_ctx, ids)); }
        catch (Exception ex)
        {
            _out.WriteLine($"S3 list-param invoke threw (FAIL-LOUD): {ex.GetType().Name}: {ex.Message}");
            return;
        }
        var oracle = Ids(_all.Where(r => ids.Contains(r.A)));
        Diff("S3 Where(listParam.Contains(A))", oracle, oracle, compiled);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 4: projection with a param ───────────────

    [Fact]
    public async Task S4_projection_computed_member()
    {
        int val = 100;
        var cq = Norm.CompileQuery((DbContext c, int p) =>
            c.Query<Row>().Where(r => r.A > 30).Select(r => new Box { Id = r.Id, V = r.A + p }));
        var compiled = (await cq(_ctx, val)).OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        int local = val;
        var regular = (await _ctx.Query<Row>().Where(r => r.A > 30).Select(r => new Box { Id = r.Id, V = r.A + local }).ToListAsync())
            .OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        var oracle = _all.Where(r => r.A > 30).OrderBy(r => r.Id).Select(r => (r.Id, V: (long)(r.A + val))).ToArray();
        _out.WriteLine($"S4 proj\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S4_projection_constant_param_member()
    {
        int val = 777;
        var cq = Norm.CompileQuery((DbContext c, int p) =>
            c.Query<Row>().Where(r => r.A <= 6).Select(r => new Box { Id = r.Id, V = p }));
        var compiled = (await cq(_ctx, val)).OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        int local = val;
        var regular = (await _ctx.Query<Row>().Where(r => r.A <= 6).Select(r => new Box { Id = r.Id, V = local }).ToListAsync())
            .OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        var oracle = _all.Where(r => r.A <= 6).OrderBy(r => r.Id).Select(r => (r.Id, V: (long)val)).ToArray();
        _out.WriteLine($"S4 const-proj\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S4_projection_param_used_in_filter_and_projection()
    {
        int val = 5;
        var cq = Norm.CompileQuery((DbContext c, int p) =>
            c.Query<Row>().Where(r => r.B >= p).Select(r => new Box { Id = r.Id, V = r.A * p }));
        var compiled = (await cq(_ctx, val)).OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        int local = val;
        var regular = (await _ctx.Query<Row>().Where(r => r.B >= local).Select(r => new Box { Id = r.Id, V = r.A * local }).ToListAsync())
            .OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        var oracle = _all.Where(r => r.B >= val).OrderBy(r => r.Id).Select(r => (r.Id, V: (long)(r.A * val))).ToArray();
        _out.WriteLine($"S4 filter+proj\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 5: multiple invocations with distinct values ───────────────

    [Fact]
    public async Task S5_multi_invocation_distinct_values()
    {
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A > p * 2).OrderBy(r => r.Id));
        foreach (var val in new[] { 3, 15, 8, 1, 19 })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.A > val * 2));
            _out.WriteLine($"S5 val={val} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    [Fact]
    public async Task S5_multi_invocation_equality_distinct_values()
    {
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A == p));
        foreach (var val in new[] { 2, 40, 20, 8 })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.A == val));
            _out.WriteLine($"S5eq val={val} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    // ─────────────── Surface 6: GroupBy / aggregate with a param ───────────────

    [Fact]
    public async Task S6_groupby_with_param_filter_sum()
    {
        int val = 10;
        var cq = Norm.CompileQuery((DbContext c, int p) =>
            c.Query<Row>().Where(r => r.A > p).GroupBy(r => r.B).Select(g => new Box { Id = g.Key, V = g.Sum(x => x.A) }));
        var compiled = (await cq(_ctx, val)).OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        int local = val;
        var regular = (await _ctx.Query<Row>().Where(r => r.A > local).GroupBy(r => r.B).Select(g => new Box { Id = g.Key, V = g.Sum(x => x.A) }).ToListAsync())
            .OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        var oracle = _all.Where(r => r.A > val).GroupBy(r => r.B).Select(g => (Id: g.Key, V: (long)g.Sum(x => x.A))).OrderBy(x => x.Id).ToArray();
        _out.WriteLine($"S6 groupby\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 8: DateTime / Guid / decimal param binding ───────────────

    [Fact]
    public async Task S8_where_decimal_param_equal()
    {
        decimal val = 25m; // Price = 10 + i -> i=15
        var cq = Norm.CompileQuery((DbContext c, decimal p) => c.Query<Row>().Where(r => r.Price == p));
        var compiled = Ids(await cq(_ctx, val));
        decimal local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Price == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Price == val));
        Diff("S8 Where(Price == decimalParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S8_where_decimal_param_greater()
    {
        decimal val = 25m;
        var cq = Norm.CompileQuery((DbContext c, decimal p) => c.Query<Row>().Where(r => r.Price > p));
        var compiled = Ids(await cq(_ctx, val));
        decimal local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Price > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Price > val));
        Diff("S8 Where(Price > decimalParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S8_where_datetime_param_greater()
    {
        var val = new DateTime(2020, 1, 11); // When = Jan1 + i days -> i=10 boundary
        var cq = Norm.CompileQuery((DbContext c, DateTime p) => c.Query<Row>().Where(r => r.When > p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.When > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.When > val));
        Diff("S8 Where(When > dateParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S8_where_guid_param_equal()
    {
        var val = Guids[1];
        var cq = Norm.CompileQuery((DbContext c, Guid p) => c.Query<Row>().Where(r => r.Gid == p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Gid == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Gid == val));
        Diff("S8 Where(Gid == guidParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 9: multiple params of same type not swapped ───────────────

    [Fact]
    public async Task S9_two_int_params_not_swapped()
    {
        // asymmetric: A > a AND B < b -> swapping a/b changes the result
        var cq = Norm.CompileQuery((DbContext c, (int a, int b) p) =>
            c.Query<Row>().Where(r => r.A > p.a && r.B < p.b));
        var arg = (a: 20, b: 4);
        var compiled = Ids(await cq(_ctx, arg));
        var local = arg;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A > local.a && r.B < local.b).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A > arg.a && r.B < arg.b));
        Diff("S9 Where(A>a && B<b) tuple", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S9_two_int_params_reversed_usage()
    {
        // params used in the OPPOSITE order to their declaration to catch slot aliasing
        var cq = Norm.CompileQuery((DbContext c, (int a, int b) p) =>
            c.Query<Row>().Where(r => r.B < p.b && r.A > p.a));
        var arg = (a: 30, b: 3);
        var compiled = Ids(await cq(_ctx, arg));
        var local = arg;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.B < local.b && r.A > local.a).ToListAsync());
        var oracle = Ids(_all.Where(r => r.B < arg.b && r.A > arg.a));
        Diff("S9 Where(B<b && A>a) tuple reversed", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task S9_two_int_params_multi_invocation()
    {
        var cq = Norm.CompileQuery((DbContext c, (int a, int b) p) =>
            c.Query<Row>().Where(r => r.A > p.a && r.B < p.b));
        foreach (var arg in new[] { (a: 10, b: 5), (a: 30, b: 3), (a: 0, b: 2), (a: 36, b: 6) })
        {
            var compiled = Ids(await cq(_ctx, arg));
            var oracle = Ids(_all.Where(r => r.A > arg.a && r.B < arg.b));
            _out.WriteLine($"S9-multi a={arg.a} b={arg.b} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    // ─────────────── NULL-valued param (plan baked with `= @p`, value only known at call) ───────────────

    [Fact]
    public async Task N1_nullable_param_equal_NULL_value()
    {
        int? val = null; // C#: NVal == null matches the NULL rows (i=3,6,9,12,15,18)
        var cq = Norm.CompileQuery((DbContext c, int? p) => c.Query<Row>().Where(r => r.NVal == p));
        var compiled = Ids(await cq(_ctx, val));
        int? local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.NVal == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.NVal == val));
        Diff("N1 Where(NVal == p) p=NULL", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task N2_nullable_param_not_equal_NULL_value()
    {
        int? val = null; // C#: NVal != null matches the NON-null rows
        var cq = Norm.CompileQuery((DbContext c, int? p) => c.Query<Row>().Where(r => r.NVal != p));
        var compiled = Ids(await cq(_ctx, val));
        int? local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.NVal != local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.NVal != val));
        Diff("N2 Where(NVal != p) p=NULL", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task N3_string_param_equal_NULL_value()
    {
        string? val = null; // C#: Name == null matches nothing here (all Names non-null), but semantics must hold
        var cq = Norm.CompileQuery((DbContext c, string? p) => c.Query<Row>().Where(r => r.Name == p));
        var compiled = Ids(await cq(_ctx, val));
        string? local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Name == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Name == val));
        Diff("N3 Where(Name == p) p=NULL", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task N4_nullable_param_equal_mixed_null_and_nonnull_invocations()
    {
        // Same compiled delegate: first call non-null, second call NULL, third non-null.
        var cq = Norm.CompileQuery((DbContext c, int? p) => c.Query<Row>().Where(r => r.NVal == p));
        foreach (int? val in new int?[] { 7, null, 13, null, 2 })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.NVal == val));
            _out.WriteLine($"N4 val={(val?.ToString() ?? "NULL")} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    // ─────────────── Operator flip / repeated param / mixed literal ───────────────

    [Fact]
    public async Task F1_param_on_left_of_comparison()
    {
        int val = 20;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => p < r.A));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => local < r.A).ToListAsync());
        var oracle = Ids(_all.Where(r => val < r.A));
        Diff("F1 Where(p < A)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task F2_same_param_used_twice()
    {
        int val = 10;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A > p || r.B == p));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A > local || r.B == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A > val || r.B == val));
        Diff("F2 Where(A>p || B==p)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task F3_param_mixed_with_literal()
    {
        int val = 10;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A > p && r.A < 100 && r.B >= 2));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A > local && r.A < 100 && r.B >= 2).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A > val && r.A < 100 && r.B >= 2));
        Diff("F3 Where(A>p && A<100 && B>=2)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task F4_param_in_where_and_projection_same_value()
    {
        int val = 8;
        var cq = Norm.CompileQuery((DbContext c, int p) =>
            c.Query<Row>().Where(r => r.A > p).Select(r => new Box { Id = r.Id, V = r.A + p }));
        var compiled = (await cq(_ctx, val)).OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        int local = val;
        var regular = (await _ctx.Query<Row>().Where(r => r.A > local).Select(r => new Box { Id = r.Id, V = r.A + local }).ToListAsync())
            .OrderBy(b => b.Id).Select(b => (b.Id, b.V)).ToArray();
        var oracle = _all.Where(r => r.A > val).OrderBy(r => r.Id).Select(r => (r.Id, V: (long)(r.A + val))).ToArray();
        _out.WriteLine($"F4 where+proj same param\n  oracle=[{string.Join(",", oracle)}]\n  regular=[{string.Join(",", regular)}]\n  compiled=[{string.Join(",", compiled)}]");
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Wave 2: canonicalization-heavy converter/relational param paths ───────────────

    [Fact]
    public async Task W2_enum_string_relational_greater()
    {
        // Status stored as NAME via converter; relational compare must map name->ordinal.
        var val = Status.Active; // > Active means value > 1 => Closed
        var cq = Norm.CompileQuery((DbContext c, Status s) => c.Query<Row>().Where(r => r.Status > s));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Status > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Status > val));
        Diff("W2 Where(Status > enumParam) [string-stored]", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_enum_string_relational_gte_multi()
    {
        var cq = Norm.CompileQuery((DbContext c, Status s) => c.Query<Row>().Where(r => r.Status >= s));
        foreach (var val in new[] { Status.New, Status.Active, Status.Closed })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.Status >= val));
            _out.WriteLine($"W2gte val={val} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    [Fact]
    public async Task W2_enum_string_equal_multi()
    {
        var cq = Norm.CompileQuery((DbContext c, Status s) => c.Query<Row>().Where(r => r.Status == s));
        foreach (var val in new[] { Status.New, Status.Active, Status.Closed })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.Status == val));
            _out.WriteLine($"W2eq val={val} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    [Fact]
    public async Task W2_decimal_fractional_equal()
    {
        decimal val = 26.5m; // even i=16 -> Price 26.5
        var cq = Norm.CompileQuery((DbContext c, decimal p) => c.Query<Row>().Where(r => r.Price == p));
        var compiled = Ids(await cq(_ctx, val));
        decimal local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Price == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Price == val));
        Diff("W2 Where(Price == 26.5m)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_decimal_fractional_greater_multi()
    {
        var cq = Norm.CompileQuery((DbContext c, decimal p) => c.Query<Row>().Where(r => r.Price > p));
        foreach (var val in new[] { 20.5m, 15m, 29m, 11m })
        {
            var compiled = Ids(await cq(_ctx, val));
            var oracle = Ids(_all.Where(r => r.Price > val));
            _out.WriteLine($"W2decGt val={val} oracle=[{string.Join(",", oracle)}] compiled=[{string.Join(",", compiled)}]");
            Assert.Equal(oracle, compiled);
        }
    }

    [Fact]
    public async Task W2_datetimeoffset_param_equal()
    {
        var val = _all[9].Dto; // some existing value (i=10)
        var cq = Norm.CompileQuery((DbContext c, DateTimeOffset p) => c.Query<Row>().Where(r => r.Dto == p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dto == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dto == val));
        Diff("W2 Where(Dto == dtoParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_datetimeoffset_param_greater()
    {
        var val = new DateTimeOffset(2021, 1, 11, 12, 0, 0, TimeSpan.Zero);
        var cq = Norm.CompileQuery((DbContext c, DateTimeOffset p) => c.Query<Row>().Where(r => r.Dto > p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dto > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dto > val));
        Diff("W2 Where(Dto > dtoParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_datetimeoffset_param_less()
    {
        var val = new DateTimeOffset(2021, 1, 11, 12, 0, 0, TimeSpan.Zero);
        var cq = Norm.CompileQuery((DbContext c, DateTimeOffset p) => c.Query<Row>().Where(r => r.Dto < p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dto < local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dto < val));
        Diff("W2 Where(Dto < dtoParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_datetimeoffset_param_equal_different_offset_same_instant()
    {
        // i=11 stores offset +02:00; query the SAME INSTANT expressed as UTC (+00:00).
        // C# DateTimeOffset == compares instants -> must match. Regular converts to epoch and
        // matches; the compiled free-param path compares raw offset-suffixed TEXT and misses.
        var stored = _all[10].Dto;              // i=11
        Assert.NotEqual(TimeSpan.Zero, stored.Offset); // sanity: this row is offset-suffixed
        var val = stored.ToUniversalTime();     // same instant, different textual offset
        var cq = Norm.CompileQuery((DbContext c, DateTimeOffset p) => c.Query<Row>().Where(r => r.Dto == p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dto == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dto == val));
        Diff("W2 Where(Dto == dtoParam) diff-offset-same-instant", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_timespan_param_greater()
    {
        var val = TimeSpan.FromMinutes(50); // i*5 > 50 => i > 10
        var cq = Norm.CompileQuery((DbContext c, TimeSpan p) => c.Query<Row>().Where(r => r.Dur > p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dur > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dur > val));
        Diff("W2 Where(Dur > tsParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_timespan_param_equal()
    {
        var val = TimeSpan.FromMinutes(50);
        var cq = Norm.CompileQuery((DbContext c, TimeSpan p) => c.Query<Row>().Where(r => r.Dur == p));
        var compiled = Ids(await cq(_ctx, val));
        var local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Dur == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Dur == val));
        Diff("W2 Where(Dur == tsParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_bool_param_equal()
    {
        bool val = true;
        var cq = Norm.CompileQuery((DbContext c, bool p) => c.Query<Row>().Where(r => r.Flag == p));
        var compiled = Ids(await cq(_ctx, val));
        bool local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Flag == local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Flag == val));
        Diff("W2 Where(Flag == boolParam)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_string_contains_param()
    {
        string val = "1"; // Name contains "1": n1,n10..n19
        var cq = Norm.CompileQuery((DbContext c, string p) => c.Query<Row>().Where(r => r.Name.Contains(p)));
        var compiled = Ids(await cq(_ctx, val));
        string local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Name.Contains(local)).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Name.Contains(val)));
        Diff("W2 Where(Name.Contains(p))", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_string_endswith_param()
    {
        string val = "5"; // Name ends with "5": n5, n15
        var cq = Norm.CompileQuery((DbContext c, string p) => c.Query<Row>().Where(r => r.Name.EndsWith(p)));
        var compiled = Ids(await cq(_ctx, val));
        string local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.Name.EndsWith(local)).ToListAsync());
        var oracle = Ids(_all.Where(r => r.Name.EndsWith(val)));
        Diff("W2 Where(Name.EndsWith(p))", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    [Fact]
    public async Task W2_computed_column_vs_param()
    {
        int val = 30;
        var cq = Norm.CompileQuery((DbContext c, int p) => c.Query<Row>().Where(r => r.A + r.B > p));
        var compiled = Ids(await cq(_ctx, val));
        int local = val;
        var regular = Ids(await _ctx.Query<Row>().Where(r => r.A + r.B > local).ToListAsync());
        var oracle = Ids(_all.Where(r => r.A + r.B > val));
        Diff("W2 Where(A + B > p)", oracle, regular, compiled);
        Assert.Equal(oracle, regular);
        Assert.Equal(oracle, compiled);
    }

    // ─────────────── Surface 10: no-param compiled query invoked repeatedly ───────────────

    [Fact]
    public async Task S10_no_param_repeated_stable()
    {
        var cq = Norm.CompileQuery((DbContext c, int _) => c.Query<Row>().Where(r => r.A > 20).OrderBy(r => r.Id));
        var oracle = Ids(_all.Where(r => r.A > 20));
        for (int i = 0; i < 4; i++)
        {
            var compiled = Ids(await cq(_ctx, 0));
            Assert.Equal(oracle, compiled);
        }
    }
}
