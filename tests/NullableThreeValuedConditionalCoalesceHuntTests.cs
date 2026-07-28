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
/// Adversarial hunt for silent-wrong 3VL / conditional / null-coalesce translation on SQLite.
/// Oracle = LINQ-to-Objects (same lambda on an in-memory List with NULL rows). A row wrongly
/// kept/dropped, or a wrong projected value, is a silent-wrong bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class NullableThreeValuedConditionalCoalesceHuntTests
{
    private readonly ITestOutputHelper _out;
    public NullableThreeValuedConditionalCoalesceHuntTests(ITestOutputHelper o) => _out = o;

    public enum Status { Pending = 0, Processing = 1, Shipped = 2, Delivered = 3, Cancelled = 4 }

    private sealed class StatusStringConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    [Table("NtvRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        public bool? NBool { get; set; }
        public int? NA { get; set; }
        public int? NB { get; set; }
        public bool Flag { get; set; }
        public string? NName { get; set; }
    }

    // Rows spanning every relevant NULL combination.
    private static readonly Row[] Seed = new[]
    {
        new Row { Id = 1, NBool = true,  NA = 10,   NB = 20,   Flag = true,  NName = "alpha" },
        new Row { Id = 2, NBool = false, NA = null, NB = 20,   Flag = false, NName = null    },
        new Row { Id = 3, NBool = null,  NA = 10,   NB = null, Flag = true,  NName = "gamma" },
        new Row { Id = 4, NBool = null,  NA = null, NB = null, Flag = false, NName = null    },
        new Row { Id = 5, NBool = true,  NA = 3,    NB = 8,    Flag = false, NName = "alpha" },
    };

    private static DbContext Ctx(bool withStatusConverter = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NtvRow (Id INTEGER PRIMARY KEY, NBool INTEGER NULL, NA INTEGER NULL, NB INTEGER NULL, Flag INTEGER NOT NULL, NName TEXT NULL);";
            foreach (var r in Seed)
            {
                string B(bool? b) => b.HasValue ? (b.Value ? "1" : "0") : "NULL";
                string I(int? i) => i.HasValue ? i.Value.ToString() : "NULL";
                string S(string? s) => s == null ? "NULL" : $"'{s}'";
                cmd.CommandText += $"INSERT INTO NtvRow VALUES ({r.Id},{B(r.NBool)},{I(r.NA)},{I(r.NB)},{(r.Flag ? 1 : 0)},{S(r.NName)});";
            }
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    /// <summary>Runs the identical query against nORM (SQLite) and the LINQ-to-Objects oracle and asserts equal.</summary>
    private void OracleEqual<T>(Func<IQueryable<Row>, IEnumerable<T>> q, string? label = null)
    {
        var expected = q(Seed.AsQueryable()).ToList();
        using var ctx = Ctx();
        var norm = ctx.Query<Row>().AsQueryable();
        try
        {
            var sql = ((IQueryable<Row>)norm).ToQueryString();
            _out.WriteLine($"[{label}] SQL:\n{sql}");
        }
        catch { /* ToQueryString only over the raw source; ignore for shaped queries */ }
        var actual = q(norm).ToList();
        _out.WriteLine($"[{label}] expected=[{string.Join(",", expected)}] actual=[{string.Join(",", actual)}]");
        Assert.Equal(expected, actual);
    }

    // ---------------------------------------------------------------------
    // 39A: bool? compared in WHERE vs projected. Oracle = LINQ-to-Objects.
    // ---------------------------------------------------------------------

    [Fact]
    public void Where_nullable_bool_not_equal_true_keeps_null_rows_like_linq()
    {
        // C#: null != true  => TRUE  => row KEPT. Oracle Expected: {2,3,4}.
        OracleEqual(q => q.Where(r => r.NBool != true).OrderBy(r => r.Id).Select(r => r.Id), "where NBool!=true");
    }

    [Fact]
    public void Where_nullable_bool_not_equal_false_keeps_null_rows_like_linq()
    {
        // C#: null != false => TRUE  => row KEPT. Oracle Expected: {1,3,4,5}.
        OracleEqual(q => q.Where(r => r.NBool != false).OrderBy(r => r.Id).Select(r => r.Id), "where NBool!=false");
    }

    [Fact]
    public void Project_nullable_bool_not_equal_true_matches_linq()
    {
        // Projection is the control: should already be null-safe (SCV expands).
        OracleEqual(q => q.OrderBy(r => r.Id).Select(r => r.NBool != true), "select NBool!=true");
    }

    [Fact]
    public void Where_nullable_bool_equal_true_matches_linq()
    {
        // C#: null == true => false => excluded. Expected {1,5}. (Clean-bill control.)
        OracleEqual(q => q.Where(r => r.NBool == true).OrderBy(r => r.Id).Select(r => r.Id), "where NBool==true");
    }

    [Fact]
    public void Where_nullable_bool_equal_false_matches_linq()
    {
        // C#: null == false => false => excluded. Expected {2}. (Clean-bill control.)
        OracleEqual(q => q.Where(r => r.NBool == false).OrderBy(r => r.Id).Select(r => r.Id), "where NBool==false");
    }

    // ---------------------------------------------------------------------
    // Conditional ?:  (projection + where) with null branches / null tests.
    // ---------------------------------------------------------------------

    [Fact]
    public void Ternary_projection_nullable_branches()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => r.Flag ? r.NA : r.NB), "ternary NA:NB");

    [Fact]
    public void Ternary_condition_nullable_int_comparison()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => (r.NA > 5) ? 100 : 200), "ternary NA>5");

    [Fact]
    public void Ternary_condition_coalesced_nullable_bool()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => (r.NBool ?? false) ? 1 : 0), "ternary (NBool??false)");

    [Fact]
    public void Ternary_in_where_with_nullable_branches()
        => OracleEqual(q => q.Where(r => (r.Flag ? r.NA : r.NB) > 5).OrderBy(r => r.Id).Select(r => r.Id), "where (Flag?NA:NB)>5");

    [Fact]
    public void Ternary_condition_nullable_equality()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => (r.NA == 10) ? 1 : 0), "ternary NA==10");

    // ---------------------------------------------------------------------
    // Null-coalescing ??  chains / feeding operators / ordering / aggregate.
    // ---------------------------------------------------------------------

    [Fact]
    public void Coalesce_chain_two_nullable_columns()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => r.NA ?? r.NB ?? -1), "NA??NB??-1");

    [Fact]
    public void Coalesce_feeding_arithmetic()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => (r.NA ?? 0) + 5), "(NA??0)+5");

    [Fact]
    public void Coalesce_feeding_comparison_in_where()
        => OracleEqual(q => q.Where(r => (r.NA ?? r.NB ?? 0) > 5).OrderBy(r => r.Id).Select(r => r.Id), "where (NA??NB??0)>5");

    [Fact]
    public void Coalesce_order_by_with_maxvalue_fallback()
        => OracleEqual(q => q.OrderBy(r => r.NA ?? int.MaxValue).ThenBy(r => r.Id).Select(r => r.Id), "orderby NA??max");

    [Fact]
    public void Coalesce_in_sum_aggregate()
    {
        var expected = Seed.AsQueryable().Sum(r => r.NA ?? 0);
        using var ctx = Ctx();
        var actual = ctx.Query<Row>().Sum(r => r.NA ?? 0);
        _out.WriteLine($"[sum NA??0] expected={expected} actual={actual}");
        Assert.Equal(expected, actual);
    }

    // ---------------------------------------------------------------------
    // Nullable comparison 3VL between two nullable columns.
    // ---------------------------------------------------------------------

    [Fact]
    public void Where_two_nullable_columns_not_equal()
        => OracleEqual(q => q.Where(r => r.NA != r.NB).OrderBy(r => r.Id).Select(r => r.Id), "where NA!=NB");

    [Fact]
    public void Where_two_nullable_columns_equal()
        => OracleEqual(q => q.Where(r => r.NA == r.NB).OrderBy(r => r.Id).Select(r => r.Id), "where NA==NB");

    [Fact]
    public void Project_two_nullable_columns_not_equal()
        => OracleEqual(q => q.OrderBy(r => r.Id).Select(r => r.NA != r.NB), "select NA!=NB");

    // ---------------------------------------------------------------------
    // Coalesce over a converter (enum-as-string) column: does the fallback bind right?
    // ---------------------------------------------------------------------

    [Table("NtvOrder")]
    public sealed class Ord
    {
        [Key] public int Id { get; set; }
        public Status? Status { get; set; }
    }

    private static DbContext OrderCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NtvOrder (Id INTEGER PRIMARY KEY, Status TEXT NULL);" +
                "INSERT INTO NtvOrder (Id, Status) VALUES (1,'Shipped'),(2,NULL),(3,'Cancelled');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Ord>().Property(o => o.Status).HasConversion(new StatusStringConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Coalesce_over_enum_string_converter_column_projection_is_fail_loud()
    {
        // CLEAN BILL: `col ?? fallback` over an enum-as-string value-converter column in a
        // projection cannot be translated (SQL yields the stored name; ConvertFromProvider can't
        // apply to the COALESCE result). nORM correctly FAILS LOUD instead of returning a wrong
        // value — no silent-wrongness here.
        using var ctx = OrderCtx();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Ord>().OrderBy(o => o.Id).Select(o => o.Status ?? Status.Pending).ToList());
    }

    // ---------------------------------------------------------------------
    // Root-cause trace: emitted SQL for the two confirmed silent-wrong WHERE cases.
    // ---------------------------------------------------------------------

    [Fact]
    public void Trace_where_nullable_bool_not_equal_emits_no_null_rescue()
    {
        using var ctx = Ctx();
        var sqlNe = ctx.Query<Row>().Where(r => r.NBool != true).Select(r => r.Id).ToQueryString();
        var sqlEq = ctx.Query<Row>().Where(r => r.NBool == true).Select(r => r.Id).ToQueryString();
        var sqlIntNe = ctx.Query<Row>().Where(r => r.NA != 10).Select(r => r.Id).ToQueryString();
        _out.WriteLine("bool? != true :\n" + sqlNe);
        _out.WriteLine("bool? == true :\n" + sqlEq);
        _out.WriteLine("int?  != 10   :\n" + sqlIntNe);
        // The bug: `!= true` emits a bare `<> 1` with NO `OR ... IS NULL` rescue, unlike the
        // int? path which DOES emit the rescue. This documents the divergence.
    }
}
