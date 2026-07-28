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

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Adversarial hunt: Contains / IN-list / correlated Any-All-subquery translation on SQLite with
/// NULLable operands and value-converter (enum-as-string / decimal-as-TEXT) columns. Every case is
/// checked against a LINQ-to-Objects oracle (the SAME lambda over in-memory Lists with the SAME NULL
/// data), NOT against SQL 3VL. A divergence that keeps/drops the wrong rows without throwing is the
/// worst-class silent-wrong bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SubqueryContainsNullConverterHuntTests
{
    public enum Kind { Active, Pending, Closed }

    private sealed class KindToStringConverter : ValueConverter<Kind, string>
    {
        public override object? ConvertToProvider(Kind v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Kind>(v);
    }

    private sealed class NKindToStringConverter : ValueConverter<Kind?, string?>
    {
        public override object? ConvertToProvider(Kind? v) => v?.ToString();
        public override object? ConvertFromProvider(string? v) => v == null ? (Kind?)null : Enum.Parse<Kind>(v);
    }

    [Table("HuntNi")]
    public class NiRow { [Key] public int Id { get; set; } public int? NInt { get; set; } }

    [Table("HuntDec")]
    public class DecRow { [Key] public int Id { get; set; } public decimal Cost { get; set; } }

    public readonly record struct Money(decimal Amount);

    private sealed class MoneyConverter : ValueConverter<Money, decimal>
    {
        public override object ConvertToProvider(Money value) => value.Amount;
        public override object ConvertFromProvider(decimal value) => new Money(value);
    }

    [Table("HuntMoney")]
    public class MoneyRow { [Key] public int Id { get; set; } public Money Cost { get; set; } }

    [Table("HuntEnum")]
    public class EnumRow { [Key] public int Id { get; set; } public Kind Kind { get; set; } }

    // --- Parent / child for correlated Any/All/aggregate/subquery-Contains ---
    [Table("HuntParent")]
    public class Parent { [Key] public int Id { get; set; } public int Y { get; set; } }

    [Table("HuntChild")]
    public class Child { [Key] public int Id { get; set; } public int PId { get; set; } public int? X { get; set; } }

    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    // =====================================================================================
    // Item 1 — list.Contains(nullableColumn) with NULLs in the data (clean-bill expectation)
    // =====================================================================================
    [Fact]
    public async Task NullableIn_list_with_null_matches_the_null_row()
    {
        using var cn = Open("CREATE TABLE HuntNi (Id INTEGER PRIMARY KEY, NInt INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] { new NiRow { Id = 1, NInt = 1 }, new NiRow { Id = 2, NInt = 2 },
                           new NiRow { Id = 3, NInt = null }, new NiRow { Id = 4, NInt = 4 } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<int?> { 2, null };
        var expected = data.Where(r => list.Contains(r.NInt)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<NiRow>().Where(r => list.Contains(r.NInt)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET keeps NInt==null (list has null); id 2 and id 3
    }

    [Fact]
    public async Task Empty_in_list_matches_nothing()
    {
        using var cn = Open("CREATE TABLE HuntNi (Id INTEGER PRIMARY KEY, NInt INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] { new NiRow { Id = 1, NInt = 1 }, new NiRow { Id = 2, NInt = null } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<int?>();
        var expected = data.Where(r => list.Contains(r.NInt)).Select(r => r.Id).ToList(); // empty
        var actual = ctx.Query<NiRow>().Where(r => list.Contains(r.NInt)).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }

    // =====================================================================================
    // Item 3 — !list.Contains(nullableColumn) with NULLs (clean-bill expectation)
    // =====================================================================================
    [Fact]
    public async Task NotContains_null_column_no_null_in_list_keeps_null_row()
    {
        using var cn = Open("CREATE TABLE HuntNi (Id INTEGER PRIMARY KEY, NInt INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] { new NiRow { Id = 1, NInt = 1 }, new NiRow { Id = 2, NInt = 2 },
                           new NiRow { Id = 3, NInt = null }, new NiRow { Id = 4, NInt = 4 } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<int?> { 2 };
        var expected = data.Where(r => !list.Contains(r.NInt)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<NiRow>().Where(r => !list.Contains(r.NInt)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET keeps null row: [1,3,4]
    }

    [Fact]
    public async Task NotContains_null_in_list_drops_the_null_row()
    {
        using var cn = Open("CREATE TABLE HuntNi (Id INTEGER PRIMARY KEY, NInt INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] { new NiRow { Id = 1, NInt = 1 }, new NiRow { Id = 2, NInt = 2 },
                           new NiRow { Id = 3, NInt = null }, new NiRow { Id = 4, NInt = 4 } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<int?> { 2, null };
        var expected = data.Where(r => !list.Contains(r.NInt)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<NiRow>().Where(r => !list.Contains(r.NInt)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // null-row now dropped (list has null): [1,4]
    }

    // =====================================================================================
    // Item 2 — list.Contains(decimalColumn): decimal stored as TEXT, scale-insensitive .NET equality
    //          The scalar `==` path canonicalizes (ExactDecimalKeySql); the IN-list path does not.
    // =====================================================================================
    [Fact]
    public async Task NativeDecimal_in_list_is_scale_insensitive()
    {
        using var cn = Open("CREATE TABLE HuntDec (Id INTEGER PRIMARY KEY, Cost TEXT NOT NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        // Stored with scale 1 (nORM binds decimal -> canonical TEXT "10.5", "20.5").
        var data = new[] { new DecRow { Id = 1, Cost = 10.5m }, new DecRow { Id = 2, Cost = 20.5m },
                           new DecRow { Id = 3, Cost = 30.0m } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        // List holds the SAME numeric values at a DIFFERENT scale (10.50 == 10.5 in .NET decimal).
        var list = new List<decimal> { 10.50m, 30m };
        var expected = data.Where(r => list.Contains(r.Cost)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<DecRow>().Where(r => list.Contains(r.Cost)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [1,3]. A raw TEXT IN ('10.50','30') misses stored '10.5'/'30.0'.
    }

    [Fact]
    public async Task ConverterDecimal_in_list_is_scale_insensitive()
    {
        using var cn = Open("CREATE TABLE HuntMoney (Id INTEGER PRIMARY KEY, Cost TEXT NOT NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<MoneyRow>().Property(e => e.Cost).HasConversion(new MoneyConverter())
        });
        var data = new[] { new MoneyRow { Id = 1, Cost = new Money(10.5m) }, new MoneyRow { Id = 2, Cost = new Money(20.5m) },
                           new MoneyRow { Id = 3, Cost = new Money(30.0m) } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<Money> { new Money(20.500m), new Money(30m) };
        var expected = data.Where(r => list.Contains(r.Cost)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<MoneyRow>().Where(r => list.Contains(r.Cost)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [2,3].
    }

    [Table("HuntTemporal")]
    public class TempRow { [Key] public int Id { get; set; } public DateTime Dt { get; set; } public TimeOnly T { get; set; } }

    // Round-trip consistency: value written by nORM, matched by IN-list of the same value.
    [Fact]
    public async Task DateTime_in_list_roundtrip_subsecond()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] {
            new TempRow { Id = 1, Dt = new DateTime(2020,1,1,12,0,0), T = new TimeOnly(12,0,0) },
            new TempRow { Id = 2, Dt = new DateTime(2020,1,1,12,0,0,500), T = new TimeOnly(12,0,0,500) } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<DateTime> { new DateTime(2020,1,1,12,0,0,500) };
        var expected = data.Where(r => list.Contains(r.Dt)).Select(r => r.Id).ToList();
        var actual = ctx.Query<TempRow>().Where(r => list.Contains(r.Dt)).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task TimeOnly_in_list_roundtrip_subsecond()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var data = new[] {
            new TempRow { Id = 1, Dt = new DateTime(2020,1,1,12,0,0), T = new TimeOnly(12,0,0) },
            new TempRow { Id = 2, Dt = new DateTime(2020,1,1,12,0,0,500), T = new TimeOnly(12,0,0,500) } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<TimeOnly> { new TimeOnly(12,0,0,500) };
        var expected = data.Where(r => list.Contains(r.T)).Select(r => r.Id).ToList();
        var actual = ctx.Query<TempRow>().Where(r => list.Contains(r.T)).Select(r => r.Id).ToList();
        Assert.Equal(expected, actual);
    }

    // Scale-mismatch scenario the scalar `==` canonical path exists for: a stored whole-second value
    // (written by raw SQL / another tool) vs a nORM-bound value whose text carries a fractional part.
    // The scalar `==` path canonicalizes both and matches; the IN-list path must not diverge from it.
    [Fact]
    public void DateTime_scalar_eq_and_in_list_agree_on_wholesecond_stored_value()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntTemporal VALUES (1,'2020-01-01 12:00:00','12:00:00'),(2,'2020-01-01 09:00:00','09:00:00');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = new DateTime(2020, 1, 1, 12, 0, 0);
        var list = new List<DateTime> { target };
        var scalar = ctx.Query<TempRow>().Where(r => r.Dt == target).Select(r => r.Id).ToList();
        var inList = ctx.Query<TempRow>().Where(r => list.Contains(r.Dt)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 1 }, scalar.ToArray());   // scalar == canonicalizes and matches
        Assert.Equal(scalar, inList);                    // IN-list must agree, not silently drop row 1
    }

    [Fact]
    public void TimeOnly_scalar_eq_and_in_list_agree_on_wholesecond_stored_value()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntTemporal VALUES (1,'2020-01-01 12:00:00','12:00:00'),(2,'2020-01-01 09:00:00','09:00:00');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = new TimeOnly(12, 0, 0);
        var list = new List<TimeOnly> { target };
        var scalar = ctx.Query<TempRow>().Where(r => r.T == target).Select(r => r.Id).ToList();
        var inList = ctx.Query<TempRow>().Where(r => list.Contains(r.T)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 1 }, scalar.ToArray());
        Assert.Equal(scalar, inList);
    }

    // Externally-written data at millisecond precision ("12:00:00.500" — the common EF Core / ISO8601
    // format). The scalar `==` path canonicalizes trailing fraction zeros and matches; the IN-list path
    // must not silently drop the row.
    [Fact]
    public void TimeOnly_millisecond_stored_scalar_matches_but_in_list_must_agree()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntTemporal VALUES (1,'2020-01-01 12:00:00.500','12:00:00.500');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = new TimeOnly(12, 0, 0, 500);
        var list = new List<TimeOnly> { target };
        var scalar = ctx.Query<TempRow>().Where(r => r.T == target).Select(r => r.Id).ToList();
        var inList = ctx.Query<TempRow>().Where(r => list.Contains(r.T)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 1 }, scalar.ToArray());   // canonical `==` matches the .5 == .500 value
        Assert.Equal(scalar, inList);                    // IN-list must not silently drop row 1
    }

    [Fact]
    public void DateTime_millisecond_stored_scalar_matches_but_in_list_must_agree()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntTemporal VALUES (1,'2020-01-01 12:00:00.500','12:00:00.500');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = new DateTime(2020, 1, 1, 12, 0, 0, 500);
        var list = new List<DateTime> { target };
        var scalar = ctx.Query<TempRow>().Where(r => r.Dt == target).Select(r => r.Id).ToList();
        var inList = ctx.Query<TempRow>().Where(r => list.Contains(r.Dt)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 1 }, scalar.ToArray());
        Assert.Equal(scalar, inList);
    }

    // Externally-written decimal TEXT at a different scale ("10.50"): scalar `==` canonicalizes and
    // matches; the IN-list path must not silently drop the row.
    [Fact]
    public void ExternalDecimal_trailing_zero_scalar_matches_but_in_list_must_agree()
    {
        using var cn = Open("CREATE TABLE HuntDec (Id INTEGER PRIMARY KEY, Cost TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntDec VALUES (1,'10.50'),(2,'99.0');"; // 10.50 written by another tool
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = 10.5m;
        var list = new List<decimal> { target };
        var scalar = ctx.Query<DecRow>().Where(r => r.Cost == target).Select(r => r.Id).ToList();
        var inList = ctx.Query<DecRow>().Where(r => list.Contains(r.Cost)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 1 }, scalar.ToArray());   // 10.5 == 10.50 canonically
        Assert.Equal(scalar, inList);                    // IN-list must agree
    }

    // The negated form is wrong in the mirror direction: it silently KEEPS a row it should drop.
    [Fact]
    public void TimeOnly_millisecond_stored_not_contains_must_drop_matching_row()
    {
        using var cn = Open("CREATE TABLE HuntTemporal (Id INTEGER PRIMARY KEY, Dt TEXT NOT NULL, T TEXT NOT NULL);");
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO HuntTemporal VALUES (1,'2020-01-01 12:00:00.500','12:00:00.500'),(2,'2020-01-01 09:00:00.000','09:00:00.000');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var target = new TimeOnly(12, 0, 0, 500);
        var list = new List<TimeOnly> { target };
        // .NET: row 1's T (12:00:00.5) IS in the list -> !Contains is false -> only row 2 kept.
        var scalar = ctx.Query<TempRow>().Where(r => r.T != target).Select(r => r.Id).ToList();
        var inList = ctx.Query<TempRow>().Where(r => !list.Contains(r.T)).Select(r => r.Id).ToList();
        Assert.Equal(new[] { 2 }, scalar.ToArray());   // canonical != keeps only row 2
        Assert.Equal(scalar, inList);                    // !Contains must also drop row 1
    }

    // =====================================================================================
    // Item 2 — list.Contains(enumStoredAsString): binds provider (string) values (clean-bill on SQLite)
    // =====================================================================================
    [Fact]
    public async Task EnumString_in_list_binds_provider_names()
    {
        using var cn = Open("CREATE TABLE HuntEnum (Id INTEGER PRIMARY KEY, Kind TEXT NOT NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<EnumRow>().Property(e => e.Kind).HasConversion(new KindToStringConverter())
        });
        var data = new[] { new EnumRow { Id = 1, Kind = Kind.Active }, new EnumRow { Id = 2, Kind = Kind.Pending },
                           new EnumRow { Id = 3, Kind = Kind.Closed }, new EnumRow { Id = 4, Kind = Kind.Active } };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        var list = new List<Kind> { Kind.Active, Kind.Closed };
        var expected = data.Where(r => list.Contains(r.Kind)).Select(r => r.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<EnumRow>().Where(r => list.Contains(r.Kind)).Select(r => r.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // [1,3,4]
    }

    // =====================================================================================
    // Item 4 — correlated All over a nullable comparison + empty set
    // =====================================================================================
    [Fact]
    public async Task Correlated_All_with_nullable_and_empty_set()
    {
        using var cn = Open(
            "CREATE TABLE HuntParent (Id INTEGER PRIMARY KEY, Y INTEGER NOT NULL);" +
            "CREATE TABLE HuntChild (Id INTEGER PRIMARY KEY, PId INTEGER NOT NULL, X INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parents = new[] { new Parent { Id = 1, Y = 5 }, new Parent { Id = 2, Y = 5 }, new Parent { Id = 3, Y = 5 } };
        var children = new[]
        {
            new Child { Id = 1, PId = 1, X = 10 }, new Child { Id = 2, PId = 1, X = 20 },   // all > 5 -> All true
            new Child { Id = 3, PId = 2, X = 10 }, new Child { Id = 4, PId = 2, X = null },  // has null -> null>5 false -> All false
            // parent 3 has NO children -> All is vacuously TRUE
        };
        foreach (var p in parents) ctx.Add(p);
        foreach (var c in children) ctx.Add(c);
        await ctx.SaveChangesAsync();

        var childList = children.ToList();
        var expected = parents.Where(p => childList.Where(c => c.PId == p.Id).All(c => c.X > p.Y))
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.PId == p.Id).All(c => c.X > p.Y))
            .Select(p => p.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [1,3] (2 fails on null child, 3 vacuously true)
    }

    // =====================================================================================
    // Item 4 — correlated Any (EXISTS) with a nullable inner comparison
    // =====================================================================================
    [Fact]
    public async Task Correlated_Any_with_nullable_inner_comparison()
    {
        using var cn = Open(
            "CREATE TABLE HuntParent (Id INTEGER PRIMARY KEY, Y INTEGER NOT NULL);" +
            "CREATE TABLE HuntChild (Id INTEGER PRIMARY KEY, PId INTEGER NOT NULL, X INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parents = new[] { new Parent { Id = 1, Y = 5 }, new Parent { Id = 2, Y = 5 }, new Parent { Id = 3, Y = 5 } };
        var children = new[]
        {
            new Child { Id = 1, PId = 1, X = 10 },
            new Child { Id = 2, PId = 2, X = null },  // only null child -> Any(X>5) false
            // parent 3: no children -> Any false
        };
        foreach (var p in parents) ctx.Add(p);
        foreach (var c in children) ctx.Add(c);
        await ctx.SaveChangesAsync();

        var childList = children.ToList();
        var expected = parents.Where(p => childList.Where(c => c.PId == p.Id).Any(c => c.X > p.Y))
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.PId == p.Id).Any(c => c.X > p.Y))
            .Select(p => p.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [1]
    }

    // =====================================================================================
    // Item 5 — correlated aggregate subquery in predicate (Sum over empty -> 0)
    // =====================================================================================
    [Fact]
    public async Task Correlated_Sum_subquery_empty_is_zero()
    {
        using var cn = Open(
            "CREATE TABLE HuntParent (Id INTEGER PRIMARY KEY, Y INTEGER NOT NULL);" +
            "CREATE TABLE HuntChild (Id INTEGER PRIMARY KEY, PId INTEGER NOT NULL, X INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parents = new[] { new Parent { Id = 1, Y = 0 }, new Parent { Id = 2, Y = 0 } };
        var children = new[] { new Child { Id = 1, PId = 1, X = 3 }, new Child { Id = 2, PId = 1, X = 4 } };
        foreach (var p in parents) ctx.Add(p);
        foreach (var c in children) ctx.Add(c);
        await ctx.SaveChangesAsync();

        var childList = children.ToList();
        // Parent 2 has no children -> Sum == 0 -> matches "<= 0"
        var expected = parents.Where(p => childList.Where(c => c.PId == p.Id).Sum(c => c.X ?? 0) <= 0)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.PId == p.Id).Sum(c => c.X ?? 0) <= 0)
            .Select(p => p.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [2]
    }

    // =====================================================================================
    // Item 6 — subquery Contains (IN subquery) with nullable projected values
    // =====================================================================================
    [Fact]
    public async Task Subquery_Contains_over_nullable_projection()
    {
        using var cn = Open(
            "CREATE TABLE HuntParent (Id INTEGER PRIMARY KEY, Y INTEGER NOT NULL);" +
            "CREATE TABLE HuntChild (Id INTEGER PRIMARY KEY, PId INTEGER NOT NULL, X INTEGER NULL);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        // Parents keyed by Id 1..4; child.X projects the set { 1, null, 3 }.
        var parents = new[] { new Parent { Id = 1, Y = 0 }, new Parent { Id = 2, Y = 0 },
                             new Parent { Id = 3, Y = 0 }, new Parent { Id = 4, Y = 0 } };
        var children = new[] { new Child { Id = 1, PId = 0, X = 1 }, new Child { Id = 2, PId = 0, X = null },
                              new Child { Id = 3, PId = 0, X = 3 } };
        foreach (var p in parents) ctx.Add(p);
        foreach (var c in children) ctx.Add(c);
        await ctx.SaveChangesAsync();

        var childList = children.ToList();
        var expected = parents.Where(p => childList.Select(c => c.X).Contains(p.Id))
            .Select(p => p.Id).OrderBy(i => i).ToList();
        var actual = ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Select(c => c.X).Contains(p.Id))
            .Select(p => p.Id).ToList().OrderBy(i => i).ToList();
        Assert.Equal(expected, actual); // .NET: [1,3]
    }
}
