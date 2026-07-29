using System;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ---------------------------------------------------------------------------
// TPH inheritance + value-converter materialization.
// Model: TphAcct (base, [DiscriminatorColumn]) + TphSavings / TphChecking subtypes.
// ---------------------------------------------------------------------------

[DiscriminatorColumn(nameof(Kind))]
[Trait("Category", TestCategory.Fast)]
public class TphAcct
{
    [Key] public int Id { get; set; }
    public int Kind { get; set; }
    public string Owner { get; set; } = "";
}

[DiscriminatorValue(1)]
[Trait("Category", TestCategory.Fast)]
public class TphSavings : TphAcct
{
    public int Balance { get; set; }   // value-converted (+1000/-1000)
    public string Note { get; set; } = "";
}

[DiscriminatorValue(2)]
[Trait("Category", TestCategory.Fast)]
public class TphChecking : TphAcct
{
    public int Overdraft { get; set; }
}

[Trait("Category", TestCategory.Fast)]
public class TphSubtypeConverterMaterializationTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object ConvertToProvider(int value) => value + 1000;   // model -> stored (+1000)
        public override object ConvertFromProvider(int value) => value - 1000; // stored -> model (-1000)
    }

    private sealed class TagConverter : ValueConverter<string, string>
    {
        public override object ConvertToProvider(string value) => "S:" + value;
        public override object ConvertFromProvider(string value) =>
            value.StartsWith("S:") ? value.Substring(2) : value;
    }

    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE TphAcct(Id INTEGER PRIMARY KEY, Kind INTEGER, Owner TEXT, " +
            "Balance INTEGER, Note TEXT, Overdraft INTEGER);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContextOptions Opts() => new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<TphSavings>().Property(s => s.Balance).HasConversion(new OffsetConverter());
            mb.Entity<TphSavings>().Property(s => s.Note).HasConversion(new TagConverter());
        }
    };

    // -----------------------------------------------------------------------
    // SURFACE 1/5: converter on a SUBTYPE column is dropped when a derived row
    // materializes through the polymorphic base query path.
    // -----------------------------------------------------------------------
    [Fact]
    public void Polymorphic_query_of_derived_row_applies_subtype_value_converter()
    {
        using var cn = NewDb();
        using (var cmd = cn.CreateCommand())
        {
            // Seed a SAVINGS row with the STORED (provider) representation:
            //   Balance stored 1500 -> model 500 ;  Note stored "S:hello" -> model "hello".
            cmd.CommandText =
                "INSERT INTO TphAcct(Id,Kind,Owner,Balance,Note,Overdraft) " +
                "VALUES (1, 1, 'Ann', 1500, 'S:hello', NULL);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), Opts());

        // Contrast: query the leaf subtype directly (normal materializer path) -> converter applied.
        var direct = ctx.Query<TphSavings>().AsNoTracking().Single();
        Assert.Equal(500, direct.Balance);
        Assert.Equal("hello", direct.Note);

        // Polymorphic query of the base: the row must still materialize as TphSavings
        // WITH its converter applied.
        var poly = ((INormQueryable<TphAcct>)ctx.Query<TphAcct>()).AsNoTracking().Single();
        var sav = Assert.IsType<TphSavings>(poly);
        Assert.Equal(500, sav.Balance);      // EXPECTED 500 (converter). BUG: 1500 (raw stored)
        Assert.Equal("hello", sav.Note);     // EXPECTED "hello". BUG: "S:hello"
    }

    // -----------------------------------------------------------------------
    // SURFACE 4: OfType<Derived>() — does the subtype filter also drop the converter?
    // -----------------------------------------------------------------------
    [Fact]
    public void OfType_derived_applies_subtype_value_converter()
    {
        using var cn = NewDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO TphAcct(Id,Kind,Owner,Balance,Note,Overdraft) VALUES (1,1,'Ann',1500,'S:hi',NULL);" + // savings
                "INSERT INTO TphAcct(Id,Kind,Owner,Balance,Note,Overdraft) VALUES (2,2,'Bob',NULL,NULL,5);";        // checking
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), Opts());

        var savings = ((INormQueryable<TphAcct>)ctx.Query<TphAcct>())
            .AsNoTracking().OfType<TphSavings>().Single();
        Assert.Equal(500, savings.Balance);   // converter expected; documents whether OfType shares the defect
        Assert.Equal("hi", savings.Note);
    }

    // -----------------------------------------------------------------------
    // SURFACE 3: sibling-column leakage on WRITE. Saving a TphSavings must not
    // populate TphChecking's Overdraft column, and vice-versa.
    // -----------------------------------------------------------------------
    [Fact]
    public async System.Threading.Tasks.Task Saving_subtype_leaves_sibling_columns_null()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), Opts());

        ctx.Add(new TphSavings { Id = 1, Owner = "Ann", Balance = 500, Note = "hi" });
        ctx.Add(new TphChecking { Id = 2, Owner = "Bob", Overdraft = 100 });
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Kind,Balance,Note,Overdraft FROM TphAcct ORDER BY Id";
        using var r = cmd.ExecuteReader();
        // Savings row: Overdraft must be NULL (sibling col not written with A's data).
        r.Read();
        Assert.Equal(1L, r.GetInt64(0));          // Kind = savings
        Assert.Equal(1500L, r.GetInt64(1));       // Balance stored (500 + 1000 converter)
        Assert.Equal("S:hi", r.GetString(2));     // Note stored (converter)
        Assert.True(r.IsDBNull(3), "Savings row leaked TphChecking.Overdraft");
        // Checking row: Balance/Note must be NULL.
        r.Read();
        Assert.Equal(2L, r.GetInt64(0));          // Kind = checking
        Assert.True(r.IsDBNull(1), "Checking row leaked TphSavings.Balance");
        Assert.True(r.IsDBNull(2), "Checking row leaked TphSavings.Note");
        Assert.Equal(100L, r.GetInt64(3));        // Overdraft
    }
}

// ---------------------------------------------------------------------------
// SURFACE 1 (3-level hierarchy): derived-type discovery only matches direct
// children (tp.BaseType == baseType), so a grandchild subtype is never
// registered in the ROOT's TphMappings. A grandchild row read through the
// polymorphic base query silently materializes as the base type.
// ---------------------------------------------------------------------------

[DiscriminatorColumn(nameof(AKind))]
[Trait("Category", TestCategory.Fast)]
public class TphAsset
{
    [Key] public int Id { get; set; }
    public int AKind { get; set; }
    public string Owner { get; set; } = "";
}

[DiscriminatorValue(1)]
[Trait("Category", TestCategory.Fast)]
public class TphEquity : TphAsset
{
    public int Shares { get; set; }
}

[DiscriminatorValue(2)]
[Trait("Category", TestCategory.Fast)]
public class TphRestrictedEquity : TphEquity
{
    public int LockupDays { get; set; }
}

[Trait("Category", TestCategory.Fast)]
public class TphThreeLevelHierarchyTests
{
    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE TphAsset(Id INTEGER PRIMARY KEY, AKind INTEGER, Owner TEXT, " +
            "Shares INTEGER, LockupDays INTEGER);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    // FAIL-LOUD GAP (not a silent-wrong): a 3-level hierarchy's grandchild is never registered in
    // the ROOT's TphMappings (discovery matches only tp.BaseType == baseType = direct children), so a
    // grandchild row falls to the base materializer. Because WithTphBaseSafeGetter wraps only the GETTER,
    // the merged sibling column's SETTER still hard-casts to its declaring type, and setting the
    // grandchild's non-null inherited column (Shares, declared on TphEquity) onto a base TphAsset instance
    // throws InvalidCastException. It CRASHES (fail-loud) rather than silently corrupting — documented here.
    [Fact]
    public void ThreeLevel_grandchild_row_currently_fails_loud_not_silent()
    {
        using var cn = NewDb();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO TphAsset(Id,AKind,Owner,Shares,LockupDays) VALUES (1,1,'Ann',10,NULL);" +   // TphEquity
                "INSERT INTO TphAsset(Id,AKind,Owner,Shares,LockupDays) VALUES (2,2,'Bob',20,90);";       // TphRestrictedEquity
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Ideal (EF parity) would materialize row 2 as TphRestrictedEquity. nORM does NOT support 3-level
        // TPH: the grandchild row currently throws during materialization. Assert fail-loud (no silent corruption).
        Assert.ThrowsAny<Exception>(() =>
            ((INormQueryable<TphAsset>)ctx.Query<TphAsset>()).AsNoTracking().OrderBy(a => a.Id).ToList());
    }
}
