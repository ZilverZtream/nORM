using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests.SgOwnedNavigation;

// ── Entity definitions ────────────────────────────────────────────────────────

/// <summary>
/// Owned type embedded in SgonPerson. Not decorated with [Owned] at the class level;
/// the ownership is configured via fluent OwnsOne.
/// </summary>
internal sealed class SgonAddress
{
    public string Street { get; set; } = string.Empty;
    public string City   { get; set; } = string.Empty;
}

/// <summary>
/// Entity with [GenerateMaterializer]. Fluent OwnsOne configures Address as an
/// inline owned navigation. The compiled materializer cannot reconstruct the
/// Address object from inline columns, so it must be bypassed (SG1 fix).
/// </summary>
[GenerateMaterializer]
[Table("sgon_persons")]
internal sealed class SgonPerson
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public SgonAddress Address { get; set; } = new();
}

/// <summary>
/// Owned type for the [Owned]-attribute-level test.
/// </summary>
[nORM.Mapping.Owned]
internal sealed class SgonAttrAddress
{
    public string Line1 { get; set; } = string.Empty;
    public string Zip   { get; set; } = string.Empty;
}

/// <summary>
/// Entity with [GenerateMaterializer] and an owned navigation declared via the
/// [Owned] attribute on the owned CLR type. Compiled materializer must be bypassed.
/// </summary>
[GenerateMaterializer]
[Table("sgon_customers")]
internal sealed class SgonCustomer
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Name    { get; set; } = string.Empty;
    public SgonAttrAddress Addr { get; set; } = new();
}

/// <summary>
/// Entity with [GenerateMaterializer] and NO owned navigations. The compiled
/// materializer should still be used (control group).
/// </summary>
[GenerateMaterializer]
[Table("sgon_plain")]
internal sealed class SgonPlain
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Verifies that entities with inline owned scalar navigations (OwnsOne / [Owned])
/// fall back from the compiled source-generated materializer to the runtime
/// materializer, which correctly populates the owned navigation object from inlined
/// columns. (SG1 fix in MaterializerFactory.HasOwnedNavigationColumns.)
/// </summary>
public class SourceGenOwnedNavigationTests
{
    private static SqliteConnection CreateOpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void ExecNonQuery(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ── SG1-1: Fluent OwnsOne populates navigation ───────────────────────────

    /// <summary>
    /// Entity with [GenerateMaterializer] + fluent OwnsOne(p => p.Address).
    /// The Address columns are inlined as "Address_Street" / "Address_City".
    /// The runtime materializer must be used to reconstruct the Address object.
    /// </summary>
    [Fact]
    public async Task FluentOwnsOne_OwnedNavPopulated()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgon_persons (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL DEFAULT '', " +
            "Address_Street TEXT NOT NULL DEFAULT '', " +
            "Address_City TEXT NOT NULL DEFAULT '')");

        ExecNonQuery(cn, "INSERT INTO sgon_persons (Name, Address_Street, Address_City) " +
                         "VALUES ('Bob', '123 Main St', 'Springfield')");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgonPerson>()
                  .OwnsOne(p => p.Address)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var persons = await ctx.Query<SgonPerson>().ToListAsync();

        Assert.Single(persons);
        var p = persons[0];
        Assert.Equal("Bob", p.Name);
        Assert.NotNull(p.Address);
        Assert.Equal("123 Main St", p.Address.Street);
        Assert.Equal("Springfield", p.Address.City);
    }

    // ── SG1-2: Multiple rows with owned navigation ────────────────────────────

    /// <summary>
    /// All rows in the result set must have their owned navigation populated.
    /// </summary>
    [Fact]
    public async Task FluentOwnsOne_MultipleRows_AllPopulated()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgon_persons (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL DEFAULT '', " +
            "Address_Street TEXT NOT NULL DEFAULT '', " +
            "Address_City TEXT NOT NULL DEFAULT '')");

        ExecNonQuery(cn, "INSERT INTO sgon_persons (Name, Address_Street, Address_City) VALUES " +
                         "('Alice', '1 Alpha St', 'Atown'), " +
                         "('Bob', '2 Beta Ave', 'Btown'), " +
                         "('Carol', '3 Gamma Rd', 'Ctown')");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgonPerson>()
                  .OwnsOne(p => p.Address)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var persons = await ctx.Query<SgonPerson>().OrderBy(p => p.Name).ToListAsync();

        Assert.Equal(3, persons.Count);
        Assert.All(persons, p => Assert.NotNull(p.Address));
        Assert.Equal("1 Alpha St",  persons[0].Address.Street);
        Assert.Equal("2 Beta Ave",  persons[1].Address.Street);
        Assert.Equal("3 Gamma Rd",  persons[2].Address.Street);
    }

    // ── SG1-3: No owned navigation → compiled materializer still used ─────────

    /// <summary>
    /// Control: entity with [GenerateMaterializer] and no owned navigation.
    /// Compiled materializer is used and query returns correct data.
    /// </summary>
    [Fact]
    public async Task NoOwnedNavigation_CompiledMaterializer_Works()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgon_plain (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL DEFAULT '', Age INTEGER NOT NULL DEFAULT 0)");

        ExecNonQuery(cn, "INSERT INTO sgon_plain (Name, Age) VALUES ('Plain', 30)");

        await using var ctx = new DbContext(cn, new SqliteProvider());

        var items = await ctx.Query<SgonPlain>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("Plain", items[0].Name);
        Assert.Equal(30, items[0].Age);
    }

    // ── SG1-4: WHERE predicate works with owned-nav entity ────────────────────

    /// <summary>
    /// A WHERE filter on a non-owned property must work correctly even when the
    /// runtime materializer is in use for the owned navigation.
    /// </summary>
    [Fact]
    public async Task FluentOwnsOne_WhereFilter_Works()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgon_persons (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL DEFAULT '', " +
            "Address_Street TEXT NOT NULL DEFAULT '', " +
            "Address_City TEXT NOT NULL DEFAULT '')");

        ExecNonQuery(cn, "INSERT INTO sgon_persons (Name, Address_Street, Address_City) VALUES " +
                         "('Alice', '1 A St', 'A-City'), " +
                         "('Bob', '2 B Ave', 'B-City')");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgonPerson>()
                  .OwnsOne(p => p.Address)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var persons = await ctx.Query<SgonPerson>()
                                .Where(p => p.Name == "Alice")
                                .ToListAsync();

        Assert.Single(persons);
        Assert.Equal("1 A St", persons[0].Address.Street);
    }

    // ── SG1-5: Attribute-level [Owned] type also falls back ───────────────────

    /// <summary>
    /// When the owned type carries the [Owned] attribute (type-level declaration),
    /// the same compiled-materializer bypass must apply.
    /// </summary>
    [Fact]
    public async Task AttributeOwned_OwnedNavPopulated()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE sgon_customers (Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "Name TEXT NOT NULL DEFAULT '', " +
            "Addr_Line1 TEXT NOT NULL DEFAULT '', " +
            "Addr_Zip TEXT NOT NULL DEFAULT '')");

        ExecNonQuery(cn, "INSERT INTO sgon_customers (Name, Addr_Line1, Addr_Zip) " +
                         "VALUES ('Dave', '42 Oak Lane', '12345')");

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<SgonCustomer>()
                  .OwnsOne(c => c.Addr)
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var customers = await ctx.Query<SgonCustomer>().ToListAsync();

        Assert.Single(customers);
        Assert.NotNull(customers[0].Addr);
        Assert.Equal("42 Oak Lane", customers[0].Addr.Line1);
        Assert.Equal("12345", customers[0].Addr.Zip);
    }
}
