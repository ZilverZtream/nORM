using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Explicit (Entry(e).Collection(...).Load()) and lazy collection loads build their own SQL, so — like the
/// eager Include loader — they must re-emit the global (soft-delete) filter and the tenant predicate. They
/// did not, so a soft-deleted or another tenant's child leaked into the loaded collection while Include
/// correctly excluded it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ExplicitCollectionLoadGlobalFilterTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("EclOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("EclLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public bool IsDeleted { get; set; }
        public Order? Order { get; set; }
    }

    [Fact]
    public void Explicit_collection_load_applies_the_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EclOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EclLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                "INSERT INTO EclOrder VALUES (1);" +
                "INSERT INTO EclLine VALUES (1, 1, 0), (2, 1, 1);"; // line 2 soft-deleted
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne(l => l.Order).HasForeignKey(l => l.OrderId, o => o.Id)
        };
        opts.AddGlobalFilter<Line>(l => !l.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var order = ctx.Query<Order>().First();
        ctx.Entry(order).Collection("Lines").Load();

        // Only the non-deleted line, matching Include.
        Assert.Equal(new[] { 1 }, order.Lines.Select(l => l.Id).OrderBy(i => i).ToArray());
    }

    [Table("EctLine")]
    public class TLine
    {
        [Key] public int Id { get; set; }
        public int TOrderId { get; set; }
        public string TenantKey { get; set; } = "";
        public TOrder? TOrder { get; set; }
    }

    [Table("EctOrder")]
    public class TOrder
    {
        [Key] public int Id { get; set; }
        public string TenantKey { get; set; } = "";
        public List<TLine> Lines { get; set; } = new();
    }

    [Fact]
    public void Explicit_collection_load_keeps_the_tenant_boundary()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EctOrder (Id INTEGER PRIMARY KEY, TenantKey TEXT NOT NULL);" +
                "CREATE TABLE EctLine (Id INTEGER PRIMARY KEY, TOrderId INTEGER NOT NULL, TenantKey TEXT NOT NULL);" +
                "INSERT INTO EctOrder VALUES (1, 'T1');" +
                // line 3 belongs to tenant T2 but shares the FK — must not leak.
                "INSERT INTO EctLine VALUES (1, 1, 'T1'), (2, 1, 'T1'), (3, 1, 'T2');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1"), TenantColumnName = "TenantKey" };
        opts.OnModelCreating = mb =>
            mb.Entity<TOrder>().HasMany(o => o.Lines).WithOne(l => l.TOrder).HasForeignKey(l => l.TOrderId, o => o.Id);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var order = ctx.Query<TOrder>().First();
        ctx.Entry(order).Collection("Lines").Load();

        // T2's line (3) must not leak into T1's loaded collection.
        Assert.Equal(new[] { 1, 2 }, order.Lines.Select(l => l.Id).OrderBy(i => i).ToArray());
    }

    [Table("EclPerson")]
    public class Person
    {
        [Key] public int Id { get; set; }
        public Passport? Passport { get; set; }
    }

    [Table("EclPassport")]
    public class Passport
    {
        [Key] public int Id { get; set; }
        public int PersonId { get; set; }
        public bool IsRevoked { get; set; }
        public Person? Person { get; set; }
    }

    [Fact]
    public void Explicit_reference_load_principal_to_dependent_applies_the_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EclPerson (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE EclPassport (Id INTEGER PRIMARY KEY, PersonId INTEGER NOT NULL, IsRevoked INTEGER NOT NULL);" +
                "INSERT INTO EclPerson VALUES (1);" +
                "INSERT INTO EclPassport VALUES (1, 1, 1);"; // revoked
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Person>().HasOne(p => p.Passport).WithOne(pp => pp.Person).HasForeignKey(pp => pp.PersonId, p => p.Id)
        };
        opts.AddGlobalFilter<Passport>(pp => !pp.IsRevoked);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var person = ctx.Query<Person>().First();
        ctx.Entry(person).Reference("Passport").Load();

        // The revoked passport (the FK lives on the dependent) must read as absent, matching Include.
        Assert.Null(person.Passport);
    }
}
