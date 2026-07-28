using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Owned-collection (OwnsMany) element columns must read back with the same invariant, type-aware conversion
/// the main-entity materializer uses. The owned-collection loader used Convert.ChangeType(object, Type) with
/// no format provider (current culture) and no Guid/temporal handling, so a decimal stored as TEXT read back
/// 100x wrong on a comma-decimal locale (silent corruption) and a Guid/DateTimeOffset TEXT column threw even
/// under the invariant culture — while the identical value on the main entity read correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionInvariantReadTests
{
    [Table("OcirOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public class Line
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
        public Guid Ref { get; set; }
    }

    private static readonly Guid RefGuid = new("11111111-2222-3333-4444-555555555555");

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Decimal + Guid stored as TEXT (how nORM stores them on SQLite).
            cmd.CommandText =
                "CREATE TABLE OcirOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE OcirLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount TEXT NOT NULL, Ref TEXT NOT NULL);" +
                "INSERT INTO OcirOrder VALUES (1);" +
                "INSERT INTO OcirLine VALUES (10, 1, '1234.56', '11111111-2222-3333-4444-555555555555');";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "OcirLine", foreignKey: "OrderId")
        });
        return (cn, ctx);
    }

    private static IDisposable ForceCulture(string name)
    {
        var prev = (CultureInfo.CurrentCulture, CultureInfo.CurrentUICulture);
        var c = new CultureInfo(name);
        CultureInfo.CurrentCulture = c; CultureInfo.CurrentUICulture = c;
        return new Restore(prev);
    }
    private sealed class Restore : IDisposable
    {
        private readonly (CultureInfo, CultureInfo) _p;
        public Restore((CultureInfo, CultureInfo) p) => _p = p;
        public void Dispose() { CultureInfo.CurrentCulture = _p.Item1; CultureInfo.CurrentUICulture = _p.Item2; }
    }

    [Fact]
    public void Owned_collection_guid_element_round_trips()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var order = ((INormQueryable<Order>)ctx.Query<Order>()).Include(o => o.Lines).ToList().Single();
        Assert.Equal(RefGuid, order.Lines.Single().Ref);
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    [InlineData("en-US")]
    public void Owned_collection_decimal_element_reads_invariantly_under_any_culture(string culture)
    {
        using var _culture = ForceCulture(culture);
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var order = ((INormQueryable<Order>)ctx.Query<Order>()).Include(o => o.Lines).ToList().Single();
        Assert.Equal(1234.56m, order.Lines.Single().Amount);
    }
}
