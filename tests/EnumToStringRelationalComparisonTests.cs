using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A relational comparison (&lt;, &lt;=, &gt;, &gt;=) against an enum column stored as its NAME (value converter to
/// string) must compare by the enum's underlying value, exactly as C# does — not lexicographically by member
/// name. The converter-column comparison bound the provider (string) value for relational operators, so
/// <c>o.Status &gt;= Status.Shipped</c> emitted <c>Status &gt;= 'Shipped'</c> and returned the wrong rows
/// (e.g. 'Cancelled' and 'Delivered' sort before 'Shipped').
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class EnumToStringRelationalComparisonTests
{
    private enum Status { Pending = 0, Processing = 1, Shipped = 2, Delivered = 3, Cancelled = 4 }

    private sealed class StatusStringConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>(value);
    }

    [Table("EsrOrder")]
    private sealed class Order
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Id == underlying enum value; Status stored as the member name.
            cmd.CommandText =
                "CREATE TABLE EsrOrder (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);" +
                "INSERT INTO EsrOrder (Id, Status) VALUES " +
                "(0,'Pending'),(1,'Processing'),(2,'Shipped'),(3,'Delivered'),(4,'Cancelled');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Order>().Property(o => o.Status).HasConversion(new StatusStringConverter())
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void GreaterThanOrEqual_compares_by_enum_value_not_name()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Order>().Where(o => o.Status >= Status.Shipped).Select(o => o.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3, 4 }, ids);   // BUG: only { 2 } — 'Cancelled'/'Delivered' sort before 'Shipped'
    }

    [Fact]
    public void LessThan_compares_by_enum_value_not_name()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Order>().Where(o => o.Status < Status.Processing).Select(o => o.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 0 }, ids);          // only Pending (value 0)
    }

    [Fact]
    public void Equality_still_compares_by_name()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var ids = ctx.Query<Order>().Where(o => o.Status == Status.Delivered).Select(o => o.Id).ToList();
        Assert.Equal(new[] { 3 }, ids);          // regression: == is unchanged
    }
}
