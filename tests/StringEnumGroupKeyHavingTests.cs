using System;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// A relational/equality comparison over a string-backed (value-converter) enum GROUP KEY in a HAVING
/// clause — GroupBy(o => o.Status).Where(g => g.Key OP Status.X) — must compare by the enum VALUE, exactly
/// as the WHERE path does. The HAVING path bound the enum's underlying int against the TEXT group-key column,
/// so SQLite affinity made >= match every group and == match none — silently wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringEnumGroupKeyHavingTests
{
    public enum Status { Pending = 0, Processing = 1, Shipped = 2, Delivered = 3, Cancelled = 4 }

    private sealed class StatusStringConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status value) => value.ToString();
        public override object? ConvertFromProvider(string value) => Enum.Parse<Status>((string)value);
    }

    [Table("EsbOrder")]
    public sealed class Order { [Key] public int Id { get; set; } public Status Status { get; set; } }

    private static async Task<DbContext> Bootstrap(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE EsbOrder (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().Property(o => o.Status).HasConversion(new StatusStringConverter());
            }
        }, ownsConnection: false);
        // Insert through the converter so Status stores the member name.
        foreach (var (id, st) in new[] { (1, Status.Pending), (2, Status.Shipped), (3, Status.Shipped),
                                         (4, Status.Delivered), (5, Status.Cancelled), (6, Status.Pending) })
            ctx.Add(new Order { Id = id, Status = st });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task Having_ge_on_string_enum_group_key_compares_by_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = await Bootstrap(cn);
        var actual = ctx.Query<Order>().GroupBy(o => o.Status)
            .Where(g => g.Key >= Status.Shipped)
            .Select(g => new { g.Key, N = g.Count() })
            .ToList().OrderBy(x => x.Key).ToList();
        // Shipped(2)+Delivered(1)+Cancelled(1); Pending (value 0) must NOT be included.
        Assert.Equal(new[] { Status.Shipped, Status.Delivered, Status.Cancelled },
            actual.Select(x => x.Key).ToArray());
    }

    [Fact]
    public async Task Having_eq_on_string_enum_group_key_matches_group()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        await using var ctx = await Bootstrap(cn);
        var actual = ctx.Query<Order>().GroupBy(o => o.Status)
            .Where(g => g.Key == Status.Shipped)
            .Select(g => new { g.Key, N = g.Count() })
            .ToList();
        Assert.Single(actual);
        Assert.Equal(Status.Shipped, actual[0].Key);
        Assert.Equal(2, actual[0].N);
    }
}
