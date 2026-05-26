using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Include on a dependent entity with a composite primary key works: the FK is
/// a single column even though the dependent PK is composite, so the standard
/// IN-batched parent-key fetch loads children correctly.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqIncludeCompositePkErrorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IcpkOrder (Id INTEGER PRIMARY KEY, Customer TEXT NOT NULL);
            CREATE TABLE IcpkLine  (OrderId INTEGER NOT NULL, LineNo INTEGER NOT NULL, Amount INTEGER NOT NULL,
                                    PRIMARY KEY (OrderId, LineNo));
            INSERT INTO IcpkOrder VALUES (1,'Alice'),(2,'Bob');
            INSERT INTO IcpkLine  VALUES (1,1,100),(1,2,50),(2,1,200);
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IcpkOrder>().HasKey(o => o.Id);
                mb.Entity<IcpkLine>().HasKey(l => new { l.OrderId, l.LineNo });
                mb.Entity<IcpkOrder>().HasMany(o => o.Lines).WithOne()
                    .HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Include_on_composite_pk_dependent_loads_children()
    {
        var orders = await ((INormQueryable<IcpkOrder>)_ctx.Query<IcpkOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .ToListAsync();

        Assert.Equal(2, orders.Count);

        var alice = orders.First(o => o.Customer == "Alice");
        Assert.Equal(2, alice.Lines.Count);
        Assert.Contains(alice.Lines, l => l.LineNo == 1 && l.Amount == 100);
        Assert.Contains(alice.Lines, l => l.LineNo == 2 && l.Amount == 50);

        var bob = orders.First(o => o.Customer == "Bob");
        Assert.Single(bob.Lines);
        Assert.Equal(200, bob.Lines.First().Amount);
    }

    [Fact]
    public async Task Include_composite_pk_dependent_foreign_keys_correct()
    {
        var orders = await ((INormQueryable<IcpkOrder>)_ctx.Query<IcpkOrder>())
            .AsSplitQuery()
            .Include(o => o.Lines)
            .ToListAsync();

        foreach (var order in orders)
            Assert.All(order.Lines, l => Assert.Equal(order.Id, l.OrderId));
    }

    [Table("IcpkOrder")]
    public sealed class IcpkOrder
    {
        [Key] public int Id { get; set; }
        public string Customer { get; set; } = string.Empty;
        public ICollection<IcpkLine> Lines { get; set; } = new List<IcpkLine>();
    }

    [Table("IcpkLine")]
    public sealed class IcpkLine
    {
        public int OrderId { get; set; }
        public int LineNo { get; set; }
        public int Amount { get; set; }
    }
}
