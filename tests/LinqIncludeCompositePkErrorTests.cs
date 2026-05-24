using System;
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
/// Pins the deterministic error message for Include on a dependent type
/// that has a composite primary key. The IN-batched parent-key fetch
/// nORM uses for eager loading hardcodes single-column key matching;
/// composite-PK dependents would need either a tuple-IN predicate or a
/// JOIN-then-projection emit that the loader doesn't wire today. The
/// throw must point at the supported workarounds — manual JOIN with
/// projection, or a separate child query — instead of leaving the user
/// guessing at "manual loading".
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
                // Composite PK on the dependent — (OrderId, LineNo).
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
    public async Task Include_on_composite_pk_dependent_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ((INormQueryable<IcpkOrder>)_ctx.Query<IcpkOrder>())
                .Include(o => o.Lines)
                .AsSplitQuery()
                .ToListAsync();
        });
        // Message must identify the constraint and point at the supported workarounds.
        Assert.Contains("composite",  ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("join",       ex.Message, StringComparison.OrdinalIgnoreCase);
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
