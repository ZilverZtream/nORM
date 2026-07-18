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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a projected single-hop navigation aggregate (<c>o.Lines.Count()</c>,
/// <c>o.Lines.Sum(l =&gt; l.Amount)</c>) reads the dependent table by its MAPPED name, honouring a
/// fluent <c>ToTable()</c> override. The nav-aggregate subquery emit resolved its dependent
/// <c>FROM</c> table from the <c>[Table]</c> attribute (or the CLR type name) only, so a child
/// configured via fluent <c>ToTable("...")</c> with no matching attribute produced
/// <c>FROM "&lt;TypeName&gt;"</c> — a table that does not exist (SQL error) or, worse, a
/// differently-named real table (wrong rows). The FROM source now routes through the authoritative
/// <see cref="nORM.Mapping.TableMapping"/> like the two-hop and outer-query paths already do.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateFluentTableNameTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // The child's PHYSICAL table is "NtChildPhysical" — deliberately NOT the CLR type name
        // ("NtLine") that the attribute-only table resolver would emit.
        cmd.CommandText = """
            CREATE TABLE NtOrder (Id INTEGER PRIMARY KEY, Customer TEXT NOT NULL);
            CREATE TABLE NtChildPhysical (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO NtOrder VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Order 1: 3 lines summing 250; Order 2: 1 line summing 50; Order 3: no lines.
            INSERT INTO NtChildPhysical VALUES (1,1,100),(2,1,75),(3,1,75),(4,2,50);
            """;
        await cmd.ExecuteNonQueryAsync();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NtOrder>().HasKey(o => o.Id);
                mb.Entity<NtLine>().HasKey(l => l.Id);
                // Fluent table name, no [Table] attribute on NtLine → GetTableName would say "NtLine".
                mb.Entity<NtLine>().ToTable("NtChildPhysical");
                mb.Entity<NtOrder>().HasMany(o => o.Lines).WithOne()
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
    public async Task Projected_nav_Count_reads_the_fluent_table_name()
    {
        var rows = (await _ctx.Query<NtOrder>()
                .Select(o => new { o.Id, N = o.Lines.Count() })
                .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        Assert.Equal(new[] { 3, 1, 0 }, rows.Select(r => r.N).ToArray());
    }

    [Fact]
    public async Task Projected_nav_Sum_reads_the_fluent_table_name()
    {
        var rows = (await _ctx.Query<NtOrder>()
                .Select(o => new { o.Id, S = o.Lines.Sum(l => l.Amount) })
                .ToListAsync())
            .OrderBy(r => r.Id).ToArray();

        // Order 1 → 250, Order 2 → 50, Order 3 → 0 (no lines).
        Assert.Equal(new[] { 250, 50, 0 }, rows.Select(r => r.S).ToArray());
    }

    // NO [Table] attribute — the mapped name comes only from the fluent ToTable() above.
    public sealed class NtOrder
    {
        [Key] public int Id { get; set; }
        public string Customer { get; set; } = string.Empty;
        public ICollection<NtLine> Lines { get; set; } = new List<NtLine>();
    }

    public sealed class NtLine
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int Amount { get; set; }
    }
}
