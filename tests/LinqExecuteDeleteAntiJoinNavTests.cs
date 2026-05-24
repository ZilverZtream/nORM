using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins the orphan-cleanup pattern:
/// <c>Query&lt;Parent&gt;().Where(p =&gt; !p.Children.Any()).ExecuteDeleteAsync()</c>.
/// Anti-join inside DELETE — high silent-wrongness risk because a broken
/// translation could delete parents that DO have children (catastrophic data
/// loss). Pin both the anti-join shape and its negated semi-join companion.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteDeleteAntiJoinNavTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EdaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE EdaChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
            -- Parents 1 and 3 have children; 2 and 4 are orphans.
            INSERT INTO EdaParent VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
            INSERT INTO EdaChild VALUES (10, 1), (11, 1), (12, 3);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<EdaParent>().HasKey(p => p.Id);
                mb.Entity<EdaChild>().HasKey(c => c.Id);
                mb.Entity<EdaParent>()
                    .HasMany(p => p.Children!)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ExecuteDelete_where_no_children_removes_only_orphan_parents()
    {
        var affected = await _ctx.Query<EdaParent>()
            .Where(p => !p.Children!.Any())
            .ExecuteDeleteAsync();
        Assert.Equal(2, affected);

        // Silent-wrongness probe: if !Any() failed to correlate and deleted ALL
        // parents (or deleted the ones with children instead), remaining Ids
        // would not be exactly [1, 3].
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM EdaParent ORDER BY Id";
        await using var rdr = await cmd.ExecuteReaderAsync();
        var remaining = new List<int>();
        while (await rdr.ReadAsync()) remaining.Add(rdr.GetInt32(0));
        Assert.Equal(new[] { 1, 3 }, remaining);
    }

    [Fact]
    public async Task ExecuteDelete_where_any_children_removes_only_parents_with_children()
    {
        // Sister shape: delete parents that DO have children. Verifies the
        // positive Any() arm composes with DELETE identically to the negated
        // form, so the comparison isn't accidentally inverted under the hood.
        var affected = await _ctx.Query<EdaParent>()
            .Where(p => p.Children!.Any())
            .ExecuteDeleteAsync();
        Assert.Equal(2, affected);

        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM EdaParent ORDER BY Id";
        await using var rdr = await cmd.ExecuteReaderAsync();
        var remaining = new List<int>();
        while (await rdr.ReadAsync()) remaining.Add(rdr.GetInt32(0));
        Assert.Equal(new[] { 2, 4 }, remaining);
    }

    [Table("EdaParent")]
    public sealed class EdaParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<EdaChild>? Children { get; set; }
    }

    [Table("EdaChild")]
    public sealed class EdaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }
}
