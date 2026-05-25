using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>q.SelectMany(p => p.Children, (p, c) =&gt; c.Tag)</c> — the
/// bare-MemberAccess result selector form. The translator previously
/// emitted SELECT-all-columns and the materialiser then read column 0
/// (the outer's first column) and stringified it — returning parent Ids
/// where the caller expected child Tag values.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyBareMemberSelectorTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private SmbmContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmbmParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmbmChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO SmbmParent VALUES (1,'P1'),(2,'P2');
            INSERT INTO SmbmChild  VALUES (1,1,'a'),(2,1,'b'),(3,2,'c'),(4,2,'d');
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmbmParent>().HasKey(p => p.Id);
                mb.Entity<SmbmParent>().HasMany<SmbmChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new SmbmContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SelectMany_with_bare_child_member_selector_returns_member_values()
    {
        // SelectMany((p, c) => c.Tag) should return the 4 child Tag values.
        var tags = (await _ctx.Query<SmbmParent>()
            .OrderBy(p => p.Id)
            .SelectMany(p => p.Children, (p, c) => c.Tag)
            .ToListAsync())
            .OrderBy(t => t)
            .ToArray();
        Assert.Equal(new[] { "a", "b", "c", "d" }, tags);
    }

    [Fact]
    public async Task SelectMany_with_bare_parent_member_selector_returns_parent_member()
    {
        // SelectMany((p, c) => p.Name) — projecting parent's column repeats parent's
        // Name once per child. 2 children per parent → 4 rows: [P1, P1, P2, P2].
        var names = (await _ctx.Query<SmbmParent>()
            .OrderBy(p => p.Id)
            .SelectMany(p => p.Children, (p, c) => p.Name)
            .ToListAsync())
            .OrderBy(n => n)
            .ToArray();
        Assert.Equal(new[] { "P1", "P1", "P2", "P2" }, names);
    }

    [Fact]
    public async Task SelectMany_after_take_with_bare_child_member_selector_returns_windowed_tags()
    {
        // OrderBy(Id).Take(1) → parent 1. SelectMany(Children, c.Tag) → ['a', 'b'].
        var tags = (await _ctx.Query<SmbmParent>()
            .OrderBy(p => p.Id)
            .Take(1)
            .SelectMany(p => p.Children, (p, c) => c.Tag)
            .ToListAsync())
            .OrderBy(t => t)
            .ToArray();
        Assert.Equal(new[] { "a", "b" }, tags);
    }

    [Table("SmbmParent")]
    public sealed class SmbmParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmbmChild> Children { get; set; } = new();
    }

    [Table("SmbmChild")]
    public sealed class SmbmChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private sealed class SmbmContext : DbContext
    {
        public SmbmContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
