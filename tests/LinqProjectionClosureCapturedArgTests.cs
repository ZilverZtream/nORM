using System;
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
/// Strict pin + implement-first for SelectClauseVisitor's handling of
/// closure-captured locals passed as arguments to translatable methods.
/// ExpressionToSqlVisitor folds closures via TryGetConstantValue +
/// CreateSafeParameter, but SCV walks each argument verbatim and emits
/// a MemberAccess on the closure DisplayClass which falls through to its
/// "not mapped to a column in table" throw.
///
/// Surfaced from 9a445c9 (Enum.HasFlag projection): the multibit Where
/// test had to inline <c>Perms.Read | Perms.Write</c> instead of using a
/// local because the local capture broke SCV. This affects EVERY
/// projection method call that takes a captured non-column value -- e.g.
/// any user query that pre-computes a flag, a substring length, a
/// rounding precision and passes it to the SQL function.
///
/// Silent-wrongness shape: throws a misleading "X is not mapped" error
/// pointing the user at entity mapping when the real fix is a one-line
/// expression-tree fold. The Where-vs-projection asymmetry breaks parity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionClosureCapturedArgTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Flags]
    public enum Perms
    {
        None = 0,
        Read = 1,
        Write = 2,
        Admin = 4,
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PccaItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Perms INTEGER NOT NULL);
            INSERT INTO PccaItem VALUES
                (1, 'alpha', 1),
                (2, 'beta',  3),
                (3, 'gamma', 7);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PccaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_StartsWith_with_closure_captured_prefix_uses_local_value()
    {
        // Captures a local string. Without the SCV fold, this throws
        // "Member 'prefix' on type '<>c__DisplayClass_X' is not mapped".
        var prefix = "be";
        var result = await _ctx.Query<PccaItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Starts = p.Name.StartsWith(prefix) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.False(result[0].Starts); // 'alpha'
        Assert.True(result[1].Starts);  // 'beta'
        Assert.False(result[2].Starts); // 'gamma'
    }

    [Fact]
    public async Task Select_enum_HasFlag_with_closure_captured_flag_uses_local_value()
    {
        // Same shape that drove 9a445c9 to inline the OR. Now that closure
        // folding works in SCV, the local can be captured directly.
        var rwFlag = Perms.Read | Perms.Write;
        var result = await _ctx.Query<PccaItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, RW = p.Perms.HasFlag(rwFlag) })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.False(result[0].RW); // 1 -- only Read
        Assert.True(result[1].RW);  // 3 -- Read|Write
        Assert.True(result[2].RW);  // 7 -- Read|Write|Admin
    }

    [Fact]
    public async Task Select_string_Substring_with_closure_captured_length_uses_local_value()
    {
        var len = 3;
        var result = await _ctx.Query<PccaItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.Substring(0, len))
            .ToListAsync();
        Assert.Equal(new[] { "alp", "bet", "gam" }, result.ToArray());
    }

    [Table("PccaItem")]
    public sealed class PccaItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Perms Perms { get; set; }
    }
}
