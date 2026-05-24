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
/// Strict pin + implement-first for <c>Enum.HasFlag</c> in projection.
/// Works in Where via ExpressionToSqlVisitor's inline emission (~line
/// 1121) as <c>(receiver &amp; flag) = flag</c>, but the projection path
/// routes through provider.TranslateFunction which returns null; SCV then
/// falls through to its generic function-name handler and emits raw
/// <c>HASFLAG(...)</c>, causing SQLite 'no such function' errors -- same
/// shape as d1e9fc5 / 0adce6a.
///
/// Silent-wrongness shapes:
///   * HasFlag collapsing to equality (=) returns true only when the
///     receiver equals the flag exactly -- catastrophic for permission-
///     style bit-tested columns.
///   * Wrong bit order yields all-zero results since AND of non-overlapping
///     bits is 0 != flag.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionEnumHasFlagTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Flags]
    public enum Perms
    {
        None = 0,
        Read = 1,
        Write = 2,
        Delete = 4,
        Admin = 8,
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PehfItem (Id INTEGER PRIMARY KEY, Perms INTEGER NOT NULL);
            INSERT INTO PehfItem VALUES
                (1, 0),    -- None
                (2, 1),    -- Read
                (3, 3),    -- Read | Write
                (4, 7),    -- Read | Write | Delete
                (5, 8);    -- Admin
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PehfItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_enum_HasFlag_projects_bool_per_row()
    {
        // HasFlag(Read) -> {Id 2, 3, 4} (bit 0 set). Silent-wrongness:
        // equality-collapse would return only {Id 2} -- visibly broken.
        var result = await _ctx.Query<PehfItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = p.Perms.HasFlag(Perms.Read) })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.False(result[0].R); // None
        Assert.True(result[1].R);  // Read
        Assert.True(result[2].R);  // Read|Write
        Assert.True(result[3].R);  // Read|Write|Delete
        Assert.False(result[4].R); // Admin (bit 3, no bit 0)
    }

    [Fact]
    public async Task Select_enum_HasFlag_with_multibit_flag_returns_true_only_when_all_bits_set()
    {
        // HasFlag(Read|Write) i.e. 3 -> ((Perms & 3) = 3) -> {Id 3, 4}.
        // Id 2 has only Read (1), Id 5 has only Admin (8) -- neither has
        // BOTH Read and Write set, so HasFlag returns false.
        var result = await _ctx.Query<PehfItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, RW = p.Perms.HasFlag(Perms.Read | Perms.Write) })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.False(result[0].RW);
        Assert.False(result[1].RW); // Read alone
        Assert.True(result[2].RW);
        Assert.True(result[3].RW);
        Assert.False(result[4].RW); // Admin alone
    }

    [Table("PehfItem")]
    public sealed class PehfItem
    {
        [Key] public int Id { get; set; }
        public Perms Perms { get; set; }
    }
}
