using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of <c>enum.HasFlag(other)</c> on a mapped
/// integer-backed [Flags] enum column. The natural lowering is
/// <c>(col &amp; other) = other</c>, which preserves the .NET semantics that
/// HasFlag returns true when every bit of <c>other</c> is set in the receiver.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqEnumHasFlagTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FlRow (Id INTEGER PRIMARY KEY, Perms INTEGER NOT NULL);
            INSERT INTO FlRow VALUES
                (1, 1),   -- Read
                (2, 3),   -- Read | Write
                (3, 5),   -- Read | Admin
                (4, 7),   -- Read | Write | Admin
                (5, 0);   -- none
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task HasFlag_with_single_bit_filters_rows_having_that_bit()
    {
        var rows = (await _ctx.Query<FlRow>()
            .Where(r => r.Perms.HasFlag(Perm.Admin))
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        // Admin (4) appears in rows 3 (5=Read|Admin) and 4 (7=Read|Write|Admin).
        Assert.Equal(2, rows.Length);
        Assert.Equal(3, rows[0].Id);
        Assert.Equal(4, rows[1].Id);
    }

    [Fact]
    public async Task HasFlag_with_combined_bits_requires_all_bits_set()
    {
        // Read|Admin (5) is fully set on rows 3 (5) and 4 (7). Row 1 (just Read) and
        // row 2 (Read|Write=3) miss the Admin bit.
        var rows = (await _ctx.Query<FlRow>()
            .Where(r => r.Perms.HasFlag(Perm.Read | Perm.Admin))
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(new[] { 3, 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Flags]
    public enum Perm
    {
        None = 0,
        Read = 1,
        Write = 2,
        Admin = 4,
        Delete = 8,
    }

    [Table("FlRow")]
    public sealed class FlRow
    {
        [Key] public int Id { get; set; }
        public Perm Perms { get; set; }
    }
}
