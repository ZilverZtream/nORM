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
/// Strict pin + implement-first for <c>Enum.IsDefined&lt;T&gt;(int)</c> in
/// Where. Common validation pattern: filter rows whose enum-int column
/// matches a defined enum value (filters out "invalid" enum ints like
/// 99 that arrived via wire data or legacy migration). Similar to the
/// enum.ToString fix from 9e3bca2 — enumerate the enum's defined values
/// at translation time and emit `col IN (...)`.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Returns all rows -> the validation gate doesn't actually filter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereEnumIsDefinedTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public enum Status
    {
        Pending = 0,
        Active = 1,
        Closed = 2,
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WeidItem (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL);
            INSERT INTO WeidItem VALUES
                (1, 0),
                (2, 1),
                (3, 2),
                (4, 99),
                (5, 1);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WeidItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_Enum_IsDefined_filters_to_defined_enum_values_only()
    {
        // Filter to rows whose Status int matches a defined enum member.
        // Defined: 0, 1, 2. Id 4 (Status=99) should be excluded.
        var result = await _ctx.Query<WeidItem>()
            .Where(p => Enum.IsDefined(typeof(Status), (int)p.Status))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WeidItem")]
    public sealed class WeidItem
    {
        [Key] public int Id { get; set; }
        public Status Status { get; set; }
    }
}
