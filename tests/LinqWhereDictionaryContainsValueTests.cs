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
/// Sister of f4f3219 (Dictionary.ContainsKey fix). Dictionary&lt;K,V&gt;.ContainsValue
/// is an instance method that scans values rather than keys; same shape as the
/// ContainsKey gate-and-handler problem but the handler must walk .Values instead
/// of .Keys when emitting the SQL IN clause.
///
/// Usage shape: <c>Where(p =&gt; dict.ContainsValue(p.Code))</c> -- keep rows
/// whose Code appears as a value in the in-memory dictionary.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDictionaryContainsValueTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DvItem (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO DvItem VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DvItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_Dictionary_ContainsValue_returns_rows_matching_dict_values()
    {
        // Dictionary maps fruit name -> color. ContainsValue("apple") would check
        // if any value equals "apple". For this test we use the dict's keys as
        // VALUES so we can construct a known match set.
        var lookup = new Dictionary<int, string>
        {
            { 1, "banana" },
            { 2, "cherry" }
        };
        var result = await _ctx.Query<DvItem>()
            .Where(i => lookup.ContainsValue(i.Code))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_Dictionary_Values_ToList_Contains_works_as_documented_workaround()
    {
        // Workaround pattern -- pre-existing List.Contains path (d97c5f0).
        var lookup = new Dictionary<int, string>
        {
            { 1, "apple" },
            { 2, "date" }
        };
        var values = lookup.Values.ToList();
        var result = await _ctx.Query<DvItem>()
            .Where(i => values.Contains(i.Code))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("DvItem")]
    public sealed class DvItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
