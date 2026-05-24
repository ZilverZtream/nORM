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
/// Pins <c>Union</c> across two anonymous projections that share field names but
/// come from different source tables. SQL semantics: UNION matches columns by
/// position, not name, and de-duplicates the result. Silent-wrongness risk if
/// the translator emits the two SELECT lists in different column orders, the
/// rows materialize with shuffled values (e.g. Tag where Name is expected) —
/// no exception, just wrong data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqUnionAnonymousProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UapPerson  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER NOT NULL);
            CREATE TABLE UapCompany (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL, Founded INTEGER NOT NULL);
            INSERT INTO UapPerson  VALUES (1,'Alice',30),(2,'Bob',45);
            INSERT INTO UapCompany VALUES (1,'Acme',1985),(2,'Globex',2002);
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
    public async Task Union_with_anonymous_projection_returns_distinct_rows_with_correct_column_values()
    {
        // Both sides project into { Label, Year } so the union is well-typed.
        // Label/Year values must come from the right source — Alice/30, Acme/1985, etc.
        var rows = (await _ctx.Query<UapPerson>()
            .Select(p => new { Label = p.Name, Year = p.Age })
            .Union(_ctx.Query<UapCompany>().Select(c => new { Label = c.Title, Year = c.Founded }))
            .ToListAsync())
            .OrderBy(r => r.Label)
            .ToArray();

        Assert.Equal(4, rows.Length);
        Assert.Equal("Acme",   rows[0].Label); Assert.Equal(1985, rows[0].Year);
        Assert.Equal("Alice",  rows[1].Label); Assert.Equal(30,   rows[1].Year);
        Assert.Equal("Bob",    rows[2].Label); Assert.Equal(45,   rows[2].Year);
        Assert.Equal("Globex", rows[3].Label); Assert.Equal(2002, rows[3].Year);
    }

    [Table("UapPerson")]
    public sealed class UapPerson
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    [Table("UapCompany")]
    public sealed class UapCompany
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public int Founded { get; set; }
    }
}
