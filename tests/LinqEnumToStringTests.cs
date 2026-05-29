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
/// Verifies that calling .ToString() on an enum column in a projection is
/// translated server-side instead of requiring client-evaluation opt-in.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqEnumToStringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EtsRow (Id INTEGER PRIMARY KEY, Status INTEGER NOT NULL);
            INSERT INTO EtsRow VALUES (1, 0), (2, 1), (3, 2);
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
    public async Task Enum_ToString_in_projection_translates_to_case_expression()
    {
        var query = _ctx.Query<EtsRow>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, Name = r.Status.ToString() });
        var sql = query.ToString();
        Assert.Contains("CASE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(nameof(EtsStatus.Active), sql, StringComparison.Ordinal);
        Assert.Contains(nameof(EtsStatus.Pending), sql, StringComparison.Ordinal);
        Assert.Contains(nameof(EtsStatus.Archived), sql, StringComparison.Ordinal);

        var labels = (await query.ToListAsync()).ToArray();
        Assert.Equal(3, labels.Length);
        Assert.Equal(nameof(EtsStatus.Active), labels[0].Name);
        Assert.Equal(nameof(EtsStatus.Pending), labels[1].Name);
        Assert.Equal(nameof(EtsStatus.Archived), labels[2].Name);
    }

    public enum EtsStatus { Active = 0, Pending = 1, Archived = 2 }

    [Table("EtsRow")]
    public sealed class EtsRow
    {
        [Key] public int Id { get; set; }
        public EtsStatus Status { get; set; }
    }
}
