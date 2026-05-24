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
/// Pins <c>enumColumn.ToString()</c> inside a Select projection. Two ways
/// this can go silently wrong: (1) the SQL emitter routes it through the
/// generic ToString-CAST path and returns the underlying numeric as text
/// ("1" instead of "Active"); (2) client-eval split happens but the
/// enum materialiser comes back as int and the client-side .ToString()
/// also returns the number. Either failure makes user-facing dashboards
/// look like row data even though the rendered value is just an int.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionEnumToStringTests : IAsyncLifetime
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
            -- Underlying numeric values for the enum members below:
            --   Pending = 0, Active = 1, Archived = 2
            INSERT INTO EtsRow VALUES (1, 1), (2, 0), (3, 2), (4, 1), (5, 0);
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
    public async Task Projection_enum_tostring_returns_member_name_via_case_when_in_sql()
    {
        // The translator emits CASE WHEN col=<n> THEN '<name>' ... ELSE CAST(col
        // AS TEXT) END for enum.ToString() so the SQL returns the .NET-equivalent
        // member name rather than the underlying numeric, matching Enum.ToString
        // semantics. Defined values map to names; undefined ints (none here) fall
        // through to the ELSE branch for parity with .NET.
        var rows = await _ctx.Query<EtsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Label = r.Status.ToString() })
            .ToListAsync();
        Assert.Equal(new[]
        {
            (1, "Active"),
            (2, "Pending"),
            (3, "Archived"),
            (4, "Active"),
            (5, "Pending"),
        }, rows.Select(r => (r.Id, r.Label)).ToArray());
    }

    [Fact]
    public async Task Projection_enum_tostring_returns_integer_text_for_undefined_value()
    {
        // .NET's Enum.ToString returns the integer as text for undefined values
        // (e.g. (EtsStatus)99 -> "99"). The SQL CASE-WHEN ELSE branch preserves
        // this so the in-memory and SQL projections behave identically.
        await using (var seedCmd = _cn.CreateCommand())
        {
            seedCmd.CommandText = "INSERT INTO EtsRow VALUES (99, 42)";
            await seedCmd.ExecuteNonQueryAsync();
        }
        var label = await _ctx.Query<EtsRow>()
            .Where(r => r.Id == 99)
            .Select(r => r.Status.ToString())
            .FirstAsync();
        Assert.Equal("42", label);
    }

    [Fact]
    public async Task Projection_returning_enum_value_then_post_tostring_returns_member_name()
    {
        // Recommended workaround: project the enum value, then call
        // .ToString() in C# after the rows are materialised. The
        // materializer hydrates the underlying int back into the enum
        // type, and .NET's Enum.ToString() produces the member name.
        var rows = (await _ctx.Query<EtsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, r.Status })
            .ToListAsync())
            .Select(r => (r.Id, Label: r.Status.ToString()))
            .ToArray();
        Assert.Equal(new[]
        {
            (1, "Active"),
            (2, "Pending"),
            (3, "Archived"),
            (4, "Active"),
            (5, "Pending"),
        }, rows);
    }

    [Fact]
    public async Task Projection_enum_column_returned_as_enum_value()
    {
        // Baseline: when the user wants the enum value (not the name), the
        // materializer should hydrate the int back into the enum type.
        var rows = (await _ctx.Query<EtsRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, r.Status })
            .ToListAsync())
            .Select(r => (r.Id, r.Status))
            .ToArray();
        Assert.Equal(new[]
        {
            (1, EtsStatus.Active),
            (2, EtsStatus.Pending),
            (3, EtsStatus.Archived),
            (4, EtsStatus.Active),
            (5, EtsStatus.Pending),
        }, rows);
    }

    public enum EtsStatus
    {
        Pending = 0,
        Active = 1,
        Archived = 2,
    }

    [Table("EtsRow")]
    public sealed class EtsRow
    {
        [Key] public int Id { get; set; }
        public EtsStatus Status { get; set; }
    }
}
