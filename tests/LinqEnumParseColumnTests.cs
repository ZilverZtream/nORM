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
/// Pins server-side translation of <see cref="Enum.Parse{T}(string)"/> applied
/// to a TEXT column. The translator emits a CASE-WHEN cascading the column's
/// string value into the enum's underlying integer; the materialiser then
/// returns the correct enum value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqEnumParseColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public enum Severity { Low = 1, Medium = 2, High = 3 }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EpcRow (Id INTEGER PRIMARY KEY, SeverityName TEXT NOT NULL);
            INSERT INTO EpcRow VALUES
                (1, 'Low'),
                (2, 'Medium'),
                (3, 'High'),
                (4, 'Low');
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
    public async Task Enum_Parse_generic_form_on_string_column_returns_enum_in_projection()
    {
        var rows = (await _ctx.Query<EpcRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Sev = Enum.Parse<Severity>(r.SeverityName) })
            .ToListAsync())
            .ToArray();

        Assert.Equal(Severity.Low,    rows[0].Sev);
        Assert.Equal(Severity.Medium, rows[1].Sev);
        Assert.Equal(Severity.High,   rows[2].Sev);
        Assert.Equal(Severity.Low,    rows[3].Sev);
    }

    [Fact]
    public async Task Enum_Parse_generic_form_on_string_column_filters_in_where()
    {
        var rows = (await _ctx.Query<EpcRow>()
            .Where(r => Enum.Parse<Severity>(r.SeverityName) == Severity.High)
            .OrderBy(r => r.Id)
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal(3, rows[0].Id);
    }

    [Table("EpcRow")]
    public sealed class EpcRow
    {
        [Key] public int Id { get; set; }
        public string SeverityName { get; set; } = string.Empty;
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqEnumParseColumnLiveProviderTests
{
    public enum Severity { Low = 1, Medium = 2, High = 3 }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Enum_Parse_generic_form_on_string_column_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured (set NORM_TEST_*)")) return;
        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await Setup(ctx, kind);
            try
            {
                var rows = (await ctx.Query<LiveEpcRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Sev = Enum.Parse<Severity>(r.SeverityName) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(Severity.Low,    rows[0].Sev);
                Assert.Equal(Severity.Medium, rows[1].Sev);
                Assert.Equal(Severity.High,   rows[2].Sev);
                Assert.Equal(Severity.Low,    rows[3].Sev);
            }
            finally { await Teardown(ctx); }
        }
    }

    private static async Task Setup(DbContext ctx, ProviderKind kind)
    {
        await Teardown(ctx);
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = kind switch
        {
            ProviderKind.SqlServer => "CREATE TABLE LiveEpcRow (Id INT PRIMARY KEY, SeverityName NVARCHAR(50) NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveEpcRow\" (\"Id\" INT PRIMARY KEY, \"SeverityName\" VARCHAR(50) NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveEpcRow (Id INT PRIMARY KEY, SeverityName VARCHAR(50) NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveEpcRow\" VALUES (1,'Low'),(2,'Medium'),(3,'High'),(4,'Low');"
            : "INSERT INTO LiveEpcRow VALUES (1,'Low'),(2,'Medium'),(3,'High'),(4,'Low');";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveEpcRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveEpcRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveEpcRow")]
    public sealed class LiveEpcRow
    {
        [Key] public int Id { get; set; }
        public string SeverityName { get; set; } = string.Empty;
    }
}
