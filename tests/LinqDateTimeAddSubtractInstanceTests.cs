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
/// Pins server-side translation of <see cref="DateTime.Add(TimeSpan)"/> and
/// <see cref="DateTime.Subtract(TimeSpan)"/> as instance methods on a column
/// receiver. The expression-tree form is a <see cref="System.Linq.Expressions.MethodCallExpression"/>,
/// not a <see cref="System.Linq.Expressions.BinaryExpression"/>, so the existing
/// <c>DateTime + TimeSpan</c> binary handler doesn't fire automatically.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDateTimeAddSubtractInstanceTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DtaRow (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            INSERT INTO DtaRow VALUES
                (1, '2026-05-25 12:00:00'),
                (2, '2025-01-01 00:00:00'),
                (3, '2024-12-31 23:30:00');
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
    public async Task Add_TimeSpan_instance_method_on_column_shifts_forward()
    {
        var rows = (await _ctx.Query<DtaRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Plus = r.Stamp.Add(TimeSpan.FromHours(2)) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(new DateTime(2026,  5, 25, 14,  0, 0), rows[0].Plus);
        Assert.Equal(new DateTime(2025,  1,  1,  2,  0, 0), rows[1].Plus);
        Assert.Equal(new DateTime(2025,  1,  1,  1, 30, 0), rows[2].Plus);
    }

    [Fact]
    public async Task Subtract_TimeSpan_instance_method_on_column_shifts_backward()
    {
        var rows = (await _ctx.Query<DtaRow>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Minus = r.Stamp.Subtract(TimeSpan.FromHours(2)) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(new DateTime(2026,  5, 25, 10,  0, 0), rows[0].Minus);
        Assert.Equal(new DateTime(2024, 12, 31, 22,  0, 0), rows[1].Minus);
        Assert.Equal(new DateTime(2024, 12, 31, 21, 30, 0), rows[2].Minus);
    }

    [Fact]
    public async Task Add_TimeSpan_column_via_instance_method_on_column_shifts_forward()
    {
        // Seed an offset column. The instance Add(TimeSpan) form with a TimeSpan
        // COLUMN arg routes through AddTimeSpanColumnToDateTimeSql, not the
        // constant-seconds path.
        await using (var c = _ctx.Connection.CreateCommand())
        {
            c.CommandText = "ALTER TABLE DtaRow ADD COLUMN Offset TEXT NOT NULL DEFAULT '00:00:00';";
            await c.ExecuteNonQueryAsync();
        }
        await using (var c = _ctx.Connection.CreateCommand())
        {
            c.CommandText = "UPDATE DtaRow SET Offset = '02:00:00' WHERE Id IN (1,2,3);";
            await c.ExecuteNonQueryAsync();
        }
        var rows = (await _ctx.Query<DtaRowWithOffset>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, Plus = r.Stamp.Add(r.Offset) })
            .ToListAsync())
            .ToArray();
        Assert.Equal(new DateTime(2026,  5, 25, 14,  0, 0), rows[0].Plus);
        Assert.Equal(new DateTime(2025,  1,  1,  2,  0, 0), rows[1].Plus);
        Assert.Equal(new DateTime(2025,  1,  1,  1, 30, 0), rows[2].Plus);
    }

    [Table("DtaRow")]
    public sealed class DtaRow
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }

    [Table("DtaRow")]
    public sealed class DtaRowWithOffset
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Offset { get; set; }
    }
}

[Trait("Category", TestCategory.LiveProvider)]
public class LinqDateTimeAddSubtractInstanceLiveProviderTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Add_TimeSpan_instance_method_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveDtaRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Plus = r.Stamp.Add(TimeSpan.FromHours(2)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(new DateTime(2026,  5, 25, 14,  0, 0), rows[0].Plus);
                Assert.Equal(new DateTime(2025,  1,  1,  2,  0, 0), rows[1].Plus);
                Assert.Equal(new DateTime(2025,  1,  1,  1, 30, 0), rows[2].Plus);
            }
            finally { await Teardown(ctx); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task Subtract_TimeSpan_instance_method_on_live_provider(ProviderKind kind)
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
                var rows = (await ctx.Query<LiveDtaRow>()
                    .OrderBy(r => r.Id)
                    .Select(r => new { r.Id, Minus = r.Stamp.Subtract(TimeSpan.FromHours(2)) })
                    .ToListAsync())
                    .ToArray();
                Assert.Equal(new DateTime(2026,  5, 25, 10,  0, 0), rows[0].Minus);
                Assert.Equal(new DateTime(2024, 12, 31, 22,  0, 0), rows[1].Minus);
                Assert.Equal(new DateTime(2024, 12, 31, 21, 30, 0), rows[2].Minus);
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
            ProviderKind.SqlServer => "CREATE TABLE LiveDtaRow (Id INT PRIMARY KEY, Stamp DATETIME2 NOT NULL);",
            ProviderKind.Postgres  => "CREATE TABLE \"LiveDtaRow\" (\"Id\" INT PRIMARY KEY, \"Stamp\" TIMESTAMP NOT NULL);",
            ProviderKind.MySql     => "CREATE TABLE LiveDtaRow (Id INT PRIMARY KEY, Stamp DATETIME(6) NOT NULL);",
            _ => throw new ArgumentOutOfRangeException()
        };
        await c.ExecuteNonQueryAsync();
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = kind == ProviderKind.Postgres
            ? "INSERT INTO \"LiveDtaRow\" VALUES (1,'2026-05-25 12:00:00'),(2,'2025-01-01 00:00:00'),(3,'2024-12-31 23:30:00');"
            : "INSERT INTO LiveDtaRow VALUES (1,'2026-05-25 12:00:00'),(2,'2025-01-01 00:00:00'),(3,'2024-12-31 23:30:00');";
        await c2.ExecuteNonQueryAsync();
    }

    private static async Task Teardown(DbContext ctx)
    {
        await using var c = ctx.Connection.CreateCommand();
        c.CommandText = "DROP TABLE IF EXISTS LiveDtaRow;";
        try { await c.ExecuteNonQueryAsync(); } catch { }
        await using var c2 = ctx.Connection.CreateCommand();
        c2.CommandText = "DROP TABLE IF EXISTS \"LiveDtaRow\";";
        try { await c2.ExecuteNonQueryAsync(); } catch { }
    }

    [Table("LiveDtaRow")]
    public sealed class LiveDtaRow
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
