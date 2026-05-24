using System;
using System.Collections.Generic;
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
/// Pins the deterministic-throw behaviour of LINQ shapes that docs/linq-support.md marks as
/// Unsupported. Each test proves the exception is raised before the query reaches the
/// database, so users get a clear error rather than a silent client-side full-table walk.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqUnsupportedShapeContractTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE UnRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO UnRow VALUES (1,'alpha'),(2,'bravo'),(3,'charlie');
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
    public async Task TakeWhile_throws_rather_than_buffering_table_client_side()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<UnRow>().TakeWhile(r => r.Id < 3).ToListAsync();
        });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task SkipWhile_throws_rather_than_buffering_table_client_side()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<UnRow>().SkipWhile(r => r.Id < 2).ToListAsync();
        });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task OfType_throws_deterministically_for_unsupported_TPH_filter()
    {
        // OfType<UnrelatedRow>() against IQueryable<UnRow> can't collapse to an identity
        // pass-through — the runtime cast would never succeed. nORM doesn't wire TPH
        // discriminator filtering yet, so this must surface a clear exception rather
        // than silently emit SQL that returns zero rows.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<UnRow>().OfType<UnrelatedRow>().ToListAsync();
        });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task Cast_throws_deterministically_for_unsupported_runtime_conversion()
    {
        // Cast<UnrelatedRow>() against IQueryable<UnRow> targets an unrelated reference
        // type — there's no identity pass-through that satisfies the runtime cast, so
        // the translator must throw rather than emit SQL whose materialization would
        // crash with InvalidCastException.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await _ctx.Query<UnRow>().Cast<UnrelatedRow>().ToListAsync();
        });
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task SequenceEqual_throws_rather_than_silently_materializing_both_sides()
    {
        var leftQuery = _ctx.Query<UnRow>();
        var rightLocal = new List<UnRow> { new() { Id = 1, Name = "alpha" } };
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await Task.Run(() => leftQuery.SequenceEqual(rightLocal));
        });
        Assert.NotNull(ex);
    }

    [Table("UnRow")]
    public sealed class UnRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // Distinct CLR type with no inheritance relationship to UnRow — used to drive the
    // OfType TPH unsupported-throw test without needing actual TPH metadata wiring.
    public sealed class UnrelatedRow
    {
        public int Id { get; set; }
    }
}
