using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[ReadOnlyEntity]
[Table("RoGuardView")]
public sealed class RoGuardRow
{
    [Key] public int Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
/// Security regression (mass-assignment / fail-open): a [ReadOnlyEntity] (view / query-only) mapping must
/// reject writes on EVERY write path. The single-entity and SaveChanges paths already called
/// EnsureWritableMapping, but the three bulk entry points (BulkInsert/Update/DeleteAsync) did not — a
/// fail-open hole in a fail-closed contract that let a read-only entity be written through the bulk APIs.
/// All three now fail loud with NormUnsupportedFeatureException before any statement executes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class BulkWriteReadOnlyEntityGuardTests
{
    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    private static readonly RoGuardRow[] Rows = { new() { Id = 1, Name = "x" } };

    [Fact]
    public async Task BulkInsert_on_read_only_entity_is_rejected()
    {
        using var ctx = NewCtx();
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkInsertAsync(Rows));
        Assert.Contains("read-only", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkUpdate_on_read_only_entity_is_rejected()
    {
        using var ctx = NewCtx();
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkUpdateAsync(Rows));
        Assert.Contains("read-only", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkDelete_on_read_only_entity_is_rejected()
    {
        using var ctx = NewCtx();
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() => ctx.BulkDeleteAsync(Rows));
        Assert.Contains("read-only", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
