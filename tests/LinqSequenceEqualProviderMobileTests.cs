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

[Trait("Category", TestCategory.Fast)]
public sealed class LinqSequenceEqualProviderMobileTests : IAsyncLifetime
{
    private SqliteConnection _connection = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _connection = new SqliteConnection("Data Source=:memory:");
        await _connection.OpenAsync();
        await using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SeqRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
            INSERT INTO SeqRow VALUES
                (1, 'a', 10),
                (2, 'b', 20),
                (3, 'c', 30),
                (4, 'd', 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_connection, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _connection.DisposeAsync();
    }

    [Fact]
    public void SequenceEqual_ordered_queryable_sources_returns_true_for_same_sequence()
    {
        var left = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
        var right = _ctx.Query<SeqRow>().Where(r => r.Score < 40).OrderBy(r => r.Id);

        Assert.True(left.SequenceEqual(right));
    }

    [Fact]
    public void SequenceEqual_ordered_queryable_sources_returns_false_for_different_order()
    {
        var left = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
        var right = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderByDescending(r => r.Id);

        Assert.False(left.SequenceEqual(right));
    }

    [Fact]
    public void SequenceEqual_ordered_queryable_source_matches_local_sequence_in_enumeration_order()
    {
        var left = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
        var local = new[]
        {
            new SeqRow { Id = 1, Name = "a", Score = 10 },
            new SeqRow { Id = 2, Name = "b", Score = 20 },
            new SeqRow { Id = 3, Name = "c", Score = 30 }
        };

        Assert.True(left.SequenceEqual(local));
    }

    [Fact]
    public void SequenceEqual_ordered_queryable_source_detects_local_order_difference()
    {
        var left = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
        var local = new[]
        {
            new SeqRow { Id = 3, Name = "c", Score = 30 },
            new SeqRow { Id = 2, Name = "b", Score = 20 },
            new SeqRow { Id = 1, Name = "a", Score = 10 }
        };

        Assert.False(left.SequenceEqual(local));
    }

    [Fact]
    public void SequenceEqual_ordered_queryable_source_matches_empty_local_sequence_only_when_query_empty()
    {
        var empty = _ctx.Query<SeqRow>().Where(r => r.Id > 99).OrderBy(r => r.Id);
        var nonEmpty = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);
        var local = Array.Empty<SeqRow>();

        Assert.True(empty.SequenceEqual(local));
        Assert.False(nonEmpty.SequenceEqual(local));
    }

    [Fact]
    public void SequenceEqual_requires_explicit_order_on_both_sources()
    {
        var left = _ctx.Query<SeqRow>().Where(r => r.Id <= 3);
        var right = _ctx.Query<SeqRow>().Where(r => r.Id <= 3).OrderBy(r => r.Id);

        var ex = Assert.Throws<NormUnsupportedFeatureException>(() => left.SequenceEqual(right));
        Assert.Contains("explicit OrderBy", ex.Message, StringComparison.Ordinal);
    }

    [Table("SeqRow")]
    private sealed class SeqRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }
}
