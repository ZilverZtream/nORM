using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ORDER BY over a DateTimeOffset column must sort by instant (the order LINQ-to-Objects gives),
/// not by the raw stored text. On SQLite a DateTimeOffset stores as offset-suffixed TEXT, so mixed
/// offsets sort wrong under a plain text ORDER BY even though WHERE already normalizes to the UTC
/// instant — an asymmetry that silently returns rows in the wrong order. Plain DateTime stores as
/// fixed-width offset-free TEXT whose lexical order already is chronological (including MaxValue),
/// so it must keep sorting correctly and must NOT be wrapped in datetime() (which overflows on
/// DateTime.MaxValue's .9999999 fraction).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DateTimeOrderingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Evt: mixed-offset DateTimeOffset values whose TEXT order (A,B,C by Id) disagrees with
        // their instant order. Rec: DateTime including Min/Max and normal values.
        //  A id1 2020-01-01 00:30:00+00:00 -> 00:30 UTC
        //  B id2 2020-01-01 01:00:00+05:00 -> 2019-12-31 20:00 UTC  (earliest instant)
        //  C id3 2020-01-01 02:00:00+00:00 -> 02:00 UTC             (latest instant)
        //  4 id4 9999-12-31 23:59:59.9999999+00:00 -> MaxValue instant (latest)
        //  5 id5 0001-01-01 00:00:00.0000000+00:00 -> MinValue instant (earliest)
        //  6 id6 2020-01-01 00:30:00.5000000-03:00 -> 03:30:00.5 UTC (fraction + negative offset)
        cmd.CommandText = """
            CREATE TABLE Evt (Id INTEGER PRIMARY KEY, WhenAt TEXT NOT NULL);
            INSERT INTO Evt VALUES
                (1, '2020-01-01 00:30:00+00:00'),
                (2, '2020-01-01 01:00:00+05:00'),
                (3, '2020-01-01 02:00:00+00:00'),
                (4, '9999-12-31 23:59:59.9999999+00:00'),
                (5, '0001-01-01 00:00:00.0000000+00:00'),
                (6, '2020-01-01 00:30:00.5000000-03:00');
            CREATE TABLE Rec (Id INTEGER PRIMARY KEY, Ts TEXT NOT NULL);
            INSERT INTO Rec VALUES
                (1, '2020-06-01 12:00:00'),
                (2, '9999-12-31 23:59:59.9999999'),
                (3, '1999-12-31 23:59:59'),
                (4, '0001-01-01 00:00:00'),
                (5, '2020-06-01 12:00:01');
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
    public async Task OrderBy_datetimeoffset_mixed_offsets_sorts_by_instant()
    {
        var ordered = (await _ctx.Query<Evt>().OrderBy(e => e.WhenAt).ToListAsync())
            .Select(e => e.Id).ToArray();
        // Instant order: MinValue(5) < id2(2019-12-31 20:00) < id1(00:30) < id3(02:00)
        //   < id6(03:30:00.5) < MaxValue(4).
        Assert.Equal(new[] { 5, 2, 1, 3, 6, 4 }, ordered);
    }

    [Fact]
    public async Task OrderByDescending_datetimeoffset_mixed_offsets_sorts_by_instant()
    {
        var ordered = (await _ctx.Query<Evt>().OrderByDescending(e => e.WhenAt).ToListAsync())
            .Select(e => e.Id).ToArray();
        Assert.Equal(new[] { 4, 6, 3, 1, 2, 5 }, ordered);
    }

    [Fact]
    public async Task OrderBy_datetime_including_extremes_sorts_chronologically()
    {
        var ordered = (await _ctx.Query<Rec>().OrderBy(r => r.Ts).ToListAsync())
            .Select(r => r.Id).ToArray();
        // Chronological: MinValue(4) < 1999(3) < 2020-12:00:00(1) < 2020-12:00:01(5) < MaxValue(2).
        Assert.Equal(new[] { 4, 3, 1, 5, 2 }, ordered);
    }

    [Table("Evt")]
    public sealed class Evt
    {
        [Key] public int Id { get; set; }
        public DateTimeOffset WhenAt { get; set; }
    }

    [Table("Rec")]
    public sealed class Rec
    {
        [Key] public int Id { get; set; }
        public DateTime Ts { get; set; }
    }
}
