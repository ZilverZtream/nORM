using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class DateTimeOffsetKeyedOperatorTests
{
    private readonly ITestOutputHelper _output;
    public DateTimeOffsetKeyedOperatorTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("DtoKey_Test")]
    public class DtoKeyRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task DistinctBy_a_DateTimeOffset_key_dedups_by_instant()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DtoKey_Test (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        // Rows 1 and 2 are the SAME INSTANT written with different offsets;
        // row 3 is a distinct instant.
        ctx.Add(new DtoKeyRow { Id = 1, Stamp = new DateTimeOffset(2026, 7, 14, 12, 0, 0, TimeSpan.Zero), Val = 1 });
        ctx.Add(new DtoKeyRow { Id = 2, Stamp = new DateTimeOffset(2026, 7, 14, 14, 0, 0, TimeSpan.FromHours(2)), Val = 2 });
        ctx.Add(new DtoKeyRow { Id = 3, Stamp = new DateTimeOffset(2026, 7, 14, 13, 0, 0, TimeSpan.Zero), Val = 3 });
        await ctx.SaveChangesAsync();

        var rows = new[]
        {
            new DtoKeyRow { Id = 1, Stamp = new DateTimeOffset(2026, 7, 14, 12, 0, 0, TimeSpan.Zero), Val = 1 },
            new DtoKeyRow { Id = 2, Stamp = new DateTimeOffset(2026, 7, 14, 14, 0, 0, TimeSpan.FromHours(2)), Val = 2 },
            new DtoKeyRow { Id = 3, Stamp = new DateTimeOffset(2026, 7, 14, 13, 0, 0, TimeSpan.Zero), Val = 3 },
        };

        var db = (await ctx.Query<DtoKeyRow>().OrderBy(r => r.Id).DistinctBy(r => r.Stamp).ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();
        var oracle = rows.OrderBy(r => r.Id).DistinctBy(r => r.Stamp)
            .Select(r => r.Id).OrderBy(x => x).ToList();

        _output.WriteLine($"db: [{string.Join(",", db)}] oracle: [{string.Join(",", oracle)}]");
        Assert.Equal(oracle, db);

        // UnionBy shares the partition-key emission: a local row whose key is the
        // same instant as a stored one (different offset) must not join the set.
        var locals = new[]
        {
            new DtoKeyRow { Id = 90, Stamp = new DateTimeOffset(2026, 7, 14, 15, 0, 0, TimeSpan.FromHours(3)), Val = 90 }, // == row 1's instant
            new DtoKeyRow { Id = 91, Stamp = new DateTimeOffset(2026, 7, 14, 20, 0, 0, TimeSpan.Zero), Val = 91 },          // new instant
        };
        var dbUnion = (await ctx.Query<DtoKeyRow>().OrderBy(r => r.Id).UnionBy(locals, r => r.Stamp).ToListAsync())
            .Select(r => r.Id).OrderBy(x => x).ToList();
        var oracleUnion = rows.OrderBy(r => r.Id).UnionBy(locals, r => r.Stamp)
            .Select(r => r.Id).OrderBy(x => x).ToList();
        _output.WriteLine($"union db: [{string.Join(",", dbUnion)}] oracle: [{string.Join(",", oracleUnion)}]");
        Assert.Equal(oracleUnion, dbUnion);
    }
}

