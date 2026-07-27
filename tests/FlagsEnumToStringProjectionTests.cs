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
/// Enum.ToString() on a [Flags] enum decomposes a combined value into its ", "-joined member names
/// ("Read, Write"), not the underlying integer. A translated projection / predicate over such a column
/// must produce the same text as .NET, including the leftover-bits -> numeric fallback and the value-0
/// member name.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FlagsEnumToStringProjectionTests
{
    [Flags]
    public enum Perm { None = 0, Read = 1, Write = 2, Execute = 4 }

    [Table("FlagsRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public Perm P { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb(params int[] storedValues)
    {
        var cs = $"Data Source=file:flags_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            var rows = string.Join(", ", storedValues.Select((v, i) => $"({i + 1}, {v})"));
            cmd.CommandText = $"CREATE TABLE FlagsRow_Test (Id INTEGER PRIMARY KEY, P INTEGER NOT NULL); "
                            + $"INSERT INTO FlagsRow_Test VALUES {rows};";
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Projected_flags_ToString_matches_dotnet_for_every_shape()
    {
        // combined, single, zero-member, all-set, undefined-bit, non-adjacent combo
        int[] stored = { 3, 1, 0, 7, 8, 5 };
        var (keeper, ctx) = CreateDb(stored);
        using var _ = keeper;
        await using var __ = ctx;

        var projected = await ctx.Query<Row>()
            .OrderBy(r => r.Id)
            .Select(r => new { r.Id, S = r.P.ToString() })
            .ToListAsync();

        Assert.Equal(stored.Length, projected.Count);
        for (int i = 0; i < stored.Length; i++)
        {
            var expected = ((Perm)stored[i]).ToString();   // .NET's own flags formatting
            Assert.Equal(expected, projected[i].S);
        }
    }

    [Fact]
    public async Task Predicate_on_flags_ToString_matches_combined_value()
    {
        var (keeper, ctx) = CreateDb(3, 1, 2);
        using var _ = keeper;
        await using var __ = ctx;

        var combined = ((Perm)3).ToString();   // "Read, Write"
        var matched = await ctx.Query<Row>()
            .Where(r => r.P.ToString() == combined)
            .Select(r => new { r.Id })
            .ToListAsync();

        Assert.Equal(new[] { 1 }, matched.Select(x => x.Id).OrderBy(i => i).ToArray());
    }
}
