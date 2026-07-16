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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for Guid write/read round-trip and predicate case handling (write-path matrix cell).
///
/// Guids round-trip exactly (Empty, all-ones, mixed) and store as canonical LOWERCASE "D"-format
/// text regardless of the casing of the source string the Guid was parsed from - .NET Guids carry no
/// case, and the binder always emits the canonical form, so a WHERE-equality bound from an
/// UPPERCASE-parsed Guid matches the stored row, list Contains matches across parse casings, and a
/// Guid primary-key lookup resolves correctly.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GuidRoundTripContractTests
{
    [Table("GuidRtContract")]
    private sealed class G
    {
        [Key] public int Id { get; set; }
        public Guid Val { get; set; }
    }

    [Table("GuidKeyContract")]
    private sealed class GK
    {
        [Key] public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static readonly Guid Mixed = Guid.Parse("A1B2C3D4-E5F6-4708-9A0B-C1D2E3F4A5B6");

    private static readonly (int Id, Guid V)[] Rows =
    {
        (1, Guid.Empty),
        (2, Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")),
        (3, Mixed),
    };

    private static async Task<(SqliteConnection cn, DbContext ctx)> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE GuidRtContract (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);" +
                            "CREATE TABLE GuidKeyContract (Id TEXT PRIMARY KEY, Name TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, v) in Rows) await ctx.InsertAsync(new G { Id = id, Val = v });
        return (cn, ctx);
    }

    [Fact]
    public async Task Guids_round_trip_exactly_and_store_canonical_lowercase()
    {
        var (cn, ctx) = await SeedAsync();
        using (ctx)
        {
            var back = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking()
                .OrderBy(g => g.Id).Select(g => g.Val).ToList();
            Assert.Equal(Rows.Select(r => r.V).ToList(), back);

            // The mixed-case-parsed Guid stores as canonical lowercase "D" text.
            using var c = cn.CreateCommand();
            c.CommandText = "SELECT Val FROM GuidRtContract WHERE Id = 3;";
            Assert.Equal("a1b2c3d4-e5f6-4708-9a0b-c1d2e3f4a5b6", (string)c.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task Predicates_match_regardless_of_source_string_casing()
    {
        var (_, ctx) = await SeedAsync();
        using (ctx)
        {
            var q = ((INormQueryable<G>)ctx.Query<G>()).AsNoTracking();

            var upperParsed = Guid.Parse("A1B2C3D4-E5F6-4708-9A0B-C1D2E3F4A5B6");
            Assert.Equal(new[] { 3 }, q.Where(g => g.Val == upperParsed).Select(g => g.Id).ToList());

            var list = new List<Guid> { Guid.Empty, upperParsed };
            Assert.Equal(new[] { 1, 3 },
                q.Where(g => list.Contains(g.Val)).Select(g => g.Id).ToList().OrderBy(x => x));
        }
    }

    [Fact]
    public async Task Guid_primary_key_lookup_resolves()
    {
        var (_, ctx) = await SeedAsync();
        using (ctx)
        {
            var key = Guid.NewGuid();
            await ctx.InsertAsync(new GK { Id = key, Name = "k1" });

            var name = ((INormQueryable<GK>)ctx.Query<GK>()).AsNoTracking()
                .Where(k => k.Id == key).Select(k => k.Name).Single();
            Assert.Equal("k1", name);
        }
    }
}
