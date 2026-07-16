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
/// Contract for Guid value fidelity through the BULK insert path (bulk-path matrix cell).
///
/// Guids bulk-insert as the same canonical LOWERCASE "D" text the direct path stores
/// (byte-identical sibling join), round-trip exactly (Empty, all-ones, mixed-case-parsed), and a
/// WHERE-equality bound from an uppercase-parsed Guid matches bulk-written rows.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkGuidFidelityContractTests
{
    [Table("BulkGuidFidelity")]
    private sealed class BulkG { [Key] public int Id { get; set; } public Guid Val { get; set; } }

    [Table("DirGuidFidelity")]
    private sealed class DirG { [Key] public int Id { get; set; } public Guid Val { get; set; } }

    private static readonly (int Id, Guid V)[] Rows =
    {
        (1, Guid.Empty),
        (2, Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")),
        (3, Guid.Parse("A1B2C3D4-E5F6-4708-9A0B-C1D2E3F4A5B6")),
    };

    [Fact]
    public async Task Bulk_guids_store_canonical_text_identical_to_direct_and_match_predicates()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkGuidFidelity (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);" +
                "CREATE TABLE DirGuidFidelity  (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        await ctx.BulkInsertAsync(Rows.Select(r => new BulkG { Id = r.Id, Val = r.V }).ToList());
        foreach (var (id, v) in Rows) await ctx.InsertAsync(new DirG { Id = id, Val = v });

        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM BulkGuidFidelity a JOIN DirGuidFidelity b ON a.Id = b.Id AND a.Val = b.Val;";
            Assert.Equal(Rows.Length, Convert.ToInt32(c.ExecuteScalar()));
        }

        var back = ((INormQueryable<BulkG>)ctx.Query<BulkG>()).AsNoTracking()
            .OrderBy(g => g.Id).Select(g => g.Val).ToList();
        Assert.Equal(Rows.Select(r => r.V).ToList(), back);

        var upperParsed = Guid.Parse("A1B2C3D4-E5F6-4708-9A0B-C1D2E3F4A5B6");
        Assert.Equal(new[] { 3 },
            ((INormQueryable<BulkG>)ctx.Query<BulkG>()).AsNoTracking()
                .Where(g => g.Val == upperParsed).Select(g => g.Id).ToList());
    }
}
