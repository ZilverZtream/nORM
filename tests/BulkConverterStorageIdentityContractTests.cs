using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for value-converter storage identity between the BULK and DIRECT write paths (bulk-path
/// matrix cell, identity dimension).
///
/// Converter columns written through <c>BulkInsertAsync</c> store EXACTLY the same provider values as
/// the direct insert path - a negating int converter and an enum-to-name string converter both join
/// byte-identical between bulk- and direct-written sibling tables, and the raw dump shows the
/// PROVIDER representation (negated int, enum name text). Converter application itself on every bulk
/// route is pinned by the existing bulk-converter suite (BulkConverterCrossProviderTests,
/// BulkCudConverterColumnTests, BulkCudWhereConverterTests, BulkUpdateConverterCorruptionTests);
/// this contract adds the bulk==direct identity oracle.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkConverterStorageIdentityContractTests
{
    private enum Status { Active = 1, Archived = 2 }

    [Table("BulkConvIdentity")]
    private sealed class BulkR
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public Status St { get; set; }
    }

    [Table("DirConvIdentity")]
    private sealed class DirR
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public Status St { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private sealed class StatusToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    [Fact]
    public async Task Bulk_written_converter_columns_store_identical_provider_values_to_direct()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText =
                "CREATE TABLE BulkConvIdentity (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, St TEXT NOT NULL);" +
                "CREATE TABLE DirConvIdentity  (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, St TEXT NOT NULL);";
            c.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<BulkR>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
                mb.Entity<BulkR>().Property<Status>(p => p.St).HasConversion(new StatusToNameConverter());
                mb.Entity<DirR>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
                mb.Entity<DirR>().Property<Status>(p => p.St).HasConversion(new StatusToNameConverter());
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var rows = new (int Id, int Score, Status St)[] { (1, 10, Status.Active), (2, -7, Status.Archived) };
        await ctx.BulkInsertAsync(rows.Select(r => new BulkR { Id = r.Id, Score = r.Score, St = r.St }).ToList());
        foreach (var (id, score, st) in rows) await ctx.InsertAsync(new DirR { Id = id, Score = score, St = st });

        // Identity: bulk provider values equal direct provider values row-for-row.
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT COUNT(*) FROM BulkConvIdentity a JOIN DirConvIdentity b ON a.Id = b.Id AND a.Score = b.Score AND a.St = b.St;";
            Assert.Equal(rows.Length, Convert.ToInt32(c.ExecuteScalar()));
        }

        // And the stored form IS the provider representation (converter applied, not the model value).
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "SELECT Score, St FROM BulkConvIdentity WHERE Id = 1;";
            using var r = c.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(-10L, r.GetInt64(0));           // negated
            Assert.Equal("Active", r.GetString(1));      // enum name text
        }

        // Round-trip through the converter on read-back.
        var back = ((INormQueryable<BulkR>)ctx.Query<BulkR>()).AsNoTracking().OrderBy(x => x.Id).ToList();
        Assert.Equal(10, back[0].Score);
        Assert.Equal(Status.Active, back[0].St);
        Assert.Equal(-7, back[1].Score);
        Assert.Equal(Status.Archived, back[1].St);
    }
}
