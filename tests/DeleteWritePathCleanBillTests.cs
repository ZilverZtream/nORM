using System;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// Delete-write-path clean-bill probes: set-based ExecuteDeleteAsync against a value-converter column must
/// target the SAME rows the read path would (bind the provider representation), and a delete inside a
/// caller-managed transaction that is rolled back must leave every row intact. These are written fail-first
/// (asserting the CORRECT outcome); passing confirms the behavior is sound.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeleteWritePathCleanBillTests
{
    [Table("EdConvDel")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static SqliteConnection Setup(out Func<DbContext> make, string seed)
    {
        var keeper = new SqliteConnection($"Data Source=file:edconvdel_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = $"CREATE TABLE EdConvDel (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL); {seed}";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        make = () =>
        {
            var cn = new SqliteConnection(cs); cn.Open();
            return new DbContext(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    var e = mb.Entity<Row>();
                    e.Property<int>(p => p.Val).HasConversion(new NegatingConverter());
                }
            });
        };
        return keeper;
    }

    private static System.Collections.Generic.List<(int Id, int Val)> Rows(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Id, Val FROM EdConvDel ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var v = new System.Collections.Generic.List<(int, int)>();
        while (r.Read()) v.Add((r.GetInt32(0), r.GetInt32(1)));
        return v;
    }

    [Fact]
    public async Task ExecuteDelete_converter_column_predicate_targets_correct_rows()
    {
        // Stored via converter: Val 10 -> -10, Val 12 -> -12.
        using var keeper = Setup(out var make, "INSERT INTO EdConvDel VALUES (1, -10), (2, -12)");
        await using var ctx = make();
        // Model predicate Val == 10 must match the row stored as -10 (row Id 1), not row 2.
        var deleted = await ctx.Query<Row>().Where(r => r.Val == 10).ExecuteDeleteAsync();
        Assert.Equal(1, deleted);
        Assert.Equal(new System.Collections.Generic.List<(int, int)> { (2, -12) }, Rows(keeper));
    }

    [Fact]
    public async Task DeleteAsync_inside_rolled_back_caller_transaction_leaves_rows_intact()
    {
        using var keeper = Setup(out var make, "INSERT INTO EdConvDel VALUES (1, -10), (2, -12)");
        await using var ctx = make();
        var row = ctx.Query<Row>().ToList().Single(r => r.Id == 1);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            await ctx.DeleteAsync(row);           // picks up ctx.CurrentTransaction
            await tx.RollbackAsync();
        }

        // Rolled back: both rows must survive with their original values.
        Assert.Equal(2, Rows(keeper).Count);
    }

    // ---------- composite-key DeleteAsync must target ALL key columns ----------
    [Table("EdCompDel")]
    public class Comp
    {
        public int A { get; set; }
        public int B { get; set; }
        public string Note { get; set; } = "";
    }

    [Fact]
    public async Task DeleteAsync_composite_key_targets_exact_row_not_partial_key()
    {
        var keeper = new SqliteConnection($"Data Source=file:edcompdel_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using var _k = keeper;
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE EdCompDel (A INTEGER NOT NULL, B INTEGER NOT NULL, Note TEXT NOT NULL, PRIMARY KEY (A, B));" +
                "INSERT INTO EdCompDel VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z');";
            cmd.ExecuteNonQuery();
        }
        var cs = keeper.ConnectionString;
        var cn = new SqliteConnection(cs); cn.Open();
        await using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Comp>().HasKey(e => new { e.A, e.B })
        });

        var row = ctx.Query<Comp>().ToList().Single(r => r.A == 1 && r.B == 1);
        await ctx.DeleteAsync(row);

        // Only (1,1) removed; (1,2) sharing A=1 and (2,1) sharing B=1 must survive.
        using var q = keeper.CreateCommand();
        q.CommandText = "SELECT A, B FROM EdCompDel ORDER BY A, B";
        using var r = q.ExecuteReader();
        var remaining = new System.Collections.Generic.List<(int, int)>();
        while (r.Read()) remaining.Add((r.GetInt32(0), r.GetInt32(1)));
        Assert.Equal(new System.Collections.Generic.List<(int, int)> { (1, 2), (2, 1) }, remaining);
    }
}
