using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Bulk ExecuteUpdate with server-side computed SET expressions: a column-to-column
/// arithmetic assignment, chained SetProperty (SQL evaluates every RHS against the
/// pre-update row, so a later SET sees the ORIGINAL value of an earlier-set column), and a
/// WHERE-filtered computed SET touching only matching rows.
/// </summary>
[Trait("Category", "Fast")]
public class BulkUpdateComputedSetTests
{
    [Table("BucRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    private static readonly (int Id, int A, int B)[] Data =
    {
        (1, 10, 3), (2, 20, 7), (3, 5, 100), (4, 0, -4),
    };

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:buc_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE BucRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
            foreach (var (id, a, b) in Data)
            {
                using var ins = keeper.CreateCommand();
                ins.CommandText = $"INSERT INTO BucRow VALUES ({id}, {a}, {b})";
                ins.ExecuteNonQuery();
            }
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static (int A, int B) Read(SqliteConnection k, int id)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = $"SELECT A, B FROM BucRow WHERE Id = {id}";
        using var r = cmd.ExecuteReader();
        r.Read();
        return (r.GetInt32(0), r.GetInt32(1));
    }

    [Fact]
    public async Task Computed_column_to_column_set()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // A := A*2 + B for every row.
        await ctx.Query<Row>().ExecuteUpdateAsync(s => s.SetProperty(x => x.A, x => x.A * 2 + x.B));

        foreach (var (id, a, b) in Data)
        {
            var (gotA, gotB) = Read(keeper, id);
            Assert.Equal(a * 2 + b, gotA);
            Assert.Equal(b, gotB); // B untouched
        }
    }

    [Fact]
    public async Task Chained_set_uses_pre_update_values()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        // A := A + 10; B := A + 100. SQL UPDATE evaluates every RHS against the ORIGINAL row,
        // so B uses the OLD A, not the just-updated A+10.
        await ctx.Query<Row>().ExecuteUpdateAsync(s => s
            .SetProperty(x => x.A, x => x.A + 10)
            .SetProperty(x => x.B, x => x.A + 100));

        foreach (var (id, a, b) in Data)
        {
            var (gotA, gotB) = Read(keeper, id);
            Assert.Equal(a + 10, gotA);
            Assert.Equal(a + 100, gotB); // OLD a, not (a+10)
        }
    }

    [Fact]
    public async Task Computed_set_with_where_only_touches_matching_rows()
    {
        var (keeper, make) = Setup();
        using var _ = keeper;
        await using var ctx = make();

        await ctx.Query<Row>().Where(x => x.A >= 10).ExecuteUpdateAsync(s => s.SetProperty(x => x.B, x => x.B + x.A));

        foreach (var (id, a, b) in Data)
        {
            var (gotA, gotB) = Read(keeper, id);
            Assert.Equal(a, gotA);
            Assert.Equal(a >= 10 ? b + a : b, gotB);
        }
    }
}
