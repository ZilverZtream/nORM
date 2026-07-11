using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// ExecuteDelete/ExecuteUpdate translate the source query's WHERE into a
/// set-based SQL statement. Client-materialized shapes — sequence reshapes,
/// streaming GroupBy, group-join results — exist only after materialization,
/// so no set-based statement can honor them; silently emitting the statement
/// for the underlying rows would delete or update rows the reshaped query
/// never described. These shapes must fail closed.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkCudClientShapeGuardTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BcgItem")]
    private class BcgItem
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE BcgItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER NOT NULL);" +
                "INSERT INTO BcgItem (Value) VALUES (10), (20), (30);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Execute_delete_over_a_reshaped_query_fails_closed_and_deletes_nothing()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var extra = new BcgItem { Value = 999 };
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.Query<BcgItem>().Append(extra).ExecuteDeleteAsync());

        Assert.Equal(3, await ctx.Query<BcgItem>().CountAsync());
    }

    [Fact]
    public async Task Execute_delete_over_a_streaming_group_by_fails_closed_and_deletes_nothing()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(
            () => ctx.Query<BcgItem>().GroupBy(x => x.Value).ExecuteDeleteAsync());

        Assert.Equal(3, await ctx.Query<BcgItem>().CountAsync());
    }

    [Fact]
    public async Task Execute_delete_with_plain_where_still_works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var affected = await ctx.Query<BcgItem>().Where(x => x.Value > 15).ExecuteDeleteAsync();

        Assert.Equal(2, affected);
        Assert.Equal(1, await ctx.Query<BcgItem>().CountAsync());
    }
}
