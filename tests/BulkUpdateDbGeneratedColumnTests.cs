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
/// A native bulk update must SET only the updatable columns — never a DB-generated (computed /
/// store-generated) column or the TPH discriminator. SQLite and Postgres built the SET from
/// <c>!IsKey &amp;&amp; !IsTimestamp</c>, which still includes DB-generated columns, so a bulk update over an
/// entity with a computed column tried to assign that column: SQLite/Postgres reject writing a GENERATED
/// column (hard failure), and a store-generated default would be silently overwritten. The SET must use
/// the same UpdateColumns set SqlServer/MySQL already use.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class BulkUpdateDbGeneratedColumnTests
{
    [Table("BudgRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
        [DatabaseGenerated(DatabaseGeneratedOption.Computed)] public int Doubled { get; set; }
    }

    private static (SqliteConnection Keeper, DbContext Ctx) CreateDb()
    {
        var cs = $"Data Source=file:budg_{Guid.NewGuid():N}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE BudgRow_Test (
                    Id INTEGER PRIMARY KEY,
                    Val INTEGER NOT NULL,
                    Doubled INTEGER GENERATED ALWAYS AS (Val * 2) VIRTUAL);
                INSERT INTO BudgRow_Test (Id, Val) VALUES (1, 5), (2, 8);
                """;
            cmd.ExecuteNonQuery();
        }
        var cn = new SqliteConnection(cs);
        cn.Open();
        return (keeper, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public async Task Native_bulk_update_skips_computed_columns()
    {
        var (keeper, ctx) = CreateDb();
        using var _ = keeper;
        await using var __ = ctx;

        var updated = await ctx.BulkUpdateAsync(new[]
        {
            new Row { Id = 1, Val = 7 },
            new Row { Id = 2, Val = 9 },
        });

        Assert.Equal(2, updated);   // BUG: throws "cannot UPDATE generated column Doubled"

        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT Id, Val, Doubled FROM BudgRow_Test ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var rows = new System.Collections.Generic.List<(int Id, int Val, int Doubled)>();
        while (r.Read()) rows.Add((r.GetInt32(0), r.GetInt32(1), r.GetInt32(2)));

        Assert.Equal(7, rows.Single(x => x.Id == 1).Val);
        Assert.Equal(14, rows.Single(x => x.Id == 1).Doubled);   // recomputed from the new Val
        Assert.Equal(9, rows.Single(x => x.Id == 2).Val);
        Assert.Equal(18, rows.Single(x => x.Id == 2).Doubled);
    }
}
