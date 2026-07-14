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
public class RowReplacementTests
{
    private readonly ITestOutputHelper _output;
    public RowReplacementTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("RepRow_Test")]
    public class RepRow
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task Delete_then_readd_same_key_in_one_batch()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE RepRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var original = new RepRow { Id = 1, Val = 10 };
        ctx.Add(original);
        await ctx.SaveChangesAsync();

        ctx.Remove(original);
        var replacement = new RepRow { Id = 1, Val = 99 };
        ctx.Add(replacement);

        _output.WriteLine("original state: " + ctx.Entry(original).State);
        _output.WriteLine("replacement state: " + ctx.Entry(replacement).State);

        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<RepRow>().ToListAsync();
        _output.WriteLine("rows: " + string.Join(" | ", rows.Select(r => $"{r.Id}:{r.Val}")));
        Assert.Single(rows);
        Assert.Equal(99, rows[0].Val);
    }
}

