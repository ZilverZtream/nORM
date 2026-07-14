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
public class GroupJoinClosureKeyTests
{
    private readonly ITestOutputHelper _output;
    public GroupJoinClosureKeyTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("GkdParent")]
    public sealed class GkdParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GkdChild")]
    public sealed class GkdChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
    }

    [Fact]
    public async Task GroupJoin_with_closure_modulus_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GkdParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE GkdChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO GkdParent VALUES (1,'a'),(2,'b'),(3,'c');
                INSERT INTO GkdChild  VALUES (1,1,10),(2,2,20),(3,3,30),(4,4,40);
                """;
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        var m = 2;
        var q = ctx.Query<GkdParent>().Where(p => p.Id > 0)
            .GroupJoin(ctx.Query<GkdChild>(), p => p.Id % m, c => c.ParentId % m,
                (p, cs) => new { p.Id, N = cs.Count() });
        _output.WriteLine("SQL: " + q.ToString());
        var rows = (await q.ToListAsync()).OrderBy(x => x.Id).ToList();
        _output.WriteLine("rows: " + string.Join(" | ", rows));

        // C#: keys 1%2=1,2%2=0,3%2=1; children keys 1,0,1,0 → p1:{c1,c3}, p2:{c2,c4}, p3:{c1,c3}
        Assert.Equal(new[] { 2, 2, 2 }, rows.Select(x => x.N).ToArray());

        // Second execution with a different modulus hits the cached plan.
        m = 3;
        var q2 = ctx.Query<GkdParent>().Where(p => p.Id > 0)
            .GroupJoin(ctx.Query<GkdChild>(), p => p.Id % m, c => c.ParentId % m,
                (p, cs) => new { p.Id, N = cs.Count() });
        var rows2 = (await q2.ToListAsync()).OrderBy(x => x.Id).ToList();
        _output.WriteLine("rows2: " + string.Join(" | ", rows2));

        // C#: parent keys 1,2,0; child keys 1,2,0,1 → p1:{c1,c4}=2, p2:{c2}=1, p3:{c3}=1
        Assert.Equal(new[] { 2, 1, 1 }, rows2.Select(x => x.N).ToArray());
    }
}

