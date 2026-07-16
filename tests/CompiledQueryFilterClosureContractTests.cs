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
/// Pins the compiled-query x global-filter closure contract, probed differentially: ONE
/// compiled delegate (Norm.CompileQuery) executed against two contexts whose filters share
/// the SAME expression shape but capture DIFFERENT values must apply each context's live
/// value — the compiled-plan layer discriminates filter expressions (pinned elsewhere), and
/// this pins the value-only case where the SQL text is identical and only the bound closure
/// differs. A baked first-execution value would silently filter the second context wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompiledQueryFilterClosureContractTests
{
    [Table("CqfcRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    private static readonly Func<DbContext, int, Task<List<Row>>> CompiledRows =
        Norm.CompileQuery((DbContext c, int maxId) =>
            c.Query<Row>().Where(r => r.Id <= maxId).OrderBy(r => r.Id));

    [Fact]
    public async Task One_compiled_delegate_rebinds_filter_closure_values_per_context()
    {
        var cs = $"Data Source=file:cqfc_{Guid.NewGuid():N}?mode=memory&cache=shared";
        using var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqfcRow_Test (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);" +
                "INSERT INTO CqfcRow_Test VALUES (1,1),(2,5),(3,9);";
            cmd.ExecuteNonQuery();
        }

        DbContext Ctx(int min)
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
            // Identical filter EXPRESSION, different captured value: the compiled-plan
            // key sees the same shape, so the closure value must re-bind per execution.
            opts.AddGlobalFilter<Row>(r => r.Val >= min);
            return new DbContext(cn, new SqliteProvider(), opts);
        }

        await using (var ctxA = Ctx(5))
        {
            var a = await CompiledRows(ctxA, 10);
            Assert.Equal(new[] { 2, 3 }, a.Select(r => r.Id).ToArray());
        }

        await using (var ctxB = Ctx(0))
        {
            var b = await CompiledRows(ctxB, 10);
            Assert.True(b.Count == 3 && b.Select(r => r.Id).SequenceEqual(new[] { 1, 2, 3 }),
                $"compiled delegate returned [{string.Join(",", b.Select(r => r.Id))}] for the min=0 context - " +
                "the first context's filter closure value was baked into the compiled plan");
        }
    }
}
