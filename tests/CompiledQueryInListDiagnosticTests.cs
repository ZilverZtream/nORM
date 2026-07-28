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
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public sealed class CompiledQueryInListDiagnosticTests
{
    private readonly ITestOutputHelper _out;
    public CompiledQueryInListDiagnosticTests(ITestOutputHelper output) => _out = output;

    [Table("CqInList")]
    public sealed class Rec
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public int Score { get; set; }
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqInList (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        for (int i = 1; i <= 6; i++) ctx.Add(new Rec { Id = i, Score = i * 10 });
        await ctx.SaveChangesAsync();
        return ctx;
    }

    private static int[] Ids(IEnumerable<Rec> rows) => rows.Select(r => r.Id).OrderBy(x => x).ToArray();

    [Fact]
    public async Task Diagnose_ParamArray_Contains_VaryingLength()
    {
        using var ctx = await CtxAsync();

        Func<DbContext, int[], Task<List<Rec>>>? compiled = null;
        try
        {
            compiled = Norm.CompileQuery((DbContext c, int[] ids) => c.Query<Rec>().Where(x => ids.Contains(x.Id)));
            _out.WriteLine("COMPILE: succeeded");
        }
        catch (Exception ex)
        {
            _out.WriteLine($"COMPILE THREW: {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var argLists = new[]
        {
            new[] { 1, 2, 3 },
            new[] { 4 },
            new[] { 2, 5 },
            new[] { 1, 2, 3, 4, 5, 6 },
            Array.Empty<int>(),
        };

        foreach (var ids in argLists)
        {
            var local = ids;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => local.Contains(x.Id)).ToListAsync());
            string actualStr;
            bool mismatch = false;
            try
            {
                var actual = Ids(await compiled(ctx, ids));
                actualStr = "[" + string.Join(",", actual) + "]";
                mismatch = !oracle.SequenceEqual(actual);
            }
            catch (Exception ex)
            {
                actualStr = $"THREW {ex.GetType().Name}";
            }
            _out.WriteLine($"ids=[{string.Join(",", ids)}] oracle=[{string.Join(",", oracle)}] actual={actualStr} {(mismatch ? "<<< SILENT-WRONG MISMATCH" : "")}");
        }
    }

    [Fact]
    public async Task Diagnose_ParamList_Contains_VaryingLength()
    {
        using var ctx = await CtxAsync();

        Func<DbContext, List<int>, Task<List<Rec>>>? compiled = null;
        try
        {
            compiled = Norm.CompileQuery((DbContext c, List<int> ids) => c.Query<Rec>().Where(x => ids.Contains(x.Id)));
            _out.WriteLine("COMPILE: succeeded");
        }
        catch (Exception ex)
        {
            _out.WriteLine($"COMPILE THREW: {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var argLists = new[]
        {
            new List<int> { 1, 2, 3 },
            new List<int> { 4 },
            new List<int> { 2, 5 },
            new List<int> { 1, 2, 3, 4, 5, 6 },
            new List<int>(),
        };

        foreach (var ids in argLists)
        {
            var local = ids;
            var oracle = Ids(await ctx.Query<Rec>().Where(x => local.Contains(x.Id)).ToListAsync());
            string actualStr;
            bool mismatch = false;
            try
            {
                var actual = Ids(await compiled(ctx, ids));
                actualStr = "[" + string.Join(",", actual) + "]";
                mismatch = !oracle.SequenceEqual(actual);
            }
            catch (Exception ex)
            {
                actualStr = $"THREW {ex.GetType().Name}";
            }
            _out.WriteLine($"ids=[{string.Join(",", ids)}] oracle=[{string.Join(",", oracle)}] actual={actualStr} {(mismatch ? "<<< SILENT-WRONG MISMATCH" : "")}");
        }
    }
}
