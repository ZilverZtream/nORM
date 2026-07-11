using System;
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
/// Global filters must hold on every execution route. The fast-path executor
/// bails out whenever filters or a tenant provider are configured, and the
/// compiled-query context key embeds each filter's shape and captured values —
/// these tests pin both properties so an optimization can never reintroduce a
/// filter bypass.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterExecutionPathSafetyTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("GfepItem")]
    private class GfepItem
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx, DbContextOptions Opts) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GfepItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Value INTEGER NOT NULL);" +
                "INSERT INTO GfepItem (Name, Value) VALUES ('visible', 10);" +
                "INSERT INTO GfepItem (Name, Value) VALUES ('hidden', -5);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GfepItem>()
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts), opts);
    }

    [Fact]
    public async Task Fast_path_eligible_query_shapes_still_apply_global_filters()
    {
        var (cn, ctx, opts) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        opts.AddGlobalFilter<GfepItem>(e => e.Value > 0);

        // Simple Take / simple Count / unfiltered ToList are exactly the shapes
        // the fast-path executor intercepts when no filters exist.
        var all = await ctx.Query<GfepItem>().ToListAsync();
        Assert.Single(all);
        Assert.Equal("visible", all[0].Name);

        Assert.Equal(1, await ctx.Query<GfepItem>().CountAsync());

        var taken = await ctx.Query<GfepItem>().Take(5).ToListAsync();
        Assert.Single(taken);
    }

    private static readonly Func<DbContext, int, Task<System.Collections.Generic.List<GfepItem>>> _compiledAbove =
        Norm.CompileQuery((DbContext c, int min) =>
            c.Query<GfepItem>().Where(x => x.Value >= min).OrderBy(x => x.Id));

    [Fact]
    public async Task Global_filter_registered_after_compiled_query_execution_applies_to_subsequent_calls()
    {
        var (cn, ctx, opts) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // First execution compiles and caches a plan for this context shape
        // WITHOUT any global filter.
        var before = await _compiledAbove(ctx, -100);
        Assert.Equal(2, before.Count);

        // Registering a filter changes the context plan key, so the next call
        // must translate a fresh plan that includes the filter — reusing the
        // cached unfiltered plan would silently bypass it.
        opts.AddGlobalFilter<GfepItem>(e => e.Value > 0);

        var after = await _compiledAbove(ctx, -100);
        var row = Assert.Single(after);
        Assert.Equal("visible", row.Name);
    }
}
