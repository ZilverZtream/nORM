using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins compiled-query behavior when the user attempts to parameterize the
/// ORDER BY direction via a ternary in the compiled lambda body, e.g.
/// <c>(ctx, asc) =&gt; asc ? ctx.Query&lt;T&gt;().OrderBy(k) : ctx.Query&lt;T&gt;().OrderByDescending(k)</c>.
///
/// This is a real silent-wrongness shape in any ORM that bakes SQL: the
/// compiled SQL is decided ONCE at compile-call time, so the ternary on the
/// closure-captured <c>asc</c> resolves only at compile time. Both branches
/// have valid expression trees but the translator can only commit to one.
///
/// Expected outcome (good): throw a clear actionable error pointing the user
/// at the two-delegate workaround (CompileQuery one asc and one desc).
/// Silent-wrongness (bad): swallow one branch and return the same ordering
/// regardless of the param value at call time.
///
/// First test asserts the behavior; if the assertion fails because the ORM
/// silently bakes one branch, that surfaces the bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCompiledQueryConditionalOrderByTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CqcItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO CqcItem VALUES (1, 'A'), (2, 'B'), (3, 'C');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CqcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public void Compiled_query_with_ternary_orderby_direction_throws_actionable_error()
    {
        // Calling CompileQuery with a ternary that selects between two query shapes
        // should fail-fast with an actionable nORM error -- not a cryptic BCL
        // ArgumentException("Argument types do not match") from inside the expression
        // visitor, and not silent baking of one branch.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
        {
            _ = Norm.CompileQuery((DbContext ctx, bool asc) =>
                asc
                    ? ctx.Query<CqcItem>().OrderBy(i => i.Id)
                    : ctx.Query<CqcItem>().OrderByDescending(i => i.Id));
        });

        // Error message should point at the two-delegate workaround so the user
        // can fix without spelunking the source.
        Assert.Contains("OrderByDescending", ex.Message);
        Assert.Contains("Workaround", ex.Message);
        Assert.Contains("compile each branch", ex.Message);
    }

    [Fact]
    public async Task Compiled_query_with_two_separate_delegates_works_around_ternary_limitation()
    {
        // Positive pin: the documented workaround actually produces correct
        // results for both directions.
        var asc = Norm.CompileQuery((DbContext ctx, int _) =>
            ctx.Query<CqcItem>().OrderBy(i => i.Id));
        var desc = Norm.CompileQuery((DbContext ctx, int _) =>
            ctx.Query<CqcItem>().OrderByDescending(i => i.Id));

        var ascResult = await asc(_ctx, 0);
        var descResult = await desc(_ctx, 0);

        Assert.Equal(new[] { 1, 2, 3 }, ascResult.Select(i => i.Id).ToList());
        Assert.Equal(new[] { 3, 2, 1 }, descResult.Select(i => i.Id).ToList());
    }

    [Table("CqcItem")]
    public sealed class CqcItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
