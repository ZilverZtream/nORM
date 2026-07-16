using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// Pins the compiled-query x AsOf boundary: a compiled delegate whose AsOf timestamp is a
/// DELEGATE PARAMETER fails deterministically ("AsOf() requires a constant DateTime or
/// string tag") instead of silently baking the first call's timestamp. This is the right
/// semantics — every distinct AsOf timestamp owns its query plan (the timestamp is hashed
/// into the plan fingerprint), so a parameterized compiled AsOf would defeat compiled-plan
/// reuse anyway. Capture the timestamp at the call site and build the query uncompiled.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CompiledQueryAsOfBoundaryContractTests
{
    [Table("CqaRow_Test")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    private static readonly Func<DbContext, DateTime, Task<List<Row>>> CompiledAsOf =
        Norm.CompileQuery((DbContext c, DateTime ts) => c.Query<Row>().AsOf(ts).Where(r => r.Id == 1));

    [Fact]
    public async Task Compiled_delegate_with_as_of_parameter_probe()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CqaRow_Test (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        var row = new Row { Id = 1, V = 1 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();
        await Task.Delay(60);

        row.V = 2;
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t2 = await ServerNowAsync();

        // The AsOf timestamp is a DELEGATE PARAMETER here — not an expression constant —
        // so the translation must fail loud instead of silently baking one call's value.
        var ex = await Assert.ThrowsAsync<NormQueryException>(() => CompiledAsOf(ctx, t1));
        Assert.Contains("AsOf() requires a constant", ex.Message, StringComparison.Ordinal);

        // The uncompiled equivalent with a captured timestamp reconstructs correctly per call.
        var at1 = await ctx.Query<Row>().AsOf(t1).Where(r => r.Id == 1).ToListAsync();
        Assert.Equal(1, at1.Single().V);
        var at2 = await ctx.Query<Row>().AsOf(t2).Where(r => r.Id == 1).ToListAsync();
        Assert.Equal(2, at2.Single().V);
    }
}
