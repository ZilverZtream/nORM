using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// FromSqlRaw over an entity with a value-converter column. Materialization runs the column's
/// ConvertFromProvider (the row is read through the ordinary entity materializer), a composed
/// equality predicate binds the constant's PROVIDER representation, and a composed projection of the
/// column converts the scalar. Range comparisons and ORDER BY on a converter column operate on the
/// STORED representation — correct only for an order-preserving converter, the same documented caveat
/// EF Core carries — so those are pinned to be CONSISTENT with the normal ctx.Query path rather than to
/// a CLR-semantics oracle. A NegatingConverter (stored = -model) makes an unconverted result or a
/// stored-form ordering visible.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class FromSqlRawConverterColumnTests
{
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -(Convert.ToInt32(v));
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("FsrcWidget")]
    public sealed class Widget
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Score { get; set; } // stored negated via converter
        public string Name { get; set; } = "";
    }

    private static readonly (int id, int score, string name)[] Rows =
        { (1, 3, "a"), (2, 7, "b"), (3, 5, "c") };

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FsrcWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Score).HasConversion(new NegatingConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        foreach (var r in Rows) await ctx.InsertAsync(new Widget { Id = r.id, Score = r.score, Name = r.name });
        return ctx;
    }

    [Fact]
    public async Task Materializes_converter_column_as_model_value()
    {
        using var ctx = await CtxAsync();
        var scores = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrcWidget").ToList()
            .OrderBy(w => w.Id).Select(w => w.Score).ToList();
        Assert.Equal(new[] { 3, 7, 5 }, scores); // ConvertFromProvider applied — not the stored -3,-7,-5
    }

    [Fact]
    public async Task Composed_equality_on_converter_column_binds_stored_representation()
    {
        using var ctx = await CtxAsync();
        var ids = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrcWidget").Where(w => w.Score == 3).ToList()
            .Select(w => w.Id).ToList();
        Assert.Equal(new[] { 1 }, ids); // model 3 -> stored -3 matches row 1 (not "matches nothing")
    }

    [Fact]
    public async Task Composed_projection_of_converter_column_converts()
    {
        using var ctx = await CtxAsync();
        var scores = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrcWidget").OrderBy(w => w.Id)
            .Select(w => w.Score).ToList();
        Assert.Equal(new[] { 3, 7, 5 }, scores);
    }

    [Fact]
    public async Task Range_and_ordering_on_converter_match_the_normal_query_path()
    {
        using var ctx = await CtxAsync();

        List<int> Raw(Func<IQueryable<Widget>, IQueryable<Widget>> q) =>
            q(ctx.FromSqlRaw<Widget>("SELECT * FROM FsrcWidget")).ToList().Select(w => w.Id).ToList();
        List<int> Normal(Func<IQueryable<Widget>, IQueryable<Widget>> q) =>
            q(ctx.Query<Widget>()).ToList().Select(w => w.Id).ToList();

        // Range and ORDER BY run on the stored column; FromSqlRaw must not diverge from ctx.Query.
        Assert.Equal(Normal(q => q.Where(w => w.Score > 4).OrderBy(w => w.Id)),
                     Raw(q => q.Where(w => w.Score > 4).OrderBy(w => w.Id)));
        Assert.Equal(Normal(q => q.Where(w => w.Score < 6).OrderBy(w => w.Id)),
                     Raw(q => q.Where(w => w.Score < 6).OrderBy(w => w.Id)));
        Assert.Equal(Normal(q => q.OrderBy(w => w.Score)),
                     Raw(q => q.OrderBy(w => w.Score)));
        Assert.Equal(Normal(q => q.OrderByDescending(w => w.Score)),
                     Raw(q => q.OrderByDescending(w => w.Score)));
    }
}
