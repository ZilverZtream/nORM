using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a top-level <c>OrderBy</c> applied to a set-operation result (<c>Union</c>/<c>Concat</c>/
/// <c>Intersect</c>/<c>Except</c>) references the ordering column by its MAPPED name, honouring a fluent
/// <c>HasColumnName</c> rename. The set-op ORDER BY emit rendered the key from the CLR property name, while
/// the unioned SELECT aliases each column to its mapped name (<c>col AS col</c>) — so ordering a full-entity
/// union by a fluently-renamed column produced <c>ORDER BY &lt;PropName&gt;</c>, a column absent from the
/// result set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOperationOrderByRenamedColumnTests
{
    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Table name matches the CLR type (no rename there); only the "Value" column is renamed
            // to "so_val" via the fluent HasColumnName below — that is what the ORDER BY must honour.
            cmd.CommandText = """
                CREATE TABLE SoRow (Id INTEGER PRIMARY KEY, so_val INTEGER NOT NULL);
                INSERT INTO SoRow VALUES (1,40),(2,10),(3,30),(4,20);
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SoRow>().HasKey(r => r.Id);
                mb.Entity<SoRow>().Property(r => r.Value).HasColumnName("so_val");
            }
        });
    }

    [Fact]
    public void Entity_union_orders_by_the_renamed_column()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        var rows = ctx.Query<SoRow>().Where(x => x.Id <= 3)
            .Union(ctx.Query<SoRow>().Where(x => x.Id >= 2))
            .OrderBy(x => x.Value)
            .ToList();

        // Union of {1,2,3} and {2,3,4} = {1,2,3,4}; ordered by Value asc (10,20,30,40) → Ids 2,4,3,1.
        Assert.Equal(new[] { 2, 4, 3, 1 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 10, 20, 30, 40 }, rows.Select(r => r.Value).ToArray());
    }

    [Fact]
    public void Projection_union_orders_by_the_projected_member_alias()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;

        // A projection set op aliases its output by the anonymous member name (`so_val AS Amount`),
        // so the ORDER BY must use that member name — NOT the mapped column. This guards the
        // member-name branch of the same fix (the key member is declared on the anonymous type).
        var rows = ctx.Query<SoRow>().Where(x => x.Id <= 3).Select(x => new { x.Id, Amount = x.Value })
            .Union(ctx.Query<SoRow>().Where(x => x.Id >= 2).Select(x => new { x.Id, Amount = x.Value }))
            .OrderBy(r => r.Amount)
            .ToList();

        Assert.Equal(new[] { 2, 4, 3, 1 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 10, 20, 30, 40 }, rows.Select(r => r.Amount).ToArray());
    }

    private sealed class SoRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
    }
}
