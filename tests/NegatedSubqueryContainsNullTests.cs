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
/// A negated subquery Contains — Where(p => !ctx.Query&lt;R&gt;().Select(o => o.RefId).Contains(p.Id)) — must be an
/// anti-join with correct 3-valued-logic. nORM emitted a bare NOT(x IN (SELECT ...)); SQL `NOT (x IN (…, NULL))`
/// is UNKNOWN for a non-matching x, so a NULL anywhere in the subquery result silently dropped EVERY row, and
/// a nullable outer column dropped its NULL rows — while C# `!list.Contains(x)` includes them. The correct
/// lowering is NOT EXISTS with a null-safe equality (matching EF Core and LINQ-to-Objects).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NegatedSubqueryContainsNullTests
{
    [Table("NscLeft")]
    public class Left
    {
        [Key] public int Id { get; set; }
        public int? Val { get; set; }
    }

    [Table("NscRight")]
    public class Right
    {
        [Key] public int Id { get; set; }
        public int? RefId { get; set; }
    }

    private static (SqliteConnection, DbContext) Create(string rightRows)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NscLeft (Id INTEGER PRIMARY KEY, Val INTEGER NULL);" +
                "CREATE TABLE NscRight (Id INTEGER PRIMARY KEY, RefId INTEGER NULL);" +
                "INSERT INTO NscLeft VALUES (1,10),(2,20),(3,30),(4,40),(5,NULL);" +
                rightRows;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Manifestation_A_null_in_subquery_result_non_null_outer_key()
    {
        // Subquery RefId set = {2,4,NULL}. Outer key p.Id is the non-null PK (1..5).
        var (cn, ctx) = Create("INSERT INTO NscRight VALUES (100,2),(101,4),(102,NULL);");
        using var _cn = cn; using var _ctx = ctx;
        var c = ctx;

        var ids = ctx.Query<Left>()
            .Where(p => !c.Query<Right>().Select(o => o.RefId).Contains(p.Id))
            .OrderBy(p => p.Id)
            .ToList().Select(p => p.Id).ToArray();

        // !{2,4,null}.Contains(id): true for ids NOT in {2,4} -> 1,3,5. The subquery NULL must not drop them.
        Assert.Equal(new[] { 1, 3, 5 }, ids);
    }

    [Fact]
    public void Manifestation_B_nullable_outer_key_subquery_without_null()
    {
        // Subquery RefId set = {20,40} (NO null). Outer p.Val is nullable and row 5 is NULL.
        var (cn, ctx) = Create("INSERT INTO NscRight VALUES (100,20),(101,40);");
        using var _cn = cn; using var _ctx = ctx;
        var c = ctx;

        var ids = ctx.Query<Left>()
            .Where(p => !c.Query<Right>().Select(o => o.RefId).Contains(p.Val))
            .OrderBy(p => p.Id)
            .ToList().Select(p => p.Id).ToArray();

        // !{20,40}.Contains(v): Vals 10(1),30(3),NULL(5) are not in {20,40} -> included. The NULL Val must not drop row 5.
        Assert.Equal(new[] { 1, 3, 5 }, ids);
    }

    [Fact]
    public void Manifestation_C_null_outer_value_and_null_in_subquery_excludes_row()
    {
        // The case that REQUIRES null-safe equality (a plain `col = @x` inside NOT EXISTS would leave the
        // NULL outer value matching nothing via UNKNOWN-excluded WHERE and wrongly INCLUDE row 5).
        // Subquery RefId set = {20,40,NULL}; outer p.Val row 5 is NULL. C# !{20,40,null}.Contains(null) is
        // false -> row 5 EXCLUDED. Vals 10(1),30(3) are not in the set -> included; 20(2),40(4) excluded.
        var (cn, ctx) = Create("INSERT INTO NscRight VALUES (100,20),(101,40),(102,NULL);");
        using var _cn = cn; using var _ctx = ctx;
        var c = ctx;

        var ids = ctx.Query<Left>()
            .Where(p => !c.Query<Right>().Select(o => o.RefId).Contains(p.Val))
            .OrderBy(p => p.Id)
            .ToList().Select(p => p.Id).ToArray();

        Assert.Equal(new[] { 1, 3 }, ids);   // row 5 (NULL) is contained (subquery has NULL) -> negation excludes it
    }

    [Fact]
    public void Positive_subquery_contains_with_null_in_result_stays_correct()
    {
        // Control: positive path unaffected. Subquery {20,40,NULL}; Val=NULL(row 5) IS "contained" (null in set).
        var (cn, ctx) = Create("INSERT INTO NscRight VALUES (100,20),(101,40),(102,NULL);");
        using var _cn = cn; using var _ctx = ctx;
        var c = ctx;

        var ids = ctx.Query<Left>()
            .Where(p => c.Query<Right>().Select(o => o.RefId).Contains(p.Val))
            .OrderBy(p => p.Id)
            .ToList().Select(p => p.Id).ToArray();

        Assert.Equal(new[] { 2, 4, 5 }, ids);   // 20,40 match; NULL matches the subquery's NULL
    }
}
