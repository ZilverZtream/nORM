using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression (F1/F2 completion): a RUNTIME (closure) format string / string.Format template is folded
/// and inlined into the SQL. Like the string-match sites, that both risks a MySQL literal breakout (F1) and —
/// because the plan cache excludes closure values — bakes the first caller's format/template into the cached
/// plan and replays it to later callers (F2). These paths (SCV ToString(format), ETSV string.Format) now mark
/// the plan fold-no-cache; this proves distinct runtime formats/templates yield correct per-call results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class FormatClosureValueReuseTests
{
    [Table("FcrRow")]
    public sealed class Row
    {
        [Key] public int Id { get; set; }
        [Column(TypeName = "decimal(18,4)")] public decimal Val { get; set; }
    }

    private static DbContext NewCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE FcrRow (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL);" +
                              "INSERT INTO FcrRow VALUES (1,'3.25'),(2,'7.5');";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Decimal_ToString_projection_with_runtime_format_is_not_cache_poisoned()
    {
        using var ctx = NewCtx();
        string Fmt(string f) => ctx.Query<Row>().Where(r => r.Id == 1).Select(r => r.Val.ToString(f)).First();

        Assert.Equal("3.25", Fmt("F2")); // caches the plan for this format
        Assert.Equal("3", Fmt("F0"));    // a POISONED plan would still format as F2 → "3.25"
        Assert.Equal("3.25", Fmt("F2")); // and a plan poisoned by the F0 call would return "3"
    }

    [Fact]
    public void StringFormat_where_with_runtime_template_is_not_cache_poisoned()
    {
        using var ctx = NewCtx();
        int CountWhere(string tmpl, string target) =>
            ctx.Query<Row>().Count(r => string.Format(tmpl, r.Id) == target);

        Assert.Equal(1, CountWhere("[{0}]", "[1]")); // r.Id=1 → "[1]"; caches the plan with this template
        Assert.Equal(1, CountWhere("({0})", "(1)")); // POISONED plan would still emit "[1]" → "(1)" != "[1]" → 0
        Assert.Equal(0, CountWhere("[{0}]", "(1)")); // "[1]" != "(1)"
    }
}
