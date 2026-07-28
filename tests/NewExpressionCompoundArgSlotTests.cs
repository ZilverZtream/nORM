using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The NewExpression constant-fold (new DateTime(y, m, d) == x.When) reserved one _unused compiled-param slot
/// per MemberExpression ARGUMENT — but a COMPOUND argument (new DateTime(baseYear + 1, mo, da)) is a
/// BinaryExpression, not a MemberExpression, so it reserved zero slots while the extractor still collected the
/// closure member(s) inside it. The value list then outran the compiled-param slots and a following captured
/// scalar (&amp;&amp; x.Name == name) bound to the wrong slot — silent wrong/empty rows. The reservation must count
/// per closure member, not per argument.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NewExpressionCompoundArgSlotTests
{
    [Table("NecEvent")]
    public class Ev
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime When { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NecEvent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, [When] TEXT NOT NULL);" +
                "INSERT INTO NecEvent VALUES (1, 'Ann', '2020-01-15 00:00:00'), " +
                "(2, 'Eve', '2020-01-15 00:00:00'), (3, 'Eve', '2021-01-15 00:00:00');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void New_datetime_with_compound_arg_before_captured_scalar_binds_correct_slots()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var baseYear = 2019;   // baseYear + 1 == 2020 (compound arg)
        var mo = 1; var da = 15;
        var name = "Eve";

        var ids = ctx.Query<Ev>()
            .Where(x => x.When == new DateTime(baseYear + 1, mo, da) && x.Name == name)
            .ToList().Select(x => x.Id).OrderBy(i => i).ToArray();

        // When == 2020-01-15 matches Ann(1) & Eve(2); && Name == "Eve" narrows to Eve (id 2).
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void New_datetime_all_simple_args_still_works()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        var y = 2020; var mo = 1; var da = 15;
        var name = "Eve";

        var ids = ctx.Query<Ev>()
            .Where(x => x.When == new DateTime(y, mo, da) && x.Name == name)
            .ToList().Select(x => x.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 2 }, ids);
    }
}
