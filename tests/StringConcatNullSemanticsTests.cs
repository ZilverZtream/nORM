using System;
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
/// C# string concatenation treats a null operand as an empty string (String.Concat
/// semantics): <c>null + "!"</c> is <c>"!"</c>, never null. SQLite's <c>||</c> and
/// MySQL's <c>CONCAT</c> propagate NULL instead, so a bare translation returns NULL
/// projections, drops rows from predicates, and writes NULL back through
/// ExecuteUpdate SET.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class StringConcatNullSemanticsTests
{
    [Table("ConcatNull_Person")]
    private class Person
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string? Nick { get; set; }
        public string? Suffix { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ConcatNull_Person (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Nick TEXT NULL,
                    Suffix TEXT NULL
                );
                INSERT INTO ConcatNull_Person (Nick, Suffix) VALUES ('ann', '!');
                INSERT INTO ConcatNull_Person (Nick, Suffix) VALUES (NULL, '!');
                INSERT INTO ConcatNull_Person (Nick, Suffix) VALUES ('bob', NULL);
                INSERT INTO ConcatNull_Person (Nick, Suffix) VALUES (NULL, NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Projection_treats_null_operand_as_empty()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Person>().OrderBy(p => p.Id).Select(p => p.Nick + "*").ToList();
        Assert.Equal(new[] { "ann*", "*", "bob*", "*" }, actual);
    }

    [Fact]
    public void Projection_of_two_nullable_columns_treats_null_as_empty()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Person>().OrderBy(p => p.Id).Select(p => p.Nick + p.Suffix).ToList();
        Assert.Equal(new[] { "ann!", "!", "bob", "" }, actual);
    }

    [Fact]
    public void Predicate_matches_row_whose_null_operand_concats_to_the_target()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var ids = ctx.Query<Person>().Where(p => p.Nick + "*" == "*").Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Fact]
    public void Static_string_concat_treats_null_operand_as_empty()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var actual = ctx.Query<Person>().OrderBy(p => p.Id)
            .Select(p => string.Concat(p.Nick, "-", p.Suffix)).ToList();
        Assert.Equal(new[] { "ann-!", "-!", "bob-", "-" }, actual);
    }

    [Fact]
    public async Task Execute_update_concat_writes_empty_not_null_for_null_operand()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        await ctx.Query<Person>().ExecuteUpdateAsync(s => s.SetProperty(p => p.Nick, p => p.Nick + "*"));

        using var verify = new DbContext(cn, new SqliteProvider());
        var nicks = verify.Query<Person>().OrderBy(p => p.Id).Select(p => p.Nick).ToList();
        Assert.Equal(new[] { "ann*", "*", "bob*", "*" }, nicks);
    }
}
