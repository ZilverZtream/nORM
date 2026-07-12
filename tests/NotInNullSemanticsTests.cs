using System;
using System.Collections.Generic;
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
/// A negated <c>Contains</c> over a nullable column must return the same rows LINQ-to-Objects
/// would. SQL <c>col NOT IN (...)</c> is three-valued: a NULL column yields UNKNOWN (row dropped),
/// and a NULL anywhere in the list makes the whole <c>NOT IN</c> UNKNOWN for every row. C# instead
/// treats <c>!list.Contains(nullCol)</c> as true when the list has no matching null. Getting this
/// wrong silently drops rows from a <c>NOT IN</c> filter — a classic ORM footgun.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NotInNullSemanticsTests
{
    [Table("NcIn")]
    private class NcIn
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? Value { get; set; }
    }

    // Row 1: Value=5   Row 2: Value=7   Row 3: Value=NULL
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NcIn (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value INTEGER);" +
                "INSERT INTO NcIn (Value) VALUES (5),(7),(NULL);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly NcIn[] Reference =
    {
        new NcIn { Id = 1, Value = 5 },
        new NcIn { Id = 2, Value = 7 },
        new NcIn { Id = 3, Value = null },
    };

    private static void AssertMatchesLinq(Func<IQueryable<NcIn>, IQueryable<NcIn>> apply)
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = apply(Reference.AsQueryable()).Select(x => x.Id).OrderBy(i => i).ToList();
        var actual = apply(ctx.Query<NcIn>()).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact] public void Contains_list_without_null_matches_linq()
    {
        var list = new int?[] { 5, 7 };
        AssertMatchesLinq(q => q.Where(x => list.Contains(x.Value)));
    }

    [Fact] public void Negated_contains_list_without_null_includes_null_row()
    {
        // C#: {3} — the NULL row is not in {5,7}, so !Contains is true.
        var list = new int?[] { 5, 7 };
        AssertMatchesLinq(q => q.Where(x => !list.Contains(x.Value)));
    }

    [Fact] public void Contains_list_with_null_matches_linq()
    {
        // C#: {1,3} — Contains matches 5 and (null == null).
        var list = new int?[] { 5, null };
        AssertMatchesLinq(q => q.Where(x => list.Contains(x.Value)));
    }

    [Fact] public void Negated_contains_list_with_null_matches_linq()
    {
        // C#: {2} — 7 is not in {5,null}; 5 and NULL are.
        var list = new int?[] { 5, null };
        AssertMatchesLinq(q => q.Where(x => !list.Contains(x.Value)));
    }

    [Fact] public void Negated_contains_empty_list_includes_all_rows()
    {
        // C#: {1,2,3} — nothing is in an empty list, so !Contains is true for every row.
        var list = new int?[] { };
        AssertMatchesLinq(q => q.Where(x => !list.Contains(x.Value)));
    }

    [Fact] public void Negated_contains_all_null_list_matches_linq()
    {
        // C#: {1,2} — only the NULL row is "in" an all-null list, so !Contains excludes it.
        var list = new int?[] { null };
        AssertMatchesLinq(q => q.Where(x => !list.Contains(x.Value)));
    }
}
