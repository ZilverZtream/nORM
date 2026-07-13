using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The simple-query fast path handles First/FirstOrDefault WITH a predicate by folding
/// the predicate into the same slot a single Where uses — First(source, predicate) is
/// Where(source, predicate).First(). These tests pin the filter semantics for every
/// predicate shape the fast path accepts, verify the fast path actually serves the
/// query (via its SQL cache), and that two-predicate chains still work through the
/// full translator.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SimpleFastPathFirstPredicateTests
{
    [Table("FastFirst_User")]
    private class User
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool Active { get; set; }
        public string? Nick { get; set; }
    }

    private static (SqliteConnection, DbContext) Setup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FastFirst_User (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Active INTEGER NOT NULL,
                    Nick TEXT NULL
                );
                INSERT INTO FastFirst_User (Name, Active, Nick) VALUES
                    ('ann', 1, 'a'),
                    ('bob', 0, NULL),
                    ('cid', 1, 'c');
                """;
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void First_with_equality_predicate_filters()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var row = ctx.Query<User>().First(u => u.Name == "bob");
        Assert.Equal(2, row.Id);
    }

    [Fact]
    public void FirstOrDefault_with_equality_predicate_returns_null_when_no_match()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        Assert.Null(ctx.Query<User>().FirstOrDefault(u => u.Name == "nobody"));
    }

    [Fact]
    public void First_with_boolean_member_predicate_filters()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var row = ctx.Query<User>().First(u => !u.Active);
        Assert.Equal(2, row.Id);
    }

    [Fact]
    public void First_with_null_comparison_predicate_filters()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var row = ctx.Query<User>().First(u => u.Nick == null);
        Assert.Equal(2, row.Id);
    }

    [Fact]
    public void First_predicate_throws_when_no_match()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        Assert.Throws<InvalidOperationException>(() => ctx.Query<User>().First(u => u.Name == "nobody"));
    }

    [Fact]
    public void Where_then_first_predicate_still_filters_with_both_predicates()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        // Two predicates exceed the fast path's single slot — the full translator
        // must apply BOTH (a dropped one would return ann or bob).
        var row = ctx.Query<User>().Where(u => u.Active).First(u => u.Name == "cid");
        Assert.Equal(3, row.Id);
    }

    [Fact]
    public void First_predicate_is_served_by_the_simple_fast_path()
    {
        var (cn, ctx) = Setup();
        using var _ = cn; using var __ = ctx;

        var warm = ctx.Query<User>().First(u => u.Name == "ann");
        Assert.Equal(1, warm.Id);

        // The fast path caches its SQL under a SIMPLE: key that embeds the result
        // operator and predicate shape; its presence proves this query shape did
        // not fall through to the full translator.
        var provider = ctx.Query<User>().Provider;
        var cacheField = provider.GetType().GetField("_simpleSqlCache", BindingFlags.NonPublic | BindingFlags.Instance)
                         ?? provider.GetType().GetField("_simpleSqlCache", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(cacheField);
        var cache = (System.Collections.IDictionary?)cacheField!.GetValue(provider) ?? (System.Collections.IDictionary?)cacheField.GetValue(null);
        Assert.NotNull(cache);
        var hit = cache!.Keys.Cast<string>().Any(k => k.Contains(":First:") && k.Contains(nameof(User)));
        Assert.True(hit, "expected a SIMPLE:...:First:... cache entry for the predicate overload");
    }
}
