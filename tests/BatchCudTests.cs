using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class BatchCudTests
{
    [Xunit.Trait("Category", "Fast")]
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Archived { get; set; }
    }

    [Fact]
    public async Task ExecuteDeleteAsync_deletes_rows_matching_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);" +
                             "INSERT INTO User VALUES(2,'B',0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        await ctx.Query<User>().Where(u => u.Name == "A").ExecuteDeleteAsync();
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM User";
        var remaining = Convert.ToInt64(check.ExecuteScalar());
        Assert.Equal(1, remaining);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_updates_rows_matching_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);" +
                             "INSERT INTO User VALUES(2,'B',0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        await ctx.Query<User>()
            .Where(u => u.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, true));
        var users = await ctx.Query<User>().ToListAsync();
        Assert.True(users.Single(u => u.Id == 1).Archived);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_uses_captured_set_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);" +
                             "INSERT INTO User VALUES(2,'B',0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var archived = true;

        await ctx.Query<User>()
            .Where(u => u.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, archived));

        var users = await ctx.Query<User>().ToListAsync();
        Assert.True(users.Single(u => u.Id == 1).Archived);
        Assert.False(users.Single(u => u.Id == 2).Archived);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_rejects_method_set_value_without_invoking_it()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<User>()
                .Where(u => u.Id == 1)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, ThrowingName())));

        Assert.Contains("set values", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_inline_string_concat_of_captured_locals_rejected_with_actionable_message()
    {
        // Inline string concat of two captured locals is a BinaryExpression — not a literal
        // value and not a lambda. The translator can't reduce it to either form. Callers
        // should pre-compute (assign to a local first) or use the lambda overload.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        var prefix = "B";
        var suffix = "C";

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<User>()
                .Where(u => u.Id == 1)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, prefix + suffix)));

        Assert.Contains("literal/captured constant", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_rejects_paged_query_on_keyless_entity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE KeylessAudit(Code INTEGER, Archived INTEGER);" +
                             "INSERT INTO KeylessAudit VALUES(1,0);" +
                             "INSERT INTO KeylessAudit VALUES(2,0);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // A paged window resolves its target rows through a keyed subquery; an
        // entity with no key columns has no row identity to resolve against.
        // Genuinely keyless: no [Key] and no name the EF-parity key convention promotes.
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<KeylessAudit>().Where(a => !a.Archived).Take(1).ExecuteDeleteAsync());

        Assert.Contains("key columns", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_with_ordering_updates_the_filtered_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);" +
                             "INSERT INTO User VALUES(2,'B',0);" +
                             "INSERT INTO User VALUES(3,'C',1);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Ordering without paging never changes the row set, so the update runs
        // against the plain filter.
        var affected = await ctx.Query<User>()
            .Where(u => !u.Archived)
            .OrderBy(u => u.Name)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, true));
        Assert.Equal(2, affected);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM User WHERE Archived = 1";
        Assert.Equal(3L, Convert.ToInt64(check.ExecuteScalar()));
    }

    [Fact]
    public async Task ExecuteDeleteAsync_with_distinct_deletes_the_filtered_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE User(Id INTEGER, Name TEXT, Archived INTEGER);" +
                             "INSERT INTO User VALUES(1,'A',0);" +
                             "INSERT INTO User VALUES(2,'B',0);" +
                             "INSERT INTO User VALUES(3,'C',1);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Distinct over entity rows is inert for the row set, so the delete runs
        // against the plain filter.
        var affected = await ctx.Query<User>().Where(u => !u.Archived).Distinct().ExecuteDeleteAsync();
        Assert.Equal(2, affected);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM User";
        Assert.Equal(1L, Convert.ToInt64(check.ExecuteScalar()));
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("KeylessAudit")]
    private class KeylessAudit
    {
        public int Code { get; set; }
        public bool Archived { get; set; }
    }

    private static string ThrowingName()
        => throw new InvalidOperationException("The set value expression was invoked.");
}
