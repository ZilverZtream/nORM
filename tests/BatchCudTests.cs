using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class BatchCudTests
{
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
    public async Task ExecuteDeleteAsync_rejects_paged_query_shape()
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

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<User>().Where(u => !u.Archived).Take(1).ExecuteDeleteAsync());

        Assert.Contains("paged", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_rejects_ordered_query_shape()
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

        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<User>()
                .Where(u => !u.Archived)
                .OrderBy(u => u.Name)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, true)));

        Assert.Contains("ordered", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static string ThrowingName()
        => throw new InvalidOperationException("The set value expression was invoked.");
}
