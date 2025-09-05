using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public class SqliteMigrationRunnerTests
{
    [Fact]
    public async Task ApplyMigrations_ExecutesMigrationsAndTracksHistory()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var runner = new SqliteMigrationRunner(connection, typeof(SqliteMigrationRunnerTests).Assembly);

        Assert.True(await runner.HasPendingMigrationsAsync());

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(new[] { "1_CreateBlogTable", "2_AddPostsTable" }, pending);

        await runner.ApplyMigrationsAsync();

        Assert.False(await runner.HasPendingMigrationsAsync());

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='Blog';";
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal("Blog", result);
        }

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\";";
            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
            Assert.Equal(2L, count);
        }
    }

    private class CreateBlogTable : nORM.Migration.Migration
    {
        public CreateBlogTable() : base(1, nameof(CreateBlogTable)) { }

        public override void Up(DbConnection connection, DbTransaction transaction)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "CREATE TABLE Blog (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }

        public override void Down(DbConnection connection, DbTransaction transaction)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DROP TABLE Blog;";
            cmd.ExecuteNonQuery();
        }
    }

    private class AddPostsTable : nORM.Migration.Migration
    {
        public AddPostsTable() : base(2, nameof(AddPostsTable)) { }

        public override void Up(DbConnection connection, DbTransaction transaction)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "CREATE TABLE Posts (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, FOREIGN KEY(BlogId) REFERENCES Blog(Id));";
            cmd.ExecuteNonQuery();
        }

        public override void Down(DbConnection connection, DbTransaction transaction)
        {
            using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "DROP TABLE Posts;";
            cmd.ExecuteNonQuery();
        }
    }
}

