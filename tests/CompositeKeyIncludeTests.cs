using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that Include on a composite-PK dependent entity throws NotSupportedException
/// rather than silently corrupting data.
/// </summary>
public class CompositeKeyIncludeTests
{
    private class Blog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public ICollection<OrderLine> OrderLines { get; set; } = new List<OrderLine>();
    }

    private class OrderLine
    {
        public int BlogId { get; set; }
        public int LineNumber { get; set; }
        public string Description { get; set; } = string.Empty;
        public Blog? Blog { get; set; }
    }

    private static DbContextOptions BuildOptions() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<OrderLine>().HasKey(x => new { x.BlogId, x.LineNumber });
            mb.Entity<Blog>()
                .HasMany(b => b.OrderLines)
                .WithOne(o => o.Blog)
                .HasForeignKey(o => o.BlogId, b => b.Id);
        }
    };

    [Fact]
    public async Task Include_CompositeKeyDependent_ThrowsNotSupportedException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Blog(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OrderLine(BlogId INTEGER NOT NULL, LineNumber INTEGER NOT NULL, Description TEXT NOT NULL, PRIMARY KEY(BlogId, LineNumber));";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());

        // Insert a parent row so EagerLoad doesn't short-circuit on empty parents list.
        var blog = new Blog { Title = "Test" };
        ctx.Add(blog);
        await ctx.SaveChangesAsync();

        // Include on a composite-PK dependent must throw NotSupportedException.
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.OrderLines).ToListAsync());
    }

    [Fact]
    public async Task Include_CompositeKeyDependent_ErrorMessage_MentionsEntityAndGuidance()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Blog(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OrderLine(BlogId INTEGER NOT NULL, LineNumber INTEGER NOT NULL, Description TEXT NOT NULL, PRIMARY KEY(BlogId, LineNumber));";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());

        var blog = new Blog { Title = "Test" };
        ctx.Add(blog);
        await ctx.SaveChangesAsync();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.OrderLines).ToListAsync());

        Assert.Contains("OrderLine", ex.Message);
        Assert.Contains("composite primary key", ex.Message);
    }

    [Fact]
    public void Include_CompositeKeyDependent_Sync_ThrowsNotSupportedException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Blog(Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OrderLine(BlogId INTEGER NOT NULL, LineNumber INTEGER NOT NULL, Description TEXT NOT NULL, PRIMARY KEY(BlogId, LineNumber));";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), BuildOptions());

        // Insert a parent row so EagerLoad doesn't short-circuit on empty parents.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO Blog(Title) VALUES('test');";
            cmd.ExecuteNonQuery();
        }

        Assert.Throws<NotSupportedException>(() =>
            ((INormQueryable<Blog>)ctx.Query<Blog>()).Include(b => b.OrderLines).ToList());
    }
}
