using System.Data.Common;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// EF Core parity for the <c>DbSet&lt;TEntity&gt;</c> entry point: <c>context.Set&lt;T&gt;()</c> returns a
/// <see cref="DbSet{T}"/> that both composes LINQ (like <c>Query&lt;T&gt;()</c>) and exposes the change-tracking
/// verbs (Add/Remove/Find). Exposing it as a computed context property (<c>public DbSet&lt;User&gt; Users =&gt;
/// this.Set&lt;User&gt;();</c>) gives EF-style <c>context.Users</c> access with no reflection/auto-population
/// (trimming/NativeAOT-safe). Guards that property-style read and write both flow through the right paths.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DbSetTests
{
    [Table("DbsUser")]
    public sealed class User
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class AppContext : DbContext
    {
        public AppContext(DbConnection cn) : base(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<User>().HasKey(u => u.Id) }) { }

        public DbSet<User> Users => this.Set<User>();
    }

    private static SqliteConnection Db()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE DbsUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task ContextDbSetProperty_SupportsAddQueryFindRemove()
    {
        await using var cn = Db();
        await using var ctx = new AppContext(cn);

        ctx.Users.Add(new User { Id = 1, Name = "Alice" });
        ctx.Users.Add(new User { Id = 2, Name = "Bob" });
        await ctx.SaveChangesAsync();

        var names = await ctx.Users.Where(u => u.Name.StartsWith("A")).Select(u => u.Name).ToListAsync();
        Assert.Equal(new[] { "Alice" }, names);
        Assert.Equal(2, await ctx.Users.CountAsync());

        var bob = await ctx.Users.FindAsync(2);
        Assert.NotNull(bob);
        Assert.Equal("Bob", bob!.Name);

        ctx.Users.Remove(bob);
        await ctx.SaveChangesAsync();
        Assert.Equal(1, await ctx.Users.CountAsync());
    }

    [Fact]
    public async Task Set_ReturnsDbSet_ComposingQueryAndAddBothWork()
    {
        await using var cn = Db();
        await using var ctx = new AppContext(cn);

        DbSet<User> set = ctx.Set<User>();
        set.Add(new User { Id = 5, Name = "Carol" });
        await ctx.SaveChangesAsync();

        Assert.Single(await set.Where(u => u.Id == 5).ToListAsync());
    }
}
