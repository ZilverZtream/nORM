using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// EF Core parity: a derived context can put its mapping in a <c>protected override void
/// OnModelCreating(ModelBuilder)</c> instead of an options lambda — the canonical EF pattern. The override
/// runs during construction; a mapping placed there (here a <c>ToTable</c> rename) must take effect, or the
/// query would target the CLR type name and fail.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OnModelCreatingOverrideTests
{
    // No [Table] / [Key] attributes — the mapping comes solely from OnModelCreating.
    private class Widget
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private class AppDb : DbContext
    {
        public AppDb(SqliteConnection cn)
            : base(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Widget>().ToTable("OmcWidget").HasKey(w => w.Id);
    }

    [Fact]
    public void Derived_context_configures_the_model_via_OnModelCreating_override()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OmcWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO OmcWidget VALUES (1, 'a');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new AppDb(cn);

        // Targets OmcWidget only if the override ran (otherwise it would target "Widget" → no such table).
        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        Assert.Equal("a", w.Name);
    }

    private class BothDb : DbContext
    {
        public BothDb(SqliteConnection cn)
            : base(cn, new SqliteProvider(), new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Widget>().ToTable("OmcWidget")
            }, ownsConnection: false) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Widget>().HasKey(w => w.Id);
    }

    [Fact]
    public void Options_delegate_and_override_both_run()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE OmcWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO OmcWidget VALUES (2, 'b');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new BothDb(cn);   // table from the delegate, key from the override
        var w = ctx.Query<Widget>().First(x => x.Id == 2);
        Assert.Equal("b", w.Name);
    }
}
