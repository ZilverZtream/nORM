using System;
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
/// EF Core parity: <c>Property(x => x.Col).IsRowVersion()</c> marks a column as the entity's
/// optimistic-concurrency token (the fluent equivalent of <c>[Timestamp]</c>). nORM manages the token and
/// includes it in the UPDATE concurrency check, so a stale write — one made after another writer changed the
/// row-version — is rejected with <see cref="DbConcurrencyException"/> instead of silently overwriting.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PropertyBuilderIsRowVersionTests
{
    [Table("RvDoc")]
    public class Doc
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public byte[] Version { get; set; } = Array.Empty<byte>();
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool rowVersion)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE RvDoc (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Version BLOB NOT NULL DEFAULT x'');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Doc>().HasKey(d => d.Id);
            if (rowVersion)
                mb.Entity<Doc>().Property(d => d.Version).IsRowVersion();
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static void ExternallyChangeRowVersion(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE RvDoc SET Version = x'DEADBEEF' WHERE Id = 1";   // a concurrent writer's change
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task IsRowVersion_column_rejects_a_stale_update()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, rowVersion: true);

        var doc = new Doc { Id = 1, Name = "orig" };
        ctx.Add(doc);
        await ctx.SaveChangesAsync();          // nORM stamps the concurrency token

        ExternallyChangeRowVersion(cn);        // another writer bumps the row-version

        doc.Name = "updated";
        // The UPDATE's WHERE now carries the (now stale) row-version → 0 rows affected → conflict.
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Without_IsRowVersion_a_plain_column_does_not_gate_the_update()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, rowVersion: false);   // Version is just a plain column

        var doc = new Doc { Id = 1, Name = "orig" };
        ctx.Add(doc);
        await ctx.SaveChangesAsync();

        ExternallyChangeRowVersion(cn);

        doc.Name = "updated";
        var affected = await ctx.SaveChangesAsync();   // no concurrency token → update succeeds
        Assert.Equal(1, affected);
    }
}
