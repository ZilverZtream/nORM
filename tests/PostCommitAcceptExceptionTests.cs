using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The tracker-accept phase runs AFTER the transaction commits. If it throws (e.g. a faulting entity
/// getter during navigation cleanup), that exception must not reach the write try/catch and roll back
/// the already-committed transaction or reset the committed DB-generated keys - which would corrupt the
/// tracker into re-inserting committed rows on the next save. Regression: the accept phase is hoisted
/// out of the try, so a post-commit accept exception leaves the committed keys intact.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PostCommitAcceptExceptionTests
{
    [Table("PcaParent")]
    public class PcaParent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";

        // A collection navigation whose getter can be made to throw on THIS instance only.
        // RemoveDeletedInstancesFromTrackedNavigations reads this getter (uncaught) during the
        // post-commit accept phase. Instance-scoped so only the already-committed seed parent throws
        // (the newly-inserted parent's collection is read pre-commit and must not fault).
        [NotMapped] public bool Fault;
        private List<PcaChild> _children = new();
        public List<PcaChild> Children
        {
            get => Fault ? throw new InvalidOperationException("post-commit accept boom") : _children;
            set => _children = value;
        }
    }

    [Table("PcaChild")]
    public class PcaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    [Fact]
    public async Task Exception_in_post_commit_accept_phase_does_not_reset_committed_keys()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PcaParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
                CREATE TABLE PcaChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PcaParent>().HasKey(p => p.Id);
                mb.Entity<PcaChild>().HasKey(c => c.Id);
                mb.Entity<PcaParent>().HasMany(p => p.Children).WithOne()
                                      .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Track a parent (so it has a Children relation the accept phase will read) and a child to delete.
        var seedParent = new PcaParent { Name = "seed" };
        ctx.Add(seedParent);
        await ctx.SaveChangesAsync();
        var child = new PcaChild { Id = 1, ParentId = seedParent.Id };
        ctx.Add(child);
        await ctx.SaveChangesAsync();

        // A save that inserts a NEW parent (DB-generated key) and deletes the child. detectChanges:false
        // so the throwing collection is read only in the post-commit accept phase, not during detection.
        var newParent = new PcaParent { Name = "new" };
        ctx.Add(newParent);
        ctx.Remove(child);
        // Fault only the already-committed seed parent, whose collection is read solely in the
        // post-commit accept phase (RemoveDeletedInstancesFromTrackedNavigations), not pre-commit.
        seedParent.Fault = true;
        Exception ex;
        try
        {
            ex = await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync(detectChanges: false));
        }
        finally
        {
            seedParent.Fault = false;
        }

        // With the fix, the accept-phase exception propagates as-is. Without it, the exception entered the
        // write catch, which tried to roll back the ALREADY-committed transaction; that rollback throws on
        // a completed transaction, producing an AggregateException that masks the real error and falsely
        // reports the (actually committed) save as failed-and-rolled-back.
        Assert.IsNotType<AggregateException>(ex);
        Assert.DoesNotContain("rollback also failed", ex.Message, StringComparison.Ordinal);

        // The insert committed and assigned newParent.Id; it must not have been reset to its pre-insert default.
        Assert.True(newParent.Id > 0, "committed DB-generated key was reset by a post-commit accept-phase exception");

        // The row is actually committed under that key.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM PcaParent WHERE Id = @id";
        check.Parameters.AddWithValue("@id", newParent.Id);
        Assert.Equal(1L, (long)check.ExecuteScalar()!);
        // The delete also committed.
        using var childCheck = cn.CreateCommand();
        childCheck.CommandText = "SELECT COUNT(*) FROM PcaChild";
        Assert.Equal(0L, (long)childCheck.ExecuteScalar()!);
    }
}
