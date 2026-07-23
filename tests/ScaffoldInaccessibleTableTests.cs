#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A single unreadable object must not abort the whole scaffold. A view over a dropped table (or, in
/// production, a permission failure or an object dropped mid-run by a concurrent session) can no longer be
/// SELECTed; the scaffolder must skip it with a loud warning and still generate every accessible table,
/// rather than letting one broken object throw and cost the entire run.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScaffoldInaccessibleTableTests
{
    [Fact]
    public async Task Scaffolding_skips_an_unreadable_view_and_still_generates_the_accessible_tables()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ResGood1 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE ResGood2 (Id INTEGER PRIMARY KEY, Val INTEGER NOT NULL);
                CREATE TABLE ResTemp (Id INTEGER PRIMARY KEY, X INTEGER NOT NULL);
                CREATE VIEW ResBroken AS SELECT Id, X FROM ResTemp;
                DROP TABLE ResTemp;
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "norm_resilient_" + Guid.NewGuid().ToString("N"));
        try
        {
            // Must not throw even though ResBroken can no longer be read.
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "ResCtx",
                new ScaffoldOptions { EmitViewEntities = true });

            // Every accessible table is still generated.
            Assert.True(File.Exists(Path.Combine(dir, "ResGood1.cs")), "accessible table ResGood1 should scaffold");
            Assert.True(File.Exists(Path.Combine(dir, "ResGood2.cs")), "accessible table ResGood2 should scaffold");

            // The unreadable view is skipped, not emitted.
            Assert.False(File.Exists(Path.Combine(dir, "ResBroken.cs")), "the unreadable view must not be scaffolded");

            // ...and it is reported, loudly, in the warnings.
            var warnings = Path.Combine(dir, "nORM.ScaffoldWarnings.md");
            Assert.True(File.Exists(warnings), "a warning report should be written when an object is skipped");
            Assert.Contains("ResBroken", await File.ReadAllTextAsync(warnings), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
