using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins null comparisons projected as booleans — <c>Select(w =&gt; new { Flag = w.Note == null })</c>. The
/// projection path (SCV) lowers these to <c>IS [NOT] NULL</c>, so the projected flag is a real boolean, not
/// the always-unknown result of <c>col = NULL</c>. A plausible silent-wrong site; validated correct (the SCV
/// projection path already special-cases null, unlike the simplified navigation-filter grammar which had to
/// be fixed).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionNullComparisonContractTests
{
    [Table("PncWidget")]
    public class Widget { [Key] public int Id { get; set; } public string? Note { get; set; } }

    public class Dto { public int Id { get; set; } public bool NoteIsNull { get; set; } public bool NoteIsNotNull { get; set; } }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PncWidget (Id INTEGER PRIMARY KEY, Note TEXT NULL);
                INSERT INTO PncWidget VALUES (1,NULL),(2,'x');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Null_comparison_projects_a_real_boolean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);

        var dtos = ctx.Query<Widget>()
            .Select(w => new Dto { Id = w.Id, NoteIsNull = w.Note == null, NoteIsNotNull = w.Note != null })
            .ToList().OrderBy(d => d.Id).ToList();

        Assert.True(dtos[0].NoteIsNull);      // id 1: Note IS NULL
        Assert.False(dtos[0].NoteIsNotNull);
        Assert.False(dtos[1].NoteIsNull);     // id 2: Note = 'x'
        Assert.True(dtos[1].NoteIsNotNull);
    }
}
