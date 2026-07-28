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
/// A join table that carries a generated surrogate primary key AND a UNIQUE(fk, fk) constraint AND extra
/// payload columns (a Rails/ActiveRecord-style bridge with a timestamp) was still collapsed to an implicit
/// many-to-many skip navigation, because the payload guard was bypassed whenever a surrogate key was present.
/// The entity was never generated, so its payload columns became unmapped and unreadable (and a NOT NULL
/// payload without a default breaks inserts). Any payload column means the table must scaffold as a full
/// entity, matching EF Core — the surrogate key does not change that.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScaffoldSqliteJoinTablePayloadTests
{
    [Fact]
    public async Task Surrogate_key_join_table_with_payload_scaffolds_as_a_full_entity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                CREATE TABLE Student (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE Course (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE Enrollment (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    StudentId INTEGER NOT NULL,
                    CourseId INTEGER NOT NULL,
                    EnrolledAt TEXT NOT NULL,
                    UNIQUE (StudentId, CourseId),
                    CONSTRAINT FK_Enrollment_Student FOREIGN KEY (StudentId) REFERENCES Student(Id),
                    CONSTRAINT FK_Enrollment_Course FOREIGN KEY (CourseId) REFERENCES Course(Id)
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "scaffold_payload_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "SchoolCtx");

            // The join table must become a real entity so the payload column survives.
            Assert.True(File.Exists(Path.Combine(dir, "Enrollment.cs")),
                "Enrollment carries a payload column and must scaffold as a full entity, not collapse to m2m.");
            var enrollmentCode = await File.ReadAllTextAsync(Path.Combine(dir, "Enrollment.cs"));
            Assert.Contains("EnrolledAt", enrollmentCode);

            // It must NOT be collapsed to an implicit many-to-many mapping (which would drop EnrolledAt).
            var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "SchoolCtx.cs"));
            Assert.DoesNotContain(".UsingTable(\"Enrollment\"", contextCode);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
