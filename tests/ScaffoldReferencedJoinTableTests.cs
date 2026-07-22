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
/// A pure junction table is normally collapsed into a many-to-many skip navigation and not emitted
/// as its own entity. But when another table's foreign key targets the junction table, that junction
/// is depended upon as a principal: collapsing it would delete the entity the dependent needs and
/// leave a navigation pointing at a type that was never generated (a CS0246 compile error). Such a
/// junction must stay a first-class entity.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScaffoldReferencedJoinTableTests
{
    [Fact]
    public async Task A_junction_table_referenced_by_another_table_is_emitted_as_an_entity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                PRAGMA foreign_keys=ON;
                CREATE TABLE Author (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE Book (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE AuthorBook (
                    AuthorId INTEGER NOT NULL,
                    BookId INTEGER NOT NULL,
                    PRIMARY KEY (AuthorId, BookId),
                    CONSTRAINT FK_AuthorBook_Author FOREIGN KEY (AuthorId) REFERENCES Author(Id),
                    CONSTRAINT FK_AuthorBook_Book FOREIGN KEY (BookId) REFERENCES Book(Id)
                );
                CREATE TABLE Royalty (
                    Id INTEGER PRIMARY KEY,
                    AuthorId INTEGER NOT NULL,
                    BookId INTEGER NOT NULL,
                    Amount TEXT NOT NULL,
                    CONSTRAINT FK_Royalty_AuthorBook FOREIGN KEY (AuthorId, BookId) REFERENCES AuthorBook(AuthorId, BookId)
                );
                """;
            cmd.ExecuteNonQuery();
        }

        var dir = Path.Combine(Path.GetTempPath(), "norm_refjoin_" + Guid.NewGuid().ToString("N"));
        try
        {
            await DatabaseScaffolder.ScaffoldAsync(cn, new SqliteProvider(), dir, "TestNs", "RefJoinCtx");

            // The junction is referenced by Royalty, so it must be a real entity...
            Assert.True(File.Exists(Path.Combine(dir, "AuthorBook.cs")),
                "a junction table targeted by another foreign key must be emitted as an entity");

            // ...and the dependent's navigation must resolve to that generated type.
            var royaltyCode = await File.ReadAllTextAsync(Path.Combine(dir, "Royalty.cs"));
            Assert.Contains("AuthorBook", royaltyCode, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
