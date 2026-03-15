using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that SchemaSnapshot.Build(DbContext) populates FK constraints
/// from configured relations, enabling drift detection between snapshots.
/// </summary>
public class RuntimeSnapshotFkTests
{
    private class Author
    {
        public int AuthorId { get; set; }
        public string Name { get; set; } = string.Empty;
        public ICollection<Book> Books { get; set; } = new List<Book>();
    }

    private class Book
    {
        [Key]
        public int BookId { get; set; }
        public string Title { get; set; } = string.Empty;
        public int AuthorId { get; set; }
        public Author? Author { get; set; }
    }

    private static DbContext CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Book>();
                mb.Entity<Author>()
                    .HasKey(a => a.AuthorId)
                    .HasMany(a => a.Books)
                    .WithOne(b => b.Author)
                    .HasForeignKey(b => b.AuthorId, a => a.AuthorId);
            }
        };
        return new DbContext(cn, new SqliteProvider(), options);
    }

    [Fact]
    public void Build_DbContext_PopulatesForeignKeysOnDependentTable()
    {
        using var ctx = CreateContext();
        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var bookTable = snapshot.Tables.FirstOrDefault(t =>
            string.Equals(t.Name, "Book", System.StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(bookTable);
        Assert.NotEmpty(bookTable!.ForeignKeys);
    }

    [Fact]
    public void Build_DbContext_FkConstraintHasCorrectMetadata()
    {
        using var ctx = CreateContext();
        var snapshot = SchemaSnapshotBuilder.Build(ctx);

        var bookTable = snapshot.Tables.First(t =>
            string.Equals(t.Name, "Book", System.StringComparison.OrdinalIgnoreCase));
        var fk = bookTable.ForeignKeys.Single();

        Assert.Contains("AuthorId", fk.DependentColumns);
        Assert.Contains("AuthorId", fk.PrincipalColumns);
        Assert.Equal("Author", fk.PrincipalTable, ignoreCase: true);
        Assert.Contains("Book", fk.ConstraintName);
        Assert.Contains("Author", fk.ConstraintName);
    }

    [Fact]
    public void Build_OldSnapshotWithoutFk_Diff_ReportsAddedForeignKey()
    {
        using var ctx = CreateContext();

        // Old snapshot: manually constructed with no FKs
        var oldSnapshot = new SchemaSnapshot();
        oldSnapshot.Tables.Add(new TableSchema
        {
            Name = "Author",
            Columns =
            {
                new ColumnSchema { Name = "AuthorId", ClrType = "System.Int32", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Author" },
                new ColumnSchema { Name = "Name",     ClrType = "System.String" }
            }
        });
        oldSnapshot.Tables.Add(new TableSchema
        {
            Name = "Book",
            Columns =
            {
                new ColumnSchema { Name = "BookId",   ClrType = "System.Int32", IsPrimaryKey = true, IsUnique = true, IndexName = "PK_Book" },
                new ColumnSchema { Name = "Title",    ClrType = "System.String" },
                new ColumnSchema { Name = "AuthorId", ClrType = "System.Int32" }
            }
            // No ForeignKeys entry
        });

        var newSnapshot = SchemaSnapshotBuilder.Build(ctx);
        var diff = SchemaDiffer.Diff(oldSnapshot, newSnapshot);

        Assert.NotEmpty(diff.AddedForeignKeys);
    }

    [Fact]
    public void Build_TwoIdenticalDbContexts_Diff_HasNoFkChanges()
    {
        using var ctx1 = CreateContext();
        using var ctx2 = CreateContext();

        var snap1 = SchemaSnapshotBuilder.Build(ctx1);
        var snap2 = SchemaSnapshotBuilder.Build(ctx2);
        var diff = SchemaDiffer.Diff(snap1, snap2);

        Assert.Empty(diff.AddedForeignKeys);
        Assert.Empty(diff.DroppedForeignKeys);
    }
}
