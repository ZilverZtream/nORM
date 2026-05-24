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
/// Pins the deterministic error message when callers combine
/// <c>AsAsyncEnumerable</c> with <c>Include</c> / <c>GroupJoin</c>. Streaming
/// can't compose with eager dependent-query loads because the dependent
/// fetch issues a second round-trip after the principal materializer fills
/// its FK buffer — incompatible with row-by-row streaming. The exception
/// must point at the supported alternatives (ToListAsync, or splitting
/// the query into multiple awaits) rather than letting the streaming path
/// drop rows or duplicate them.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAsAsyncEnumerableIncludeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AaeAuthor (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE AaeBook   (Id INTEGER PRIMARY KEY, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL);
            INSERT INTO AaeAuthor VALUES (1,'Ada'),(2,'Betty');
            INSERT INTO AaeBook   VALUES (1, 1, 'Ada-Book'), (2, 2, 'Betty-Book');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AaeAuthor>().HasKey(a => a.Id);
                mb.Entity<AaeBook>().HasKey(b => b.Id);
                mb.Entity<AaeAuthor>().HasMany(a => a.Books).WithOne()
                    .HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task AsAsyncEnumerable_with_include_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var stream = ((INormQueryable<AaeAuthor>)_ctx.Query<AaeAuthor>())
                .Include(a => a.Books)
                .AsAsyncEnumerable();
            await foreach (var _ in stream) { /* should never enter */ }
        });
        // Message must mention both Include (to identify the constraint) and a
        // supported alternative (ToListAsync) so users have an immediate next step.
        Assert.Contains("Include",     ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ToListAsync", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("AaeAuthor")]
    public sealed class AaeAuthor
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ICollection<AaeBook> Books { get; set; } = new List<AaeBook>();
    }

    [Table("AaeBook")]
    public sealed class AaeBook
    {
        [Key] public int Id { get; set; }
        public int AuthorId { get; set; }
        public string Title { get; set; } = string.Empty;
    }
}
